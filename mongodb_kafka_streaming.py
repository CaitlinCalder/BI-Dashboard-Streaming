import json
import logging
import signal
import sys
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import bson
import requests  # 🆕 Added for Power BI


# Import configuration
try:
    from ClearVueConfig import ClearVueConfig, print_startup_banner
except ImportError:
    print("❌ Error: ClearVueConfig.py not found")
    sys.exit(1)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clearvue_streaming.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ClearVuePipeline')


class ClearVueStreamingPipeline:
    """
    Production-ready streaming pipeline for ClearVue BI
    Monitors MongoDB collections and streams enriched changes to Kafka AND Power BI
    """
    
    def __init__(self):
        """Initialize the streaming pipeline with configuration"""
        
        print_startup_banner()
        
        logger.info("Initializing ClearVue Streaming Pipeline...")
        
        # Get configuration
        self.mongo_uri = ClearVueConfig.get_mongo_uri()
        self.db_name = ClearVueConfig.get_database_name()
        self.kafka_servers = ClearVueConfig.get_kafka_servers()
        
        # Print configuration
        ClearVueConfig.print_config(detailed=True)
        
        # Connect to MongoDB Atlas
        self._connect_mongodb()
        
        # Setup Kafka Producer
        self._setup_kafka_producer()
        
        # Pipeline state
        self.is_running = False
        self.change_streams = {}
        self.stream_threads = []
        
        # Statistics tracking
        self.stats = {
            'total_changes': 0,
            'changes_by_collection': defaultdict(int),
            'changes_by_operation': defaultdict(int),
            'high_priority_events': 0,
            'last_processed': None,
            'start_time': None,
            'errors': 0,
            'kafka_send_errors': 0,
            'mongodb_errors': 0,
            'powerbi_pushes': 0,  # 🆕
            'powerbi_errors': 0   # 🆕
        }
        
        # Health monitoring
        self.last_health_check = datetime.now()
        self.is_healthy = True
        
        logger.info("Pipeline initialized successfully")
    
    def _connect_mongodb(self):
        """Establish connection to MongoDB Atlas"""
        print("\n🔌 Connecting to MongoDB Atlas...")
        
        try:
            self.mongo_client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=20000,
                socketTimeoutMS=20000,
                retryWrites=True,
                w='majority'
            )
            
            # Test connection with ping
            self.mongo_client.admin.command('ping')
            
            # Get database
            self.db = self.mongo_client[self.db_name]
            
            # Verify collections exist
            existing_collections = self.db.list_collection_names()
            monitored_collections = ClearVueConfig.get_all_collections()
            
            print(f"✅ MongoDB Atlas connected!")
            print(f"   Database: {self.db_name}")
            print(f"   Collections found: {len(existing_collections)}")
            
            # Check if monitored collections exist
            missing = [c for c in monitored_collections if c not in existing_collections]
            if missing:
                logger.warning(f"Collections not found: {missing}")
                print(f"⚠️  Warning: Missing collections: {', '.join(missing)}")
            
            logger.info(f"Connected to MongoDB: {self.db_name}")
            
        except ServerSelectionTimeoutError:
            logger.error("MongoDB connection timeout - check network/credentials")
            print("❌ MongoDB connection failed: Timeout")
            print("   Check: 1) Network connection 2) MongoDB Atlas IP whitelist 3) Credentials")
            raise
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            print(f"❌ MongoDB connection failed: {e}")
            raise
    
    def _setup_kafka_producer(self):
        """Setup Kafka producer with configuration"""
        print("\n⚙️ Setting up Kafka producer...")
        
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v, default=self._json_serializer).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                **ClearVueConfig.KAFKA_PRODUCER_CONFIG
            )
            
            print("✅ Kafka producer ready!")
            logger.info("Kafka producer initialized")
            
        except Exception as e:
            logger.error(f"Kafka setup failed: {e}")
            print(f"❌ Kafka setup failed: {e}")
            print("   Make sure Kafka is running: docker-compose up -d")
            raise
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for MongoDB and datetime objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bson.ObjectId):
            return str(obj)
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def create_kafka_topics(self):
        """Create Kafka topics if they don't exist"""
        print("\n📋 Creating/verifying Kafka topics...")
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers,
                client_id='clearvue_pipeline_admin'
            )
            
            # Get existing topics
            existing_topics = admin_client.list_topics()
            
            # Create topics based on configuration
            topics_to_create = []
            for collection, config in ClearVueConfig.KAFKA_TOPICS.items():
                topic_name = config['topic']
                
                if topic_name not in existing_topics:
                    new_topic = NewTopic(
                        name=topic_name,
                        num_partitions=config['partitions'],
                        replication_factor=1
                    )
                    topics_to_create.append(new_topic)
                    logger.info(f"Will create topic: {topic_name}")
            
            if topics_to_create:
                admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
                print(f"✅ Created {len(topics_to_create)} new Kafka topics")
                logger.info(f"Created {len(topics_to_create)} topics")
            else:
                print("✅ All Kafka topics already exist")
                logger.info("All topics exist")
            
            admin_client.close()
            
        except Exception as e:
            logger.warning(f"Topic creation warning: {e}")
            print(f"⚠️ Topic warning (may already exist): {e}")
    
    def calculate_financial_period(self, date_obj: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Calculate ClearVue financial period information
        
        Args:
            date_obj: Date to calculate period for (default: now)
        
        Returns:
            Dictionary with financial period details
        """
        if date_obj is None:
            date_obj = datetime.now()
        
        # Convert string to datetime if needed
        if isinstance(date_obj, str):
            try:
                date_obj = datetime.fromisoformat(date_obj.replace('Z', '+00:00'))
            except:
                date_obj = datetime.now()
        
        fin_period = int(f"{date_obj.year}{date_obj.month:02d}")
        quarter = ((date_obj.month - 1) // 3) + 1
        
        return {
            'fin_period': fin_period,
            'year': date_obj.year,
            'month': date_obj.month,
            'quarter': quarter,
            'month_name': date_obj.strftime('%B'),
            'quarter_name': f"Q{quarter}",
            'iso_date': date_obj.isoformat()
        }
    
    def enhance_change_event(self, change_doc: Dict[str, Any], collection_name: str) -> Dict[str, Any]:
        """
        Enrich MongoDB change event with business context
        
        Args:
            change_doc: Raw change stream document from MongoDB
            collection_name: Name of the collection
        
        Returns:
            Enriched event ready for Kafka and Power BI
        """
        
        # Base event structure
        enhanced_event = {
            'event_id': str(bson.ObjectId()),
            'timestamp': datetime.now().isoformat(),
            'source': 'mongodb_atlas',
            'database': self.db_name,
            'collection': collection_name,
            'operation': change_doc['operationType'],
            'priority': ClearVueConfig.get_collection_priority(collection_name),
            'document_key': str(change_doc.get('documentKey', {}).get('_id', '')),
            'change_data': {},
            'business_context': {},
            'metadata': {
                'pipeline_version': '2.0',
                'processed_at': datetime.now().isoformat()
            }
        }
        
        # Extract changed data based on operation type
        operation = change_doc['operationType']
        
        if operation == 'insert' or operation == 'replace':
            if 'fullDocument' in change_doc:
                enhanced_event['change_data'] = change_doc['fullDocument']
        
        elif operation == 'update':
            if 'updateDescription' in change_doc:
                enhanced_event['change_data'] = {
                    'updated_fields': change_doc['updateDescription'].get('updatedFields', {}),
                    'removed_fields': change_doc['updateDescription'].get('removedFields', []),
                }
            if 'fullDocument' in change_doc:
                enhanced_event['change_data']['full_document'] = change_doc['fullDocument']
        
        elif operation == 'delete':
            enhanced_event['change_data'] = {
                'deleted_id': str(change_doc.get('documentKey', {}).get('_id', ''))
            }
        
        # Add collection-specific business context
        doc = change_doc.get('fullDocument', {})
        
        # SALES ENRICHMENT
        if collection_name == 'Sales_flat' and doc:
            trans_date = doc.get('trans_date', datetime.now())
            fin_info = self.calculate_financial_period(trans_date)
            
            total_amount = 0
            total_cost = 0
            
            if 'lines' in doc and isinstance(doc['lines'], list):
                for line in doc['lines']:
                    line_total = line.get('total_line_cost', 0)
                    line_cost = line.get('last_cost', 0) * line.get('quantity', 0)
                    total_amount += line_total
                    total_cost += line_cost
            
            total_profit = total_amount - total_cost
            
            enhanced_event['business_context'] = {
                'transaction_type': 'sales',
                'doc_number': str(doc.get('_id', '')),
                'customer_id': doc.get('customer_id', ''),
                'trans_type': doc.get('trans_type', ''),
                'trans_date': str(trans_date),
                'fin_period': fin_info['fin_period'],
                'financial_year': fin_info['year'],
                'financial_month': fin_info['month'],
                'financial_quarter': fin_info['quarter_name'],
                'total_amount': round(total_amount, 2),
                'total_cost': round(total_cost, 2),
                'total_profit': round(total_profit, 2),
                'profit_margin_pct': round((total_profit / total_amount * 100) if total_amount > 0 else 0, 2),
                'line_count': len(doc.get('lines', [])),
                'metric_category': 'revenue',
                'kpi_type': 'sales_revenue'
            }
        
        # PAYMENTS ENRICHMENT
        elif collection_name == 'Payments_flat' and doc:
            deposit_date = None
            total_payment = 0
            total_discount = 0
            total_bank_amt = 0
            
            if 'lines' in doc and isinstance(doc['lines'], list):
                for line in doc['lines']:
                    if not deposit_date and 'deposit_date' in line:
                        deposit_date = line['deposit_date']
                    total_bank_amt += line.get('bank_amt', 0)
                    total_discount += line.get('discount', 0)
                    total_payment += line.get('tot_payment', 0)
            
            fin_info = self.calculate_financial_period(deposit_date or datetime.now())
            
            enhanced_event['business_context'] = {
                'transaction_type': 'payment',
                'customer_id': doc.get('customer_id', ''),
                'deposit_ref': doc.get('deposit_ref', ''),
                'deposit_date': str(deposit_date) if deposit_date else None,
                'fin_period': fin_info['fin_period'],
                'financial_year': fin_info['year'],
                'financial_month': fin_info['month'],
                'financial_quarter': fin_info['quarter_name'],
                'total_bank_amount': round(total_bank_amt, 2),
                'total_discount': round(total_discount, 2),
                'total_payment': round(total_payment, 2),
                'discount_pct': round((total_discount / total_bank_amt * 100) if total_bank_amt > 0 else 0, 2),
                'payment_count': len(doc.get('lines', [])),
                'metric_category': 'cash_flow',
                'kpi_type': 'payments_received'
            }
        
        # PURCHASES ENRICHMENT
        elif collection_name == 'Purchases_flat' and doc:
            purch_date = doc.get('purch_date', datetime.now())
            fin_info = self.calculate_financial_period(purch_date)
            
            total_cost = 0
            if 'lines' in doc and isinstance(doc['lines'], list):
                total_cost = sum(line.get('total_line_cost', 0) for line in doc['lines'])
            
            enhanced_event['business_context'] = {
                'transaction_type': 'purchase',
                'purch_doc_no': str(doc.get('_id', '')),
                'supplier_id': doc.get('supplier_id', ''),
                'purch_date': str(purch_date),
                'fin_period': fin_info['fin_period'],
                'financial_year': fin_info['year'],
                'financial_month': fin_info['month'],
                'financial_quarter': fin_info['quarter_name'],
                'total_cost': round(total_cost, 2),
                'line_count': len(doc.get('lines', [])),
                'metric_category': 'procurement',
                'kpi_type': 'purchase_orders'
            }
        
        # CUSTOMERS ENRICHMENT
        elif collection_name == 'Customer_flat_step2' and doc:
            age_analysis = doc.get('age_analysis', {})
            total_due = age_analysis.get('total_due', 0) if isinstance(age_analysis, dict) else 0
            
            enhanced_event['business_context'] = {
                'entity_type': 'customer',
                'customer_id': str(doc.get('_id', '')),
                'customer_name': doc.get('name', ''),
                'region_id': doc.get('region', {}).get('id', '') if isinstance(doc.get('region'), dict) else '',
                'region_name': doc.get('region', {}).get('desc', '') if isinstance(doc.get('region'), dict) else '',
                'category_id': doc.get('category', {}).get('id', '') if isinstance(doc.get('category'), dict) else '',
                'category_name': doc.get('category', {}).get('desc', '') if isinstance(doc.get('category'), dict) else '',
                'account_status': doc.get('account', {}).get('status', '') if isinstance(doc.get('account'), dict) else '',
                'account_type': doc.get('account', {}).get('type', '') if isinstance(doc.get('account'), dict) else '',
                'credit_limit': doc.get('credit_limit', 0),
                'discount_pct': doc.get('discount', 0),
                'total_due': total_due,
                'metric_category': 'customer_master',
                'kpi_type': 'customer_credit'
            }
        
        # PRODUCTS ENRICHMENT
        elif collection_name == 'Products_flat' and doc:
            enhanced_event['business_context'] = {
                'entity_type': 'product',
                'product_id': str(doc.get('_id', '')),
                'inventory_code': doc.get('inventory_code', ''),
                'brand_id': doc.get('brand', {}).get('id', '') if isinstance(doc.get('brand'), dict) else '',
                'brand_name': doc.get('brand', {}).get('desc', '') if isinstance(doc.get('brand'), dict) else '',
                'category_id': doc.get('category', {}).get('id', '') if isinstance(doc.get('category'), dict) else '',
                'category_name': doc.get('category', {}).get('desc', '') if isinstance(doc.get('category'), dict) else '',
                'last_cost': doc.get('last_cost', 0),
                'in_stock': doc.get('stock_ind', False),
                'metric_category': 'product_master',
                'kpi_type': 'inventory_value'
            }
        
        # SUPPLIERS ENRICHMENT
        elif collection_name == 'Suppliers' and doc:
            enhanced_event['business_context'] = {
                'entity_type': 'supplier',
                'supplier_id': str(doc.get('_id', '')),
                'supplier_name': doc.get('description', ''),
                'exclusive': doc.get('exclusive', False),
                'credit_limit': doc.get('credit_limit', 0),
                'payment_terms_days': doc.get('normal_payterms', 0),
                'metric_category': 'supplier_master',
                'kpi_type': 'supplier_terms'
            }
        
        return enhanced_event
    
    def send_to_kafka(self, topic: str, key: str, message: Dict[str, Any]) -> bool:
        """Send enriched message to Kafka with retry logic"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                future = self.kafka_producer.send(topic, key=key, value=message)
                record_metadata = future.get(timeout=10)
                
                logger.debug(f"✓ Kafka: {topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")
                return True
                
            except KafkaError as e:
                retry_count += 1
                logger.error(f"Kafka error (attempt {retry_count}/{max_retries}): {e}")
                self.stats['kafka_send_errors'] += 1
                
                if retry_count < max_retries:
                    time.sleep(1)
                else:
                    logger.error(f"Failed to send to Kafka after {max_retries} attempts")
                    return False
                    
            except Exception as e:
                logger.error(f"Unexpected error sending to Kafka: {e}")
                self.stats['errors'] += 1
                return False
        
        return False
    
    # 🆕 POWER BI METHODS START HERE
    # Replace the existing send_to_powerbi method with this one:

    def send_to_powerbi(self, enhanced_event: Dict[str, Any]) -> bool:
        """
        Send Sales events to Power BI streaming dataset
        Schema-compliant with exact field types:
        - TEXT fields: _id, DOC_NUMBER, TRANSTYPE_CODE, REP_CODE, CUSTOMER_NUMBER, line_INVENTORY_CODE
        - DATE fields: TRANS_DATE, FIN_PERIOD
        - INT fields: line_QUANTITY, line_UNIT_SELL_PRICE, line_TOTAL_LINE_PRICE, line_LAST_COST
        """
        if enhanced_event.get('collection') != 'Sales_flat':
            return False
        
        try:
            powerbi_url = ClearVueConfig.get_powerbi_push_url()
            if not powerbi_url:
                logger.warning("Power BI URL not configured")
                return False

            # Get the original document data
            doc = enhanced_event.get('change_data', {})
            if not doc:
                return False

            # Get all line items
            lines = doc.get('lines', [])
            if not lines:
                logger.debug("No line items in sales document")
                return False

            # Get transaction date
            trans_date = doc.get('trans_date', datetime.now())
            if isinstance(trans_date, str):
                try:
                    trans_date = datetime.fromisoformat(trans_date.replace('Z', '+00:00'))
                except:
                    trans_date = datetime.now()
            
            # Format dates for Power BI (ISO 8601 format)
            trans_date_str = trans_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            
            # Create FIN_PERIOD as a date (first day of the month)
            fin_period_date = datetime(trans_date.year, trans_date.month, 1)
            fin_period_str = fin_period_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

            # Push each line item as a separate record
            success_count = 0
            for line in lines:
                # Ensure all numeric fields are integers (Power BI expects INT, not FLOAT)
                powerbi_payload = {
                    # TEXT fields
                    "_id": str(doc.get('_id', '')),
                    "DOC_NUMBER": str(doc.get('doc_number', doc.get('_id', ''))),
                    "TRANSTYPE_CODE": str(doc.get('trans_type', '')),
                    "REP_CODE": str(doc.get('rep_code', doc.get('rep', {}).get('id', ''))),
                    "CUSTOMER_NUMBER": str(doc.get('customer_id', '')),
                    
                    # DATE fields (ISO 8601 format)
                    "TRANS_DATE": trans_date_str,
                    "FIN_PERIOD": fin_period_str,
                    
                    # Line item TEXT field
                    "line_INVENTORY_CODE": str(line.get('inventory_code', '')),
                    
                    # Line item INT fields (convert floats to integers)
                    "line_QUANTITY": int(line.get('quantity', 0)),
                    "line_UNIT_SELL_PRICE": int(round(line.get('unit_price', line.get('unit_sell_price', 0)))),
                    "line_TOTAL_LINE_PRICE": int(round(line.get('total_line_cost', 0))),
                    "line_LAST_COST": int(round(line.get('last_cost', 0)))
                }

                # Send to Power BI (expects array)
                response = requests.post(
                    powerbi_url,
                    json=[powerbi_payload],
                    headers={'Content-Type': 'application/json'},
                    timeout=5
                )

                if response.status_code in (200, 201):
                    success_count += 1
                    logger.debug(f"✓ Power BI: Pushed line {line.get('inventory_code')} from doc {doc.get('_id')}")
                else:
                    logger.error(f"Power BI push failed ({response.status_code}): {response.text}")
                    logger.error(f"Payload: {powerbi_payload}")
                    self.stats['powerbi_errors'] += 1

            # Update stats
            if success_count > 0:
                self.stats['powerbi_pushes'] += success_count
                logger.info(f"✓ Power BI: Successfully pushed {success_count}/{len(lines)} line items for doc {doc.get('_id')}")
                return True
            else:
                logger.error(f"✗ Power BI: Failed to push any line items for doc {doc.get('_id')}")
                return False

        except requests.exceptions.Timeout:
            logger.error("Power BI push timeout")
            self.stats['powerbi_errors'] += 1
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Power BI request error: {str(e)}")
            self.stats['powerbi_errors'] += 1
            return False
        except Exception as e:
            logger.error(f"Power BI push error: {str(e)}", exc_info=True)
            self.stats['powerbi_errors'] += 1
            return False
    
    def _transform_sales_for_powerbi(self, enhanced_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Sales event to Power BI streaming dataset schema
        
        Args:
            enhanced_event: The enriched event from pipeline
        
        Returns:
            Dictionary matching Power BI schema (16 fields)
        """
        business_context = enhanced_event.get('business_context', {})
        
        # Map to the 16 fields defined in Power BI
        powerbi_record = {
            'timestamp': enhanced_event['timestamp'],
            'event_id': enhanced_event['event_id'],
            'collection': enhanced_event['collection'],
            'operation': enhanced_event['operation'],
            'priority': enhanced_event['priority'],
            'transaction_type': business_context.get('transaction_type', 'sales'),
            'customer_id': business_context.get('customer_id', ''),
            'trans_date': business_context.get('trans_date', ''),
            'fin_period': business_context.get('fin_period', 0),
            'financial_year': business_context.get('financial_year', 0),
            'financial_quarter': business_context.get('financial_quarter', ''),
            'total_amount': business_context.get('total_amount', 0.0),
            'total_cost': business_context.get('total_cost', 0.0),
            'total_profit': business_context.get('total_profit', 0.0),
            'profit_margin_pct': business_context.get('profit_margin_pct', 0.0),
            'line_count': business_context.get('line_count', 0),
        }
        
        return powerbi_record
    # 🆕 POWER BI METHODS END HERE
    
    def process_change_event(self, change_doc: Dict[str, Any], collection_name: str):
        """Process a detected change event from MongoDB"""
        try:
            # Enrich the change event
            enhanced_event = self.enhance_change_event(change_doc, collection_name)
            
            # Get Kafka topic
            topic = ClearVueConfig.get_topic_for_collection(collection_name)
            
            # Create message key for partitioning
            message_key = enhanced_event['document_key']
            
            # Send to Kafka (existing)
            kafka_success = self.send_to_kafka(topic, message_key, enhanced_event)
            
            # 🆕 ALSO send to Power BI (only for Sales in prototype)
            powerbi_success = False
            if collection_name == 'Sales_flat':
                powerbi_success = self.send_to_powerbi(enhanced_event)
            
            if kafka_success:
                # Update statistics
                self.stats['total_changes'] += 1
                self.stats['changes_by_collection'][collection_name] += 1
                
                operation = change_doc['operationType']
                self.stats['changes_by_operation'][operation] += 1
                
                if enhanced_event['priority'] == 'HIGH':
                    self.stats['high_priority_events'] += 1
                
                self.stats['last_processed'] = datetime.now().isoformat()
                
                # 🆕 Track Power BI pushes
                if powerbi_success:
                    self.stats['powerbi_pushes'] += 1
                
                # Log high priority events
                if enhanced_event['priority'] == 'HIGH':
                    status = "→ Kafka ✓ PowerBI ✓" if powerbi_success else "→ Kafka ✓"
                    logger.info(f"⚡ HIGH: {operation.upper()} on {collection_name} {status} | Total: {self.stats['total_changes']}")
                    print(f"⚡ {collection_name}: {operation.upper()} {status}")
                else:
                    logger.debug(f"Processed {operation} on {collection_name}")
        
        except Exception as e:
            logger.error(f"Error processing change event: {e}", exc_info=True)
            self.stats['errors'] += 1
    
    def watch_collection(self, collection_name: str):
        """Watch a specific collection for changes using MongoDB Change Streams"""
        collection = self.db[collection_name]
        
        # Change stream pipeline
        pipeline = [
            {
                '$match': {
                    'operationType': {'$in': ['insert', 'update', 'delete']}
                }
            }
        ]
        
        logger.info(f"Started change stream: {collection_name}")
        print(f"  ✓ Watching: {collection_name}")
        
        try:
            with collection.watch(pipeline, **ClearVueConfig.CHANGE_STREAM_CONFIG) as stream:
                self.change_streams[collection_name] = stream
                
                for change in stream:
                    if not self.is_running:
                        break
                    
                    self.process_change_event(change, collection_name)
        
        except PyMongoError as e:
            logger.error(f"MongoDB error for {collection_name}: {e}", exc_info=True)
            self.stats['mongodb_errors'] += 1
            print(f"❌ MongoDB stream error for {collection_name}: {e}")
        
        except Exception as e:
            logger.error(f"Change stream error for {collection_name}: {e}", exc_info=True)
            self.stats['errors'] += 1
            print(f"❌ Stream error for {collection_name}: {e}")
        
        finally:
            if collection_name in self.change_streams:
                del self.change_streams[collection_name]
            logger.info(f"Change stream closed: {collection_name}")
    
    def health_check(self):
        """Perform health check on pipeline components"""
        try:
            # Check MongoDB
            self.mongo_client.admin.command('ping')
            
            # Check Kafka
            self.kafka_producer.bootstrap_connected()
            
            self.is_healthy = True
            self.last_health_check = datetime.now()
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            self.is_healthy = False
    
    def print_stats(self, final: bool = False):
        """Print pipeline statistics"""
        uptime = "Unknown"
        if self.stats['start_time']:
            start = datetime.fromisoformat(self.stats['start_time'])
            uptime = str(datetime.now() - start).split('.')[0]
        
        print("\n" + "="*70)
        print(f"{'📊 FINAL STATISTICS' if final else '📊 PIPELINE STATISTICS'}")
        print("="*70)
        print(f"  Uptime: {uptime}")
        print(f"📈 Total changes: {self.stats['total_changes']:,}")
        print(f"⚡ High priority: {self.stats['high_priority_events']:,}")
        print(f"📊 PowerBI pushes: {self.stats['powerbi_pushes']:,}")  # 🆕
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"⚠️  Kafka errors: {self.stats['kafka_send_errors']}")
        print(f"⚠️  PowerBI errors: {self.stats['powerbi_errors']}")  # 🆕
        print(f"⚠️  MongoDB errors: {self.stats['mongodb_errors']}")
        print(f"💚 Health: {'HEALTHY ✓' if self.is_healthy else 'UNHEALTHY ✗'}")
        print(f"🕐 Last processed: {self.stats['last_processed'] or 'N/A'}")
        
        if self.stats['changes_by_collection']:
            print("\n📁 Changes by collection:")
            for coll, count in sorted(
                self.stats['changes_by_collection'].items(),
                key=lambda x: x[1],
                reverse=True
            ):
                priority = ClearVueConfig.get_collection_priority(coll)
                print(f"   {coll:25} : {count:,} ({priority})")
        
        if self.stats['changes_by_operation']:
            print("\n🔧 Changes by operation:")
            for op, count in sorted(self.stats['changes_by_operation'].items()):
                print(f"   {op:10} : {count:,}")
        
        print("="*70 + "\n")
    
    def start_pipeline(self, collections: Optional[List[str]] = None):
        """
        Start the streaming pipeline
        
        Args:
            collections: List of collections to monitor (None = all collections)
        """
        if self.is_running:
            print("⚠️  Pipeline is already running!")
            return
        
        # Default to all collections from config
        if collections is None:
            collections = ClearVueConfig.get_all_collections()
        
        print("\n" + "="*70)
        print("🚀 STARTING CLEARVUE STREAMING PIPELINE")
        print("="*70)
        print(f"📂 Database: {self.db_name}")
        print(f"📋 Collections: {', '.join(collections)}")
        print(f"📨 Kafka Topics: {len(collections)}")
        
        # 🆕 Check Power BI configuration
        powerbi_url = ClearVueConfig.get_powerbi_push_url()
        if powerbi_url:
            print(f"📊 Power BI: ENABLED (Sales only)")
        else:
            print(f"📊 Power BI: DISABLED (URL not configured)")
        
        print(f"🌐 Mode: MongoDB ATLAS (Production)")
        print("="*70 + "\n")
        
        # Create Kafka topics
        self.create_kafka_topics()
        
        # Mark as running
        self.is_running = True
        self.stats['start_time'] = datetime.now().isoformat()
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            print("\n⚠️ Shutdown signal received")
            logger.info("Shutdown signal received")
            self.stop_pipeline()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start change stream watchers in separate threads
        self.stream_threads = []
        for collection_name in collections:
            if collection_name in ClearVueConfig.KAFKA_TOPICS:
                thread = threading.Thread(
                    target=self.watch_collection,
                    args=(collection_name,),
                    name=f"Stream-{collection_name}",
                    daemon=True
                )
                thread.start()
                self.stream_threads.append(thread)
            else:
                print(f"⚠️  Unknown collection: {collection_name}")
                logger.warning(f"Unknown collection: {collection_name}")
        
        print(f"\n✅ Started {len(self.stream_threads)} change stream watchers\n")
        logger.info(f"Pipeline started with {len(self.stream_threads)} watchers")
        
        print("🟢 PIPELINE IS RUNNING!")
        print("👀 Waiting for database changes...")
        print("🛑 Press Ctrl+C to stop\n")
        
        # Keep main thread alive and print stats periodically
        try:
            last_stats_time = datetime.now()
            last_health_check = datetime.now()
            
            while self.is_running and any(t.is_alive() for t in self.stream_threads):
                time.sleep(1)
                
                now = datetime.now()
                
                # Print stats periodically
                if (now - last_stats_time).seconds >= ClearVueConfig.STATS_PRINT_INTERVAL:
                    self.print_stats()
                    last_stats_time = now
                
                # Health check periodically
                if (now - last_health_check).seconds >= ClearVueConfig.HEALTHCHECK_INTERVAL:
                    self.health_check()
                    last_health_check = now
        
        except KeyboardInterrupt:
            print("\n⚠️  Manual shutdown requested")
            logger.info("Manual shutdown requested")
            self.stop_pipeline()
        
        # Wait for threads to finish
        print("\n⏳ Waiting for threads to finish...")
        for thread in self.stream_threads:
            thread.join(timeout=5)
        
        print("\n✅ Pipeline stopped successfully")
        logger.info("Pipeline stopped")
    
    def stop_pipeline(self):
        """Stop the pipeline gracefully"""
        print("\n🛑 Stopping pipeline...")
        logger.info("Stopping pipeline...")
        self.is_running = False
        
        # Close change streams
        for collection_name in list(self.change_streams.keys()):
            try:
                self.change_streams[collection_name].close()
                print(f"   ✓ Closed stream: {collection_name}")
            except Exception as e:
                logger.error(f"Error closing stream {collection_name}: {e}")
        
        # Flush and close Kafka producer
        try:
            print("   ⏳ Flushing Kafka producer...")
            self.kafka_producer.flush(timeout=10)
            self.kafka_producer.close()
            print("   ✓ Closed Kafka producer")
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        
        # Close MongoDB connection
        try:
            self.mongo_client.close()
            print("   ✓ Closed MongoDB connection")
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB: {e}")
        
        # Print final statistics
        self.print_stats(final=True)


def main():
    """Main entry point for the streaming pipeline"""
    
    print_startup_banner()
    
    print("\n📋 PIPELINE MONITORING OPTIONS:")
    print("="*70)
    print("1. ALL collections (all 6 collections)")
    print("2. TRANSACTION data only (Sales, Payments, Purchases)")
    print("3. MASTER data only (Customers, Products, Suppliers)")
    print("4. HIGH PRIORITY only (Sales, Payments)")
    print("5. CUSTOM selection")
    print("="*70)
    
    choice = input("\n👉 Select option (1-5) [default: 1]: ").strip() or "1"
    
    pipeline = None
    try:
        # Initialize pipeline
        pipeline = ClearVueStreamingPipeline()
        
        # Start based on user selection
        if choice == "1":
            # Monitor all 6 collections
            pipeline.start_pipeline()
        
        elif choice == "2":
            # Transaction data only
            print("\n📊 Monitoring TRANSACTION collections only...")
            pipeline.start_pipeline(collections=['Sales_flat', 'Payments_flat', 'Purchases_flat'])
        
        elif choice == "3":
            # Master data only
            print("\n📁 Monitoring MASTER DATA collections only...")
            pipeline.start_pipeline(collections=['Customer_flat_step2', 'Products_flat', 'Suppliers'])
        
        elif choice == "4":
            # High priority only
            print("\n⚡ Monitoring HIGH PRIORITY collections only...")
            pipeline.start_pipeline(collections=['Sales_flat', 'Payments_flat'])
        
        elif choice == "5":
            # Custom selection
            print("\n📋 Available collections:")
            all_collections = ClearVueConfig.get_all_collections()
            for i, coll in enumerate(all_collections, 1):
                priority = ClearVueConfig.get_collection_priority(coll)
                topic = ClearVueConfig.get_topic_for_collection(coll)
                print(f"   {i}. {coll:25} → {topic:25} ({priority})")
            
            selected = input("\n👉 Enter collection names (comma-separated): ").strip()
            if selected:
                colls = [c.strip() for c in selected.split(',')]
                pipeline.start_pipeline(collections=colls)
            else:
                print("⚠️ No collections selected, exiting...")
        
        else:
            print("❌ Invalid option")
    
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        logger.info("Interrupted by user")
    
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
    
    finally:
        if pipeline:
            pipeline.stop_pipeline()
        print("\n👋 Goodbye!\n")


if __name__ == "__main__":
    main()