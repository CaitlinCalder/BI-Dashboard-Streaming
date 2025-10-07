"""
ClearVue Streaming Pipeline - Real Schema Implementation
MongoDB Atlas Change Streams to Kafka Producer
Watches for real-time changes in ClearVue collections and streams them to Kafka

Author: ClearVue Implementation Team
Date: October 2025
Status: Production-ready for real ClearVue data
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
import threading
import signal
import sys
import time

from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import KafkaError
import bson

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ClearVueStreamingPipeline')

class ClearVueStreamingPipeline:
    def __init__(self, 
                 mongo_uri='mongodb+srv://Tyra:1234@novacluster.1re1a4e.mongodb.net/?authSource=admin&retryWrites=true&w=majority',
                 kafka_servers=['localhost:29092'],
                 database_name='Nova_Analytics'):
        """
        Initialize the streaming pipeline
        
        Args:
            mongo_uri: MongoDB Atlas connection string
            kafka_servers: List of Kafka bootstrap servers
            database_name: MongoDB database name
        """
        
        # Connect to MongoDB Atlas
        print("🔌 Connecting to MongoDB Atlas...")
        try:
            self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.mongo_client.admin.command('ping')
            print("✅ MongoDB Atlas connection successful!")
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
            raise
        
        self.db = self.mongo_client[database_name]
        
        # Kafka Producer setup
        print("🔧 Setting up Kafka producer...")
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                enable_idempotence=True
            )
            print("✅ Kafka producer ready!")
        except Exception as e:
            print(f"❌ Kafka setup failed: {e}")
            raise
        
        # Control variables
        self.is_running = False
        self.change_streams = {}
        
        # Statistics
        self.stats = {
            'total_changes': 0,
            'changes_by_collection': {},
            'changes_by_operation': {},
            'last_processed': None,
            'start_time': None,
            'errors': 0
        }
        
        # Topic mapping for real ClearVue collections
        # Maps MongoDB collections to Kafka topics
        self.topic_mapping = {
            # Core transaction collections (high priority)
            'sales_headers': 'clearvue.sales.realtime',
            'payment_headers': 'clearvue.payments.headers',
            'payment_lines': 'clearvue.payments.lines',
            'purchases_headers': 'clearvue.purchases.headers',
            'purchases_lines': 'clearvue.purchases.lines',
            
            # Master data collections (medium priority)
            'customers': 'clearvue.customers.changes',
            'products': 'clearvue.products.changes',
            'suppliers': 'clearvue.suppliers.changes',
            
            # Reference data collections (low priority)
            'product_brands': 'clearvue.products.brands',
            'product_categories': 'clearvue.products.categories',
            'product_ranges': 'clearvue.products.ranges',
            'products_styles': 'clearvue.products.styles',
            'representatives': 'clearvue.representatives.changes',
            'customer_regions': 'clearvue.customers.regions',
            'customer_categories': 'clearvue.customers.categories',
            'trans_types': 'clearvue.reference.transtypes',
            'age_analysis': 'clearvue.customers.ageanalysis'
        }
        
        logger.info(f"ClearVue Streaming Pipeline initialized for database: {database_name}")
        print(f"📊 Monitoring {len(self.topic_mapping)} collections")
    
    def json_serializer(self, obj):
        """Custom JSON serializer for MongoDB objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bson.ObjectId):
            return str(obj)
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def create_kafka_topics(self):
        """Create Kafka topics if they don't exist"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=['localhost:29092'],
                client_id='clearvue_topic_creator'
            )
            
            topics_to_create = []
            for topic_name in self.topic_mapping.values():
                new_topic = NewTopic(
                    name=topic_name, 
                    num_partitions=3,
                    replication_factor=1
                )
                topics_to_create.append(new_topic)
            
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            logger.info(f"Kafka topics created/verified: {len(topics_to_create)}")
            print("✅ Kafka topics ready")
            
        except Exception as e:
            logger.warning(f"Topic creation warning (may already exist): {e}")
            print("⚠️  Topics may already exist (this is fine)")
    
    def get_financial_period(self, date_obj=None):
        """Calculate ClearVue financial period (YYYYMM format)"""
        if date_obj is None:
            date_obj = datetime.now()
        
        fin_period = int(f"{date_obj.year}{date_obj.month:02d}")
        
        return {
            'fin_period': fin_period,
            'year': date_obj.year,
            'month': date_obj.month,
            'quarter': ((date_obj.month - 1) // 3) + 1
        }
    
    def enhance_change_event(self, change_doc: Dict[str, Any], collection_name: str) -> Dict[str, Any]:
        """
        Add business context to change events
        This enriches raw MongoDB changes with ClearVue business intelligence
        """
        enhanced_event = {
            'event_id': str(bson.ObjectId()),
            'timestamp': datetime.now().isoformat(),
            'source': 'clearvue_mongodb_atlas',
            'database': self.db.name,
            'collection': collection_name,
            'operation': change_doc['operationType'],
            'document_key': change_doc.get('documentKey', {}),
            'change_data': {},
            'business_context': {}
        }
        
        # Extract changed data
        if 'fullDocument' in change_doc:
            enhanced_event['change_data'] = change_doc['fullDocument']
        elif 'updateDescription' in change_doc:
            enhanced_event['change_data'] = change_doc['updateDescription']
        
        # Add collection-specific business context
        doc = change_doc.get('fullDocument', {})
        
        if collection_name == 'sales_headers' and doc:
            trans_date = doc.get('TRANS_DATE', datetime.now())
            fin_info = self.get_financial_period(trans_date)
            
            enhanced_event['business_context'] = {
                'transaction_type': 'sales',
                'doc_number': doc.get('DOC_NUMBER'),
                'customer_number': doc.get('CUSTOMER_NUMBER'),
                'rep_code': doc.get('REP_CODE'),
                'trans_type': doc.get('TRANSTYPE_CODE'),
                'fin_period': doc.get('FIN_PERIOD', fin_info['fin_period']),
                'financial_year': fin_info['year'],
                'financial_quarter': fin_info['quarter'],
                'priority': 'high'  # Sales are high priority
            }
        
        elif collection_name == 'payment_headers' and doc:
            enhanced_event['business_context'] = {
                'transaction_type': 'payment_header',
                'customer_number': doc.get('CUSTOMER_NUMBER'),
                'deposit_ref': doc.get('DEPOSIT_REF'),
                'priority': 'high'
            }
        
        elif collection_name == 'payment_lines' and doc:
            deposit_date = doc.get('DEPOSIT_DATE', datetime.now())
            fin_info = self.get_financial_period(deposit_date)
            
            enhanced_event['business_context'] = {
                'transaction_type': 'payment_line',
                'customer_number': doc.get('CUSTOMER_NUMBER'),
                'deposit_ref': doc.get('DEPOSIT_REF'),
                'fin_period': doc.get('FIN_PERIOD', fin_info['fin_period']),
                'bank_amount': doc.get('BANK_AMT'),
                'total_payment': doc.get('TOT_PAYMENT'),
                'discount': doc.get('DISCOUNT'),
                'priority': 'high'
            }
        
        elif collection_name == 'purchases_headers' and doc:
            purch_date = doc.get('PURCH_DATE', datetime.now())
            fin_info = self.get_financial_period(purch_date)
            
            enhanced_event['business_context'] = {
                'transaction_type': 'purchase_header',
                'purch_doc_no': doc.get('PURCH_DOC_NO'),
                'supplier_code': doc.get('SUPPLIER_CODE'),
                'financial_year': fin_info['year'],
                'financial_month': fin_info['month'],
                'priority': 'medium'
            }
        
        elif collection_name == 'purchases_lines' and doc:
            enhanced_event['business_context'] = {
                'transaction_type': 'purchase_line',
                'purch_doc_no': doc.get('PURCH_DOC_NO'),
                'inventory_code': doc.get('INVENTORY_CODE'),
                'quantity': doc.get('QUANTITY'),
                'unit_cost': doc.get('UNIT_COST_PRICE'),
                'total_cost': doc.get('TOTAL_LINE_COST'),
                'priority': 'medium'
            }
        
        elif collection_name == 'customers' and doc:
            enhanced_event['business_context'] = {
                'transaction_type': 'customer_change',
                'customer_number': doc.get('CUSTOMER_NUMBER'),
                'region_code': doc.get('REGION_CODE'),
                'rep_code': doc.get('REP_CODE'),
                'category': doc.get('CCAT_CODE'),
                'credit_limit': doc.get('CREDIT_LIMIT'),
                'priority': 'medium'
            }
        
        elif collection_name == 'products' and doc:
            enhanced_event['business_context'] = {
                'transaction_type': 'product_change',
                'inventory_code': doc.get('INVENTORY_CODE'),
                'category_code': doc.get('PRODCAT_CODE'),
                'last_cost': doc.get('LAST_COST'),
                'stock_indicator': doc.get('STOCK_IND'),
                'priority': 'medium'
            }
        
        elif collection_name == 'suppliers' and doc:
            enhanced_event['business_context'] = {
                'transaction_type': 'supplier_change',
                'supplier_code': doc.get('SUPPLIER_CODE'),
                'supplier_desc': doc.get('SUPPLIER_DESC'),
                'credit_limit': doc.get('CREDIT_LIMIT'),
                'priority': 'low'
            }
        
        elif collection_name == 'age_analysis' and doc:
            enhanced_event['business_context'] = {
                'transaction_type': 'age_analysis',
                'customer_number': doc.get('CUSTOMER_NUMBER'),
                'fin_period': doc.get('FIN_PERIOD'),
                'total_due': doc.get('TOTAL_DUE'),
                'current': doc.get('AMT_CURRENT'),
                'overdue_30': doc.get('AMT_30_DAYS'),
                'overdue_60': doc.get('AMT_60_DAYS'),
                'overdue_90': doc.get('AMT_90_DAYS'),
                'priority': 'medium'
            }
        
        else:
            # Generic context for other collections
            enhanced_event['business_context'] = {
                'transaction_type': f'{collection_name}_change',
                'priority': 'low'
            }
        
        return enhanced_event
    
    def send_to_kafka(self, topic: str, key: str, message: Dict[str, Any]) -> bool:
        """Send message to Kafka with retry logic"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                future = self.kafka_producer.send(topic, key=key, value=message)
                record_metadata = future.get(timeout=10)
                
                logger.debug(
                    f"Sent to Kafka - Topic: {topic}, "
                    f"Partition: {record_metadata.partition}, "
                    f"Offset: {record_metadata.offset}"
                )
                return True
                
            except KafkaError as e:
                retry_count += 1
                logger.error(f"Kafka error (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    time.sleep(1)
                
            except Exception as e:
                logger.error(f"Unexpected error sending to Kafka: {e}")
                self.stats['errors'] += 1
                return False
        
        logger.error(f"Failed to send message after {max_retries} attempts")
        self.stats['errors'] += 1
        return False
    
    def process_change_event(self, change_doc: Dict[str, Any], collection_name: str):
        """Process a single change event"""
        try:
            # Enhance with business context
            enhanced_event = self.enhance_change_event(change_doc, collection_name)
            
            # Get Kafka topic
            topic = self.topic_mapping.get(collection_name)
            if not topic:
                logger.warning(f"No topic mapping for collection: {collection_name}")
                return
            
            # Create message key
            doc_key = change_doc.get('documentKey', {})
            message_key = doc_key.get('_id', enhanced_event['event_id'])
            
            # Send to Kafka
            success = self.send_to_kafka(topic, str(message_key), enhanced_event)
            
            if success:
                # Update statistics
                self.stats['total_changes'] += 1
                
                if collection_name not in self.stats['changes_by_collection']:
                    self.stats['changes_by_collection'][collection_name] = 0
                self.stats['changes_by_collection'][collection_name] += 1
                
                operation = change_doc['operationType']
                if operation not in self.stats['changes_by_operation']:
                    self.stats['changes_by_operation'][operation] = 0
                self.stats['changes_by_operation'][operation] += 1
                
                self.stats['last_processed'] = datetime.now().isoformat()
                
                # Log based on priority
                priority = enhanced_event['business_context'].get('priority', 'low')
                if priority == 'high':
                    logger.info(
                        f"⚡ HIGH PRIORITY: {operation} on {collection_name} "
                        f"(Total: {self.stats['total_changes']})"
                    )
                else:
                    logger.debug(f"Processed {operation} on {collection_name}")
        
        except Exception as e:
            logger.error(f"Error processing change event: {e}")
            self.stats['errors'] += 1
    
    def watch_collection(self, collection_name: str):
        """Watch a collection for changes"""
        collection = self.db[collection_name]
        
        pipeline = [
            {
                '$match': {
                    'operationType': {'$in': ['insert', 'update', 'delete']}
                }
            }
        ]
        
        options = {
            'full_document': 'updateLookup',
            'max_await_time_ms': 1000
        }
        
        logger.info(f"Started watching: {collection_name}")
        print(f"👁️  Watching {collection_name}")
        
        try:
            with collection.watch(pipeline, **options) as stream:
                self.change_streams[collection_name] = stream
                
                for change in stream:
                    if not self.is_running:
                        break
                    self.process_change_event(change, collection_name)
                    
        except Exception as e:
            logger.error(f"Error in change stream for {collection_name}: {e}")
            print(f"❌ Stream error for {collection_name}: {e}")
        finally:
            if collection_name in self.change_streams:
                del self.change_streams[collection_name]
            logger.info(f"Closed stream: {collection_name}")
    
    def start_pipeline(self, collections=None, priority_filter=None):
        """
        Start the streaming pipeline
        
        Args:
            collections: List of collections to watch (None = all)
            priority_filter: Filter collections by priority ('high', 'medium', 'low')
        """
        if self.is_running:
            print("⚠️  Pipeline already running!")
            return
        
        # Default to all collections
        if collections is None:
            collections = list(self.topic_mapping.keys())
        
        # Apply priority filter if specified
        if priority_filter:
            priority_map = {
                'high': ['sales_headers', 'payment_headers', 'payment_lines'],
                'medium': ['purchases_headers', 'purchases_lines', 'customers', 
                          'products', 'suppliers', 'age_analysis'],
                'low': ['product_brands', 'product_categories', 'product_ranges',
                       'products_styles', 'representatives', 'customer_regions',
                       'customer_categories', 'trans_types']
            }
            if priority_filter in priority_map:
                collections = [c for c in collections if c in priority_map[priority_filter]]
        
        print("\n" + "="*60)
        print("ClearVue Streaming Pipeline - STARTING")
        print("="*60)
        print(f"📊 Database: {self.db.name}")
        print(f"🎯 Collections: {len(collections)}")
        if priority_filter:
            print(f"🔍 Priority Filter: {priority_filter}")
        print("="*60 + "\n")
        
        # Create topics
        self.create_kafka_topics()
        
        # Start pipeline
        self.is_running = True
        self.stats['start_time'] = datetime.now().isoformat()
        
        # Start watchers
        threads = []
        for collection_name in collections:
            if collection_name in self.topic_mapping:
                thread = threading.Thread(
                    target=self.watch_collection,
                    args=(collection_name,),
                    name=f"ChangeStream-{collection_name}"
                )
                thread.daemon = True
                thread.start()
                threads.append(thread)
        
        print(f"✅ Started {len(threads)} change stream watchers\n")
        
        # Signal handlers
        def signal_handler(signum, frame):
            print("\n⚠️  Shutdown signal received")
            self.stop_pipeline()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        print("🟢 Pipeline is RUNNING!")
        print("💡 Waiting for database changes...")
        print("⌨️  Press Ctrl+C to stop\n")
        
        # Keep alive and show stats
        try:
            stats_interval = 30
            last_stats_time = datetime.now()
            
            while self.is_running and any(t.is_alive() for t in threads):
                time.sleep(1)
                
                now = datetime.now()
                if (now - last_stats_time).seconds >= stats_interval:
                    self.print_stats()
                    last_stats_time = now
                    
        except KeyboardInterrupt:
            print("\n⚠️  Manual shutdown requested")
            self.stop_pipeline()
        
        # Wait for threads
        print("\n⏳ Waiting for threads to finish...")
        for thread in threads:
            thread.join(timeout=5)
        
        print("\n✅ Pipeline stopped successfully")
    
    def stop_pipeline(self):
        """Stop the pipeline cleanly"""
        print("\n🛑 Stopping pipeline...")
        self.is_running = False
        
        # Close streams
        for collection_name in list(self.change_streams.keys()):
            try:
                self.change_streams[collection_name].close()
            except:
                pass
        
        # Close Kafka
        try:
            self.kafka_producer.close()
            print("✅ Kafka producer closed")
        except:
            pass
        
        # Close MongoDB
        try:
            self.mongo_client.close()
            print("✅ MongoDB connection closed")
        except:
            pass
        
        # Final stats
        self.print_stats(final=True)
    
    def print_stats(self, final=False):
        """Print pipeline statistics"""
        uptime = "Unknown"
        if self.stats['start_time']:
            start = datetime.fromisoformat(self.stats['start_time'])
            uptime = str(datetime.now() - start).split('.')[0]
        
        print("\n" + "="*60)
        print("PIPELINE STATISTICS" + (" - FINAL" if final else ""))
        print("="*60)
        print(f"⏱️  Uptime: {uptime}")
        print(f"📊 Total changes: {self.stats['total_changes']}")
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"🕐 Last processed: {self.stats['last_processed']}")
        
        if self.stats['changes_by_collection']:
            print("\n📋 Changes by collection:")
            for coll, count in sorted(self.stats['changes_by_collection'].items(), 
                                     key=lambda x: x[1], reverse=True):
                print(f"   {coll}: {count}")
        
        if self.stats['changes_by_operation']:
            print("\n⚙️  Changes by operation:")
            for op, count in self.stats['changes_by_operation'].items():
                print(f"   {op}: {count}")
        
        print("="*60 + "\n")


if __name__ == "__main__":
    print("="*60)
    print("ClearVue Streaming Pipeline - Real Schema")
    print("="*60)
    print("\nOptions:")
    print("1. Monitor ALL collections")
    print("2. Monitor HIGH priority only (sales, payments)")
    print("3. Monitor MEDIUM priority (purchases, customers, products)")
    print("4. Monitor specific collections")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    pipeline = None
    try:
        pipeline = ClearVueStreamingPipeline()
        
        if choice == "1":
            pipeline.start_pipeline()
        elif choice == "2":
            pipeline.start_pipeline(priority_filter='high')
        elif choice == "3":
            pipeline.start_pipeline(priority_filter='medium')
        elif choice == "4":
            print("\nAvailable collections:")
            for i, coll in enumerate(pipeline.topic_mapping.keys(), 1):
                print(f"{i}. {coll}")
            selected = input("\nEnter collection names (comma-separated): ").strip()
            colls = [c.strip() for c in selected.split(',')]
            pipeline.start_pipeline(collections=colls)
        else:
            print("Invalid option")
            
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logger.error(f"Fatal error: {e}")
    finally:
        if pipeline:
            pipeline.stop_pipeline()