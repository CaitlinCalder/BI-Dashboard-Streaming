"""
ClearVue Streaming Pipeline - Final Production Version
Detects changes in MongoDB Atlas (6 collections) and streams to Kafka for Power BI

Real-time Detection:
- Monitors 6 main collections (customers, suppliers, products, sales, payments, purchases)
- Detects INSERT/UPDATE/DELETE operations via Change Streams
- Enriches with business context and financial period information
- Streams to Kafka topics for Power BI real-time dashboard

Author: ClearVue Streaming Team
Database: Nova_Analytics (MongoDB Atlas)
Date: October 2025
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import threading
import signal
import sys
import time

from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import KafkaError
import bson

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('streaming_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ClearVueStreamingPipeline')

class ClearVueStreamingPipeline:
    """
    Main streaming pipeline class
    Connects MongoDB Atlas to Kafka for real-time BI
    """
    
    def __init__(self, 
                 mongo_uri=None,
                 kafka_servers=None,
                 database_name=None):
        """
        Initialize the streaming pipeline
        
        Args:
            mongo_uri: MongoDB Atlas connection string
            kafka_servers: Kafka bootstrap servers
            database_name: MongoDB database name
        """
        
        print("\n" + "="*70)
        print("ClearVue Streaming Pipeline - Initializing")
        print("="*70)
        
        # Connect to MongoDB Atlas
        print("🔌 Connecting to MongoDB Atlas...")
        try:
            self.mongo_client = MongoClient(
                mongo_uri, 
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000
            )
            # Test connection
            self.mongo_client.admin.command('ping')
            print("✅ MongoDB Atlas connection successful!")
            logger.info("Connected to MongoDB Atlas")
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
            logger.error(f"MongoDB connection failed: {e}")
            raise
        
        self.db = self.mongo_client[database_name]
        
        # Kafka Producer setup
        print("🔧 Setting up Kafka producer...")
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
                enable_idempotence=True,
                compression_type='gzip'  # Compress messages
            )
            print("✅ Kafka producer ready!")
            logger.info("Kafka producer initialized")
        except Exception as e:
            print(f"❌ Kafka setup failed: {e}")
            logger.error(f"Kafka setup failed: {e}")
            raise
        
        # Pipeline control
        self.is_running = False
        self.change_streams = {}
        
        # Statistics tracking
        self.stats = {
            'total_changes': 0,
            'changes_by_collection': {},
            'changes_by_operation': {},
            'high_priority_events': 0,
            'last_processed': None,
            'start_time': None,
            'errors': 0
        }
        
        # Topic mapping for 6 main collections
        # These align with your Deliverable 1 conceptual design
        self.topic_mapping = {
            # Transaction collections (HIGH PRIORITY for real-time BI)
            'sales': 'clearvue.sales.realtime',
            'payments': 'clearvue.payments.realtime',
            'purchases': 'clearvue.purchases.realtime',
            
            # Master data collections (MEDIUM PRIORITY)
            'customers': 'clearvue.customers.changes',
            'products': 'clearvue.products.changes',
            'suppliers': 'clearvue.suppliers.changes'
        }
        
        # Priority levels for different collections
        self.collection_priority = {
            'sales': 'HIGH',
            'payments': 'HIGH',
            'purchases': 'MEDIUM',
            'customers': 'MEDIUM',
            'products': 'MEDIUM',
            'suppliers': 'LOW'
        }
        
        logger.info(f"Pipeline initialized for database: {database_name}")
        logger.info(f"Monitoring {len(self.topic_mapping)} collections")
        print(f"📊 Configured to monitor {len(self.topic_mapping)} collections")
        print("="*70 + "\n")
    
    def json_serializer(self, obj):
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
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            print("📡 Creating/verifying Kafka topics...")
            admin_client = KafkaAdminClient(
                bootstrap_servers=['localhost:29092'],
                client_id='clearvue_admin'
            )
            
            # Define topics with appropriate partitions based on priority
            topics_to_create = []
            for collection, topic_name in self.topic_mapping.items():
                priority = self.collection_priority[collection]
                # High priority gets more partitions for parallelism
                partitions = 5 if priority == 'HIGH' else 3
                
                new_topic = NewTopic(
                    name=topic_name,
                    num_partitions=partitions,
                    replication_factor=1
                )
                topics_to_create.append(new_topic)
            
            try:
                admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
                logger.info(f"Created {len(topics_to_create)} Kafka topics")
                print(f"✅ Created/verified {len(topics_to_create)} Kafka topics")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print("✅ Kafka topics already exist")
                    logger.info("Kafka topics already exist")
                else:
                    raise
            
        except Exception as e:
            logger.warning(f"Topic creation warning: {e}")
            print(f"⚠️  Topic warning (may already exist): {e}")
    
    def calculate_financial_period(self, date_obj=None):
        """
        Calculate ClearVue financial period
        Returns YYYYMM format and additional context
        """
        if date_obj is None:
            date_obj = datetime.now()
        
        fin_period = int(f"{date_obj.year}{date_obj.month:02d}")
        quarter = ((date_obj.month - 1) // 3) + 1
        
        return {
            'fin_period': fin_period,
            'year': date_obj.year,
            'month': date_obj.month,
            'quarter': quarter,
            'month_name': date_obj.strftime('%B'),
            'quarter_name': f"Q{quarter}"
        }
    
    def enhance_change_event(self, change_doc: Dict[str, Any], collection_name: str) -> Dict[str, Any]:
        """
        Transform raw MongoDB change event into enriched message for Power BI
        Adds business context, financial periods, and metadata
        """
        
        # Base event structure
        enhanced_event = {
            'event_id': str(bson.ObjectId()),
            'timestamp': datetime.now().isoformat(),
            'source': 'mongodb_atlas',
            'database': self.db.name,
            'collection': collection_name,
            'operation': change_doc['operationType'],
            'priority': self.collection_priority.get(collection_name, 'LOW'),
            'document_key': change_doc.get('documentKey', {}),
            'change_data': {},
            'business_context': {}
        }
        
        # Extract the actual changed data
        if 'fullDocument' in change_doc:
            enhanced_event['change_data'] = change_doc['fullDocument']
        elif 'updateDescription' in change_doc:
            enhanced_event['change_data'] = {
                'updated_fields': change_doc['updateDescription'].get('updatedFields', {}),
                'removed_fields': change_doc['updateDescription'].get('removedFields', [])
            }
        
        doc = change_doc.get('fullDocument', {})
        
        # Add collection-specific business context
        if collection_name == 'sales' and doc:
            # Sales transaction enrichment
            trans_date = doc.get('trans_date', datetime.now())
            fin_info = self.calculate_financial_period(trans_date)
            
            # Calculate total from embedded lines
            total_amount = 0
            if 'lines' in doc and isinstance(doc['lines'], list):
                total_amount = sum(line.get('total_line_cost', 0) for line in doc['lines'])
            
            enhanced_event['business_context'] = {
                'transaction_type': 'sales',
                'doc_number': doc.get('_id'),
                'customer_id': doc.get('customer_id'),
                'rep_id': doc.get('rep', {}).get('id') if isinstance(doc.get('rep'), dict) else doc.get('rep'),
                'trans_type': doc.get('trans_type'),
                'fin_period': doc.get('fin_period', fin_info['fin_period']),
                'financial_year': fin_info['year'],
                'financial_quarter': fin_info['quarter_name'],
                'total_amount': total_amount,
                'line_count': len(doc.get('lines', [])),
                'metric_category': 'revenue'
            }
        
        elif collection_name == 'payments' and doc:
            # Payment transaction enrichment
            deposit_date = None
            total_payment = 0
            total_discount = 0
            
            # Extract from embedded payment lines
            if 'lines' in doc and isinstance(doc['lines'], list):
                for line in doc['lines']:
                    if not deposit_date and 'deposit_date' in line:
                        deposit_date = line['deposit_date']
                    total_payment += line.get('tot_payment', 0)
                    total_discount += line.get('discount', 0)
            
            fin_info = self.calculate_financial_period(deposit_date or datetime.now())
            
            enhanced_event['business_context'] = {
                'transaction_type': 'payment',
                'customer_id': doc.get('customer_id'),
                'deposit_ref': doc.get('deposit_ref'),
                'fin_period': fin_info['fin_period'],
                'financial_year': fin_info['year'],
                'financial_quarter': fin_info['quarter_name'],
                'total_payment': round(total_payment, 2),
                'total_discount': round(total_discount, 2),
                'payment_count': len(doc.get('lines', [])),
                'metric_category': 'cash_flow'
            }
        
        elif collection_name == 'purchases' and doc:
            # Purchase order enrichment
            purch_date = doc.get('purch_date', datetime.now())
            fin_info = self.calculate_financial_period(purch_date)
            
            # Calculate total from lines
            total_cost = 0
            if 'lines' in doc and isinstance(doc['lines'], list):
                total_cost = sum(line.get('total_line_cost', 0) for line in doc['lines'])
            
            enhanced_event['business_context'] = {
                'transaction_type': 'purchase',
                'purch_doc_no': doc.get('_id'),
                'supplier_id': doc.get('supplier_id'),
                'fin_period': fin_info['fin_period'],
                'financial_year': fin_info['year'],
                'financial_quarter': fin_info['quarter_name'],
                'total_cost': round(total_cost, 2),
                'line_count': len(doc.get('lines', [])),
                'metric_category': 'procurement'
            }
        
        elif collection_name == 'customers' and doc:
            # Customer master data enrichment
            enhanced_event['business_context'] = {
                'entity_type': 'customer',
                'customer_id': doc.get('_id'),
                'customer_name': doc.get('name'),
                'region': doc.get('region', {}).get('desc') if isinstance(doc.get('region'), dict) else None,
                'category': doc.get('category', {}).get('desc') if isinstance(doc.get('category'), dict) else None,
                'credit_limit': doc.get('credit_limit'),
                'total_due': doc.get('age_analysis', {}).get('total_due', 0) if isinstance(doc.get('age_analysis'), dict) else 0,
                'metric_category': 'customer_master'
            }
        
        elif collection_name == 'products' and doc:
            # Product master data enrichment
            enhanced_event['business_context'] = {
                'entity_type': 'product',
                'inventory_code': doc.get('inventory_code'),
                'brand': doc.get('brand', {}).get('desc') if isinstance(doc.get('brand'), dict) else None,
                'category': doc.get('category', {}).get('desc') if isinstance(doc.get('category'), dict) else None,
                'last_cost': doc.get('last_cost'),
                'in_stock': doc.get('stock_ind'),
                'metric_category': 'product_master'
            }
        
        elif collection_name == 'suppliers' and doc:
            # Supplier master data enrichment
            enhanced_event['business_context'] = {
                'entity_type': 'supplier',
                'supplier_id': doc.get('_id'),
                'supplier_name': doc.get('description'),
                'exclusive': doc.get('exclusive'),
                'credit_limit': doc.get('credit_limit'),
                'payment_terms': doc.get('normal_payterms'),
                'metric_category': 'supplier_master'
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
                
                logger.debug(
                    f"Kafka: {topic} | Partition: {record_metadata.partition} | "
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
        
        logger.error(f"Failed to send to Kafka after {max_retries} attempts")
        self.stats['errors'] += 1
        return False
    
    def process_change_event(self, change_doc: Dict[str, Any], collection_name: str):
        """Process a detected change event"""
        try:
            # Enrich the change event
            enhanced_event = self.enhance_change_event(change_doc, collection_name)
            
            # Get Kafka topic
            topic = self.topic_mapping.get(collection_name)
            if not topic:
                logger.warning(f"No topic mapping for: {collection_name}")
                return
            
            # Create message key for partitioning
            doc_key = change_doc.get('documentKey', {})
            message_key = str(doc_key.get('_id', enhanced_event['event_id']))
            
            # Send to Kafka
            success = self.send_to_kafka(topic, message_key, enhanced_event)
            
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
                
                if enhanced_event['priority'] == 'HIGH':
                    self.stats['high_priority_events'] += 1
                
                self.stats['last_processed'] = datetime.now().isoformat()
                
                # Log high priority events prominently
                if enhanced_event['priority'] == 'HIGH':
                    logger.info(
                        f"⚡ HIGH PRIORITY: {operation} on {collection_name} | "
                        f"Total: {self.stats['total_changes']}"
                    )
                    print(f"⚡ {collection_name.upper()}: {operation} detected")
                else:
                    logger.debug(f"Processed {operation} on {collection_name}")
        
        except Exception as e:
            logger.error(f"Error processing change event: {e}", exc_info=True)
            self.stats['errors'] += 1
    
    def watch_collection(self, collection_name: str):
        """Watch a specific collection for changes using Change Streams"""
        collection = self.db[collection_name]
        
        # Change stream pipeline - only watch insert, update, delete
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
        
        logger.info(f"Started change stream: {collection_name}")
        print(f"👁️  Watching: {collection_name}")
        
        try:
            with collection.watch(pipeline, **options) as stream:
                self.change_streams[collection_name] = stream
                
                for change in stream:
                    if not self.is_running:
                        break
                    self.process_change_event(change, collection_name)
                    
        except Exception as e:
            logger.error(f"Change stream error for {collection_name}: {e}", exc_info=True)
            print(f"❌ Stream error for {collection_name}: {e}")
        finally:
            if collection_name in self.change_streams:
                del self.change_streams[collection_name]
            logger.info(f"Change stream closed: {collection_name}")
    
    def start_pipeline(self, collections: List[str] = None):
        """
        Start the streaming pipeline
        
        Args:
            collections: List of collections to monitor (None = all 6)
        """
        if self.is_running:
            print("⚠️  Pipeline is already running!")
            return
        
        # Default to all 6 collections
        if collections is None:
            collections = list(self.topic_mapping.keys())
        
        print("\n" + "="*70)
        print("CLEARVUE STREAMING PIPELINE - STARTING")
        print("="*70)
        print(f"📊 Database: {self.db.name}")
        print(f"🎯 Collections: {', '.join(collections)}")
        print(f"📡 Kafka Topics: {len(self.topic_mapping)}")
        print("="*70 + "\n")
        
        # Create Kafka topics
        self.create_kafka_topics()
        
        # Mark as running
        self.is_running = True
        self.stats['start_time'] = datetime.now().isoformat()
        
        # Start change stream watchers
        threads = []
        for collection_name in collections:
            if collection_name in self.topic_mapping:
                thread = threading.Thread(
                    target=self.watch_collection,
                    args=(collection_name,),
                    name=f"Stream-{collection_name}",
                    daemon=True
                )
                thread.start()
                threads.append(thread)
            else:
                print(f"⚠️  Unknown collection: {collection_name}")
        
        print(f"✅ Started {len(threads)} change stream watchers\n")
        logger.info(f"Pipeline started with {len(threads)} watchers")
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            print("\n⚠️  Shutdown signal received")
            logger.info("Shutdown signal received")
            self.stop_pipeline()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        print("🟢 PIPELINE IS RUNNING!")
        print("💡 Waiting for database changes...")
        print("⌨️  Press Ctrl+C to stop\n")
        
        # Keep main thread alive and print stats periodically
        try:
            stats_interval = 30  # Print stats every 30 seconds
            last_stats_time = datetime.now()
            
            while self.is_running and any(t.is_alive() for t in threads):
                time.sleep(1)
                
                now = datetime.now()
                if (now - last_stats_time).seconds >= stats_interval:
                    self.print_stats()
                    last_stats_time = now
                    
        except KeyboardInterrupt:
            print("\n⚠️  Manual shutdown requested")
            logger.info("Manual shutdown requested")
            self.stop_pipeline()
        
        # Wait for threads to finish
        print("\n⏳ Waiting for threads to finish...")
        for thread in threads:
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
                print(f"   Closed stream: {collection_name}")
            except Exception as e:
                logger.error(f"Error closing stream {collection_name}: {e}")
        
        # Close Kafka producer
        try:
            self.kafka_producer.close()
            print("   Closed Kafka producer")
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        
        # Close MongoDB connection
        try:
            self.mongo_client.close()
            print("   Closed MongoDB connection")
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB: {e}")
        
        # Print final statistics
        self.print_stats(final=True)
    
    def print_stats(self, final=False):
        """Print pipeline statistics"""
        uptime = "Unknown"
        if self.stats['start_time']:
            start = datetime.fromisoformat(self.stats['start_time'])
            uptime = str(datetime.now() - start).split('.')[0]
        
        print("\n" + "="*70)
        print(f"PIPELINE STATISTICS{' - FINAL REPORT' if final else ''}")
        print("="*70)
        print(f"⏱️  Uptime: {uptime}")
        print(f"📊 Total changes: {self.stats['total_changes']}")
        print(f"⚡ High priority: {self.stats['high_priority_events']}")
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"🕐 Last processed: {self.stats['last_processed']}")
        
        if self.stats['changes_by_collection']:
            print("\n📋 Changes by collection:")
            for coll, count in sorted(
                self.stats['changes_by_collection'].items(),
                key=lambda x: x[1],
                reverse=True
            ):
                priority = self.collection_priority.get(coll, 'LOW')
                print(f"   {coll:15} : {count:5} ({priority})")
        
        if self.stats['changes_by_operation']:
            print("\n⚙️  Changes by operation:")
            for op, count in self.stats['changes_by_operation'].items():
                print(f"   {op:10} : {count}")
        
        print("="*70 + "\n")


def main():
    """Main entry point"""
    print("\n" + "="*70)
    print("CLEARVUE STREAMING PIPELINE")
    print("Real-time Data Streaming for BI Dashboard")
    print("="*70)
    print("\nMonitoring Options:")
    print("1. ALL collections (sales, payments, purchases, customers, products, suppliers)")
    print("2. TRANSACTION data only (sales, payments, purchases)")
    print("3. MASTER data only (customers, products, suppliers)")
    print("4. CUSTOM selection")
    
    choice = input("\nSelect option (1-4) [default: 1]: ").strip() or "1"
    
    pipeline = None
    try:
        pipeline = ClearVueStreamingPipeline()
        
        if choice == "1":
            # Monitor all 6 collections
            pipeline.start_pipeline()
        elif choice == "2":
            # Transaction data only
            pipeline.start_pipeline(collections=['sales', 'payments', 'purchases'])
        elif choice == "3":
            # Master data only
            pipeline.start_pipeline(collections=['customers', 'products', 'suppliers'])
        elif choice == "4":
            # Custom selection
            print("\nAvailable collections:")
            for i, coll in enumerate(pipeline.topic_mapping.keys(), 1):
                priority = pipeline.collection_priority[coll]
                print(f"{i}. {coll} ({priority} priority)")
            
            selected = input("\nEnter collection names (comma-separated): ").strip()
            if selected:
                colls = [c.strip() for c in selected.split(',')]
                pipeline.start_pipeline(collections=colls)
            else:
                print("No collections selected, exiting...")
        else:
            print("Invalid option")
            
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if pipeline:
            pipeline.stop_pipeline()


if __name__ == "__main__":
    main()