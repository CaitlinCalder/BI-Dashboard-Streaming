"""
Fixed ClearVue Streaming Pipeline with Power BI Refresh Integration
"""

import json
import logging
import signal
import sys
import time
import threading
from threading import Thread, Lock
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import bson
import requests

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


class PowerBIRefreshTrigger:
    """Smart refresh trigger for Power BI datasets"""
    
    def __init__(self, 
                 workspace_id: str,
                 dataset_id: str,
                 access_token: str,
                 min_refresh_interval: int = 60,
                 batch_window: int = 30):
        
        self.workspace_id = workspace_id
        self.dataset_id = dataset_id
        self.access_token = access_token
        self.min_refresh_interval = min_refresh_interval
        self.batch_window = batch_window
        
        self.pending_refresh = False
        self.last_refresh_time = None
        self.pending_changes_count = 0
        self.lock = Lock()
        
        self.stats = {
            'total_changes_detected': 0,
            'refresh_requests_sent': 0,
            'refresh_requests_succeeded': 0,
            'refresh_requests_failed': 0,
            'last_refresh_status': None,
            'last_refresh_time': None
        }
        
        self.refresh_thread = None
        self.is_running = False
        
        self.api_base = "https://api.powerbi.com/v1.0/myorg"
        self.refresh_url = f"{self.api_base}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        
        logger.info(f"Initialized PowerBI Refresh Trigger for dataset {dataset_id}")
    
    def on_data_change(self, collection: str, operation: str, doc_id: str = None):
        with self.lock:
            self.pending_refresh = True
            self.pending_changes_count += 1
            self.stats['total_changes_detected'] += 1
        
        logger.debug(f"Change detected: {collection}.{operation} (ID: {doc_id})")
        
        if not self.is_running:
            self.start_batch_window()
    
    def start_batch_window(self):
        if self.refresh_thread and self.refresh_thread.is_alive():
            return
        
        self.is_running = True
        self.refresh_thread = Thread(target=self._batch_window_worker, daemon=True)
        self.refresh_thread.start()
    
    def _batch_window_worker(self):
        logger.info(f"Batch window started: waiting {self.batch_window}s for more changes...")
        time.sleep(self.batch_window)
        
        with self.lock:
            if not self.pending_refresh:
                self.is_running = False
                return
            
            if self.last_refresh_time:
                elapsed = (datetime.now() - self.last_refresh_time).total_seconds()
                if elapsed < self.min_refresh_interval:
                    wait_time = self.min_refresh_interval - elapsed
                    logger.info(f"Too soon to refresh, waiting {wait_time:.0f}s more...")
                    time.sleep(wait_time)
            
            changes_count = self.pending_changes_count
            self.pending_changes_count = 0
            self.pending_refresh = False
        
        logger.info(f"Triggering Power BI refresh for {changes_count} batched changes...")
        success = self.trigger_refresh()
        
        if success:
            logger.info(f"✅ Power BI refresh triggered successfully")
        else:
            logger.error(f"❌ Power BI refresh failed")
        
        self.is_running = False
    
    def trigger_refresh(self) -> bool:
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            payload = {"notifyOption": "NoNotification"}
            
            response = requests.post(
                self.refresh_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            
            self.stats['refresh_requests_sent'] += 1
            
            if response.status_code == 202:
                self.stats['refresh_requests_succeeded'] += 1
                self.stats['last_refresh_status'] = 'success'
                self.stats['last_refresh_time'] = datetime.now().isoformat()
                self.last_refresh_time = datetime.now()
                return True
            else:
                self.stats['refresh_requests_failed'] += 1
                self.stats['last_refresh_status'] = f'failed_{response.status_code}'
                logger.error(f"Power BI API error: {response.status_code} - {response.text}")
                return False
        
        except Exception as e:
            self.stats['refresh_requests_failed'] += 1
            self.stats['last_refresh_status'] = f'error: {str(e)}'
            logger.error(f"Power BI refresh error: {e}", exc_info=True)
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return {
                **self.stats,
                'pending_changes': self.pending_changes_count,
                'refresh_pending': self.pending_refresh,
                'batch_window_active': self.is_running
            }


class ClearVueStreamingPipeline:
    """Production-ready streaming pipeline for ClearVue BI"""
    
    def __init__(self):
        print_startup_banner()
        logger.info("Initializing ClearVue Streaming Pipeline...")
        
        self.mongo_uri = ClearVueConfig.get_mongo_uri()
        self.db_name = ClearVueConfig.get_database_name()
        self.kafka_servers = ClearVueConfig.get_kafka_servers()
        
        ClearVueConfig.print_config(detailed=True)
        
        self._connect_mongodb()
        self._setup_kafka_producer()
        
        # ✅ CRITICAL FIX: Initialize Power BI trigger
        self._setup_powerbi_refresh_trigger()
        
        self.is_running = False
        self.change_streams = {}
        self.stream_threads = []
        
        self.stats = {
            'total_changes': 0,
            'changes_by_collection': defaultdict(int),
            'changes_by_operation': defaultdict(int),
            'high_priority_events': 0,
            'last_processed': None,
            'start_time': None,
            'errors': 0,
            'kafka_send_errors': 0,
            'mongodb_errors': 0
        }
        
        self.last_health_check = datetime.now()
        self.is_healthy = True
        
        logger.info("Pipeline initialized successfully")
    
    def _setup_powerbi_refresh_trigger(self):
        """Setup Power BI refresh trigger"""
        try:
            workspace_id = ClearVueConfig.get_powerbi_workspace_id()
            dataset_id = ClearVueConfig.get_powerbi_dataset_id()
            access_token = ClearVueConfig.get_powerbi_access_token()
            
            if not all([workspace_id, dataset_id, access_token]):
                logger.warning("Power BI refresh trigger not configured - skipping")
                print("⚠️  Power BI refresh trigger not configured")
                self.powerbi_trigger = None
                return
            
            self.powerbi_trigger = PowerBIRefreshTrigger(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                access_token=access_token,
                min_refresh_interval=60,
                batch_window=30
            )
            
            print("✅ Power BI refresh trigger initialized")
            logger.info("Power BI refresh trigger ready")
            
        except Exception as e:
            logger.error(f"Failed to setup Power BI trigger: {e}")
            print(f"⚠️  Power BI trigger setup failed: {e}")
            self.powerbi_trigger = None
    
    def _connect_mongodb(self):
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
            
            self.mongo_client.admin.command('ping')
            self.db = self.mongo_client[self.db_name]
            
            existing_collections = self.db.list_collection_names()
            monitored_collections = ClearVueConfig.get_all_collections()
            
            print(f"✅ MongoDB Atlas connected!")
            print(f"   Database: {self.db_name}")
            print(f"   Collections found: {len(existing_collections)}")
            
            missing = [c for c in monitored_collections if c not in existing_collections]
            if missing:
                logger.warning(f"Collections not found: {missing}")
                print(f"⚠️  Warning: Missing collections: {', '.join(missing)}")
            
            logger.info(f"Connected to MongoDB: {self.db_name}")
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            print(f"❌ MongoDB connection failed: {e}")
            raise
    
    def _setup_kafka_producer(self):
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
            raise
    
    def _json_serializer(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bson.ObjectId):
            return str(obj)
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def create_kafka_topics(self):
        print("\n📋 Creating/verifying Kafka topics...")
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers,
                client_id='clearvue_pipeline_admin'
            )
            
            existing_topics = admin_client.list_topics()
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
            
            if topics_to_create:
                admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
                print(f"✅ Created {len(topics_to_create)} new Kafka topics")
            else:
                print("✅ All Kafka topics already exist")
            
            admin_client.close()
            
        except Exception as e:
            logger.warning(f"Topic creation warning: {e}")
            print(f"⚠️ Topic warning: {e}")
    
    def enhance_change_event(self, change_doc: Dict[str, Any], collection_name: str) -> Dict[str, Any]:
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
            'metadata': {
                'pipeline_version': '2.0',
                'processed_at': datetime.now().isoformat()
            }
        }
        
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
        
        return enhanced_event
    
    def send_to_kafka(self, topic: str, key: str, message: Dict[str, Any]) -> bool:
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                future = self.kafka_producer.send(topic, key=key, value=message)
                record_metadata = future.get(timeout=10)
                logger.debug(f"✓ Kafka: {topic} | Partition: {record_metadata.partition}")
                return True
            except KafkaError as e:
                retry_count += 1
                logger.error(f"Kafka error (attempt {retry_count}/{max_retries}): {e}")
                self.stats['kafka_send_errors'] += 1
                if retry_count < max_retries:
                    time.sleep(1)
                else:
                    return False
            except Exception as e:
                logger.error(f"Unexpected error sending to Kafka: {e}")
                self.stats['errors'] += 1
                return False
        
        return False
    
    def notify_powerbi_of_change(self, enhanced_event: Dict[str, Any]) -> bool:
        """Notify Power BI refresh trigger of new data"""
        if not hasattr(self, 'powerbi_trigger') or not self.powerbi_trigger:
            return False
        
        collection = enhanced_event.get('collection')
        operation = enhanced_event.get('operation')
        doc_id = enhanced_event.get('document_key')
        
        self.powerbi_trigger.on_data_change(collection, operation, doc_id)
        return True
    
    def process_change_event(self, change_doc: Dict[str, Any], collection_name: str):
        try:
            enhanced_event = self.enhance_change_event(change_doc, collection_name)
            topic = ClearVueConfig.get_topic_for_collection(collection_name)
            message_key = enhanced_event['document_key']
            
            kafka_success = self.send_to_kafka(topic, message_key, enhanced_event)
            
            # ✅ Notify Power BI for ALL collections (not just Sales)
            self.notify_powerbi_of_change(enhanced_event)
            
            if kafka_success:
                self.stats['total_changes'] += 1
                self.stats['changes_by_collection'][collection_name] += 1
                
                operation = change_doc['operationType']
                self.stats['changes_by_operation'][operation] += 1
                
                if enhanced_event['priority'] == 'HIGH':
                    self.stats['high_priority_events'] += 1
                
                self.stats['last_processed'] = datetime.now().isoformat()
                
                if enhanced_event['priority'] == 'HIGH':
                    logger.info(f"⚡ HIGH: {operation.upper()} on {collection_name} → Kafka ✓ PowerBI notified")
                    print(f"⚡ {collection_name}: {operation.upper()} → PowerBI refresh queued")
                
        except Exception as e:
            logger.error(f"Error processing change event: {e}", exc_info=True)
            self.stats['errors'] += 1
    
    def watch_collection(self, collection_name: str):
        collection = self.db[collection_name]
        pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]
        
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
        except Exception as e:
            logger.error(f"Change stream error for {collection_name}: {e}", exc_info=True)
            self.stats['errors'] += 1
        finally:
            if collection_name in self.change_streams:
                del self.change_streams[collection_name]
    
    def print_stats(self, final: bool = False):
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
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"💚 Health: {'HEALTHY ✓' if self.is_healthy else 'UNHEALTHY ✗'}")
        
        # ✅ Add Power BI stats
        if hasattr(self, 'powerbi_trigger') and self.powerbi_trigger:
            pbi_stats = self.powerbi_trigger.get_stats()
            print(f"\n📊 Power BI Refresh Stats:")
            print(f"   Changes detected: {pbi_stats['total_changes_detected']:,}")
            print(f"   Refreshes triggered: {pbi_stats['refresh_requests_sent']}")
            print(f"   Successful: {pbi_stats['refresh_requests_succeeded']}")
            print(f"   Failed: {pbi_stats['refresh_requests_failed']}")
            print(f"   Last refresh: {pbi_stats['last_refresh_time'] or 'Never'}")
            print(f"   Status: {pbi_stats['last_refresh_status'] or 'N/A'}")
        
        if self.stats['changes_by_collection']:
            print("\n📁 Changes by collection:")
            for coll, count in sorted(self.stats['changes_by_collection'].items(), key=lambda x: x[1], reverse=True):
                print(f"   {coll:25} : {count:,}")
        
        print("="*70 + "\n")
    
    def start_pipeline(self, collections: Optional[List[str]] = None):
        if self.is_running:
            print("⚠️  Pipeline is already running!")
            return
        
        if collections is None:
            collections = ClearVueConfig.get_all_collections()
        
        print("\n" + "="*70)
        print("🚀 STARTING CLEARVUE STREAMING PIPELINE")
        print("="*70)
        print(f"📂 Database: {self.db_name}")
        print(f"📋 Collections: {', '.join(collections)}")
        
        # ✅ Show Power BI status
        if hasattr(self, 'powerbi_trigger') and self.powerbi_trigger:
            print(f"📊 Power BI: ENABLED (Smart Refresh Mode)")
        else:
            print(f"📊 Power BI: DISABLED")
        
        print("="*70 + "\n")
        
        self.create_kafka_topics()
        self.is_running = True
        self.stats['start_time'] = datetime.now().isoformat()
        
        def signal_handler(signum, frame):
            print("\n⚠️ Shutdown signal received")
            self.stop_pipeline()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
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
        
        print(f"\n✅ Started {len(self.stream_threads)} change stream watchers\n")
        print("🟢 PIPELINE IS RUNNING!")
        print("👀 Waiting for database changes...")
        print("🛑 Press Ctrl+C to stop\n")
        
        try:
            last_stats_time = datetime.now()
            
            while self.is_running and any(t.is_alive() for t in self.stream_threads):
                time.sleep(1)
                
                if (datetime.now() - last_stats_time).seconds >= ClearVueConfig.STATS_PRINT_INTERVAL:
                    self.print_stats()
                    last_stats_time = datetime.now()
        
        except KeyboardInterrupt:
            print("\n⚠️  Manual shutdown requested")
            self.stop_pipeline()
        
        print("\n⏳ Waiting for threads to finish...")
        for thread in self.stream_threads:
            thread.join(timeout=5)
        
        print("\n✅ Pipeline stopped successfully")
    
    def stop_pipeline(self):
        print("\n🛑 Stopping pipeline...")
        self.is_running = False
        
        for collection_name in list(self.change_streams.keys()):
            try:
                self.change_streams[collection_name].close()
            except Exception as e:
                logger.error(f"Error closing stream {collection_name}: {e}")
        
        try:
            self.kafka_producer.flush(timeout=10)
            self.kafka_producer.close()
            print("   ✓ Closed Kafka producer")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        
        try:
            self.mongo_client.close()
            print("   ✓ Closed MongoDB connection")
        except Exception as e:
            logger.error(f"Error closing MongoDB: {e}")
        
        self.print_stats(final=True)


def main():
    print_startup_banner()
    
    print("\n📋 PIPELINE MONITORING OPTIONS:")
    print("="*70)
    print("1. ALL collections (all 6 collections)")
    print("2. TRANSACTION data only (Sales, Payments, Purchases)")
    print("3. MASTER data only (Customers, Products, Suppliers)")
    print("4. HIGH PRIORITY only (Sales, Payments)")
    print("="*70)
    
    choice = input("\n👉 Select option (1-4) [default: 1]: ").strip() or "1"
    
    pipeline = None
    try:
        pipeline = ClearVueStreamingPipeline()
        
        if choice == "1":
            pipeline.start_pipeline()
        elif choice == "2":
            pipeline.start_pipeline(collections=['Sales_flat', 'Payments_flat', 'Purchases_flat'])
        elif choice == "3":
            pipeline.start_pipeline(collections=['Customer_flat_step2', 'Products_flat', 'Suppliers'])
        elif choice == "4":
            pipeline.start_pipeline(collections=['Sales_flat', 'Payments_flat'])
        else:
            print("❌ Invalid option")
    
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if pipeline:
            pipeline.stop_pipeline()
        print("\n👋 Goodbye!\n")


if __name__ == "__main__":
    main()