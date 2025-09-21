"""
ClearVue Streaming Pipeline
MongoDB Change Streams → Kafka Producer
Watches for real-time changes in MongoDB and streams them to Kafka
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any
import threading
import signal
import sys

from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import KafkaError
import bson

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ClearVueStreamingPipeline')

class ClearVueStreamingPipeline:
    def __init__(self, 
                 mongo_uri='mongodb://admin:clearvue123@localhost:27017/',
                 kafka_servers=['localhost:29092'],
                 database_name='clearvue_bi'):
        
        # MongoDB Connection
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[database_name]
        
        # Kafka Producer
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=1,  # Ensure ordering
            enable_idempotence=True  # Prevent duplicates
        )
        
        # Pipeline control
        self.is_running = False
        self.change_streams = {}
        self.stats = {
            'total_changes': 0,
            'changes_by_collection': {},
            'last_processed': None
        }
        
        # Kafka topic mapping for ClearVue collections
        self.topic_mapping = {
            'customers': 'clearvue.customers.changes',
            'payment_headers': 'clearvue.payments.realtime',
            'payment_lines': 'clearvue.payments.lines',
            'product_brands': 'clearvue.products.updates'
        }
        
        logger.info("ClearVue Streaming Pipeline initialized")
    
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
        from kafka.admin import KafkaAdminClient, NewTopic
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=['localhost:29092'],
                client_id='clearvue_topic_creator'
            )
            
            # Define topics with partitions and replication
            topics = [
                NewTopic(name=topic, num_partitions=3, replication_factor=1)
                for topic in self.topic_mapping.values()
            ]
            
            # Create topics
            admin_client.create_topics(new_topics=topics, validate_only=False)
            logger.info(f"Created Kafka topics: {list(self.topic_mapping.values())}")
            
        except Exception as e:
            logger.warning(f"Topic creation warning (topics may already exist): {e}")
    
    def enhance_change_event(self, change_doc: Dict[str, Any], collection_name: str) -> Dict[str, Any]:
        """Enhance change event with ClearVue business context"""
        enhanced_event = {
            'event_id': str(bson.ObjectId()),
            'timestamp': datetime.now().isoformat(),
            'source': 'clearvue_mongodb',
            'collection': collection_name,
            'operation': change_doc['operationType'],
            'document_key': change_doc.get('documentKey', {}),
            'change_data': {}
        }
        
        # Add the actual changed data
        if 'fullDocument' in change_doc:
            enhanced_event['change_data'] = change_doc['fullDocument']
        elif 'updateDescription' in change_doc:
            enhanced_event['change_data'] = change_doc['updateDescription']
        
        # Add ClearVue-specific context
        if collection_name == 'payment_headers' and 'fullDocument' in change_doc:
            doc = change_doc['fullDocument']
            enhanced_event['business_context'] = {
                'financial_month': doc.get('financial_month'),
                'financial_year': doc.get('financial_year'),
                'region': doc.get('region'),
                'customer_id': doc.get('customer_id'),
                'total_amount': doc.get('total_amount'),
                'currency': doc.get('currency', 'ZAR')
            }
        
        elif collection_name == 'customers' and 'fullDocument' in change_doc:
            doc = change_doc['fullDocument']
            enhanced_event['business_context'] = {
                'customer_id': doc.get('customer_id'),
                'region': doc.get('address', {}).get('region'),
                'status': doc.get('status')
            }
        
        return enhanced_event
    
    def send_to_kafka(self, topic: str, key: str, message: Dict[str, Any]):
        """Send message to Kafka topic"""
        try:
            future = self.kafka_producer.send(topic, key=key, value=message)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"✅ Sent to Kafka - Topic: {topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Kafka error: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error sending to Kafka: {e}")
            return False
    
    def process_change_event(self, change_doc: Dict[str, Any], collection_name: str):
        """Process a single change event"""
        try:
            # Enhance the change event
            enhanced_event = self.enhance_change_event(change_doc, collection_name)
            
            # Determine Kafka topic
            topic = self.topic_mapping.get(collection_name)
            if not topic:
                logger.warning(f"No topic mapping for collection: {collection_name}")
                return
            
            # Create message key for partitioning
            doc_key = change_doc.get('documentKey', {})
            message_key = doc_key.get('_id', enhanced_event['event_id'])
            
            # Send to Kafka
            success = self.send_to_kafka(topic, str(message_key), enhanced_event)
            
            if success:
                # Update statistics
                self.stats['total_changes'] += 1
                self.stats['changes_by_collection'][collection_name] = \
                    self.stats['changes_by_collection'].get(collection_name, 0) + 1
                self.stats['last_processed'] = datetime.now().isoformat()
                
                # Log summary
                operation = change_doc['operationType']
                logger.info(
                    f"📊 Processed {operation} on {collection_name} "
                    f"(Total: {self.stats['total_changes']})"
                )
        
        except Exception as e:
            logger.error(f"❌ Error processing change event: {e}")
    
    def watch_collection(self, collection_name: str):
        """Watch a specific collection for changes"""
        collection = self.db[collection_name]
        
        # Configure change stream options
        pipeline = [
            # Only watch for insert, update, delete operations
            {'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}
        ]
        
        options = {
            'full_document': 'updateLookup',  # Include full document on updates
            'max_await_time_ms': 1000
        }
        
        logger.info(f"🔍 Starting change stream for collection: {collection_name}")
        
        try:
            with collection.watch(pipeline, **options) as stream:
                self.change_streams[collection_name] = stream
                
                for change in stream:
                    if not self.is_running:
                        break
                    
                    self.process_change_event(change, collection_name)
                    
        except Exception as e:
            logger.error(f"❌ Error in change stream for {collection_name}: {e}")
        finally:
            if collection_name in self.change_streams:
                del self.change_streams[collection_name]
            logger.info(f"🔚 Change stream closed for collection: {collection_name}")
    
    def start_pipeline(self, collections=None):
        """Start the streaming pipeline"""
        if self.is_running:
            logger.warning("Pipeline is already running")
            return
        
        if collections is None:
            collections = list(self.topic_mapping.keys())
        
        logger.info("🚀 Starting ClearVue Streaming Pipeline")
        logger.info(f"📊 Monitoring collections: {collections}")
        
        # Create Kafka topics
        self.create_kafka_topics()
        
        self.is_running = True
        
        # Start change stream watchers in separate threads
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
        
        logger.info(f"✅ Started {len(threads)} change stream watchers")
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info("🛑 Shutdown signal received")
            self.stop_pipeline()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Keep main thread alive
        try:
            while self.is_running and any(t.is_alive() for t in threads):
                # Print stats every 30 seconds
                import time
                time.sleep(30)
                self.print_stats()
        except KeyboardInterrupt:
            logger.info("🛑 Manual shutdown requested")
            self.stop_pipeline()
        
        # Wait for threads to finish
        for thread in threads:
            thread.join(timeout=5)
        
        logger.info("✅ ClearVue Streaming Pipeline stopped")
    
    def stop_pipeline(self):
        """Stop the streaming pipeline"""
        logger.info("🔄 Stopping pipeline...")
        self.is_running = False
        
        # Close change streams
        for stream in self.change_streams.values():
            try:
                stream.close()
            except:
                pass
        
        # Close Kafka producer
        try:
            self.kafka_producer.close()
        except:
            pass
        
        # Close MongoDB connection
        try:
            self.mongo_client.close()
        except:
            pass
    
    def print_stats(self):
        """Print pipeline statistics"""
        logger.info("📈 Pipeline Statistics:")
        logger.info(f"   Total changes processed: {self.stats['total_changes']}")
        logger.info(f"   Last processed: {self.stats['last_processed']}")
        for collection, count in self.stats['changes_by_collection'].items():
            logger.info(f"   {collection}: {count} changes")

if __name__ == "__main__":
    pipeline = ClearVueStreamingPipeline()
    
    try:
        # Start monitoring all collections
        pipeline.start_pipeline()
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
    finally:
        pipeline.stop_pipeline()