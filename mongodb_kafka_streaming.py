"""
ClearVue Streaming Pipeline - Student Version
MongoDB Change Streams to Kafka Producer
Watches for real-time changes in MongoDB and streams them to Kafka

Author: [Your Name]
Date: October 2024
Status: Works on my machine... hopefully works on yours too lol
"""

# Import everything we need (probably could organize this better but whatever)
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

# Set up logging - took me forever to figure out why my prints weren't showing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ClearVueStreamingPipeline')

class ClearVueStreamingPipeline:
    def __init__(self, 
                 mongo_uri='mongodb://localhost:27017/',
                 kafka_servers=['localhost:29092'],  # This port gave me nightmares
                 database_name='clearvue_bi'):
        
        # Connect to MongoDB - fingers crossed it's running
        print("Connecting to MongoDB...")
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[database_name]
        
        # Test the connection because I've been burned before
        try:
            # Simple connection test - just try to get server info
            self.mongo_client.admin.command('ismaster')
            print("MongoDB connection successful! Finally working haha")
        except Exception as e:
            print(f"MongoDB connection failed: {e}")
            print("Make sure Docker is running and you ran docker-compose up")
            raise
        
        # Kafka Producer setup - this took way too many Stack Overflow searches
        print("Setting up Kafka producer...")
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas - learned this the hard way
                retries=3,   # Because things fail sometimes
                max_in_flight_requests_per_connection=1,  # Keeps things in order
                enable_idempotence=True  # Prevents duplicates (I think?)
            )
            print("Kafka producer ready to go!")
        except Exception as e:
            print(f"Kafka setup failed: {e}")
            print("Is Kafka running? Try: docker ps")
            raise
        
        # Control variables - global state is probably bad practice but it works
        self.is_running = False
        self.change_streams = {}  # Keep track of our streams
        
        # Statistics tracking because I like to see numbers go up
        self.stats = {
            'total_changes': 0,
            'changes_by_collection': {},
            'last_processed': None,
            'start_time': None  # Added this to track uptime
        }
        
        # Topic mapping - maps our MongoDB collections to Kafka topics
        # Professor said we need "meaningful topic names" so here they are
        self.topic_mapping = {
            'customers': 'clearvue.customers.changes',
            'payment_headers': 'clearvue.payments.realtime',
            'payment_lines': 'clearvue.payments.lines',
            'product_brands': 'clearvue.products.updates'
        }
        
        logger.info("ClearVue Streaming Pipeline initialized")
        print("Pipeline object created successfully")
    
    def json_serializer(self, obj):
        """
        Custom JSON serializer for MongoDB objects
        Had to write this because MongoDB ObjectIds aren't JSON serializable by default
        Python is weird sometimes
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bson.ObjectId):
            return str(obj)  # Just convert to string, works fine
        elif hasattr(obj, 'isoformat'):  # Catch any other date-like objects
            return obj.isoformat()
        
        # If we get here, we don't know how to serialize it
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def create_kafka_topics(self):
        """
        Create Kafka topics if they don't exist
        This is probably optional since auto-create is enabled but better safe than sorry
        """
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            # Create admin client - separate from producer because reasons
            admin_client = KafkaAdminClient(
                bootstrap_servers=['localhost:29092'],
                client_id='clearvue_topic_creator'
            )
            
            # Define our topics with some reasonable defaults
            # 3 partitions because it's a good number (not too scientific)
            topics_to_create = []
            for topic_name in self.topic_mapping.values():
                new_topic = NewTopic(
                    name=topic_name, 
                    num_partitions=3, 
                    replication_factor=1  # Only 1 because we're not running a cluster
                )
                topics_to_create.append(new_topic)
            
            # Try to create the topics
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            logger.info(f"Created Kafka topics: {list(self.topic_mapping.values())}")
            print("Kafka topics created (or already existed)")
            
        except Exception as e:
            # Topics probably already exist, which is fine
            logger.warning(f"Topic creation warning: {e}")
            print("Topic creation had issues but that's probably okay")
    
    def get_clearvue_financial_month(self, date_obj=None):
        """
        Calculate ClearVue's weird financial month system
        Financial month runs from last Saturday of previous month to last Friday of current month
        This was mentioned in the RFP and took me ages to understand
        
        Note: This is a simplified version because the actual logic is complex
        In real life you'd probably use a calendar library but this works for demo
        """
        if date_obj is None:
            date_obj = datetime.now()
        
        # Simplified calculation - in reality this would be more complex
        # But this gives us the right structure for the demo
        financial_month = date_obj.month
        financial_year = date_obj.year
        
        # Adjust for ClearVue's year if we're in Jan/Feb
        if date_obj.month <= 2:
            financial_year = date_obj.year - 1
        
        # Calculate quarter (because business people love quarters)
        quarter = ((financial_month - 1) // 3) + 1
        
        return {
            'financial_month': financial_month,
            'financial_year': financial_year,
            'financial_quarter': quarter,
            'calendar_month': date_obj.month  # Keep original for reference
        }
    
    def enhance_change_event(self, change_doc: Dict[str, Any], collection_name: str) -> Dict[str, Any]:
        """
        Add business context to change events
        This is where we make the raw MongoDB change events useful for business intelligence
        The RFP wanted "business context" so here it is
        """
        # Create the base event structure
        enhanced_event = {
            'event_id': str(bson.ObjectId()),  # Unique ID for this event
            'timestamp': datetime.now().isoformat(),
            'source': 'clearvue_mongodb',
            'collection': collection_name,
            'operation': change_doc['operationType'],
            'document_key': change_doc.get('documentKey', {}),
            'change_data': {},
            'business_context': {}  # This is where the magic happens
        }
        
        # Extract the actual changed data
        if 'fullDocument' in change_doc:
            # For inserts and some updates, we get the full document
            enhanced_event['change_data'] = change_doc['fullDocument']
        elif 'updateDescription' in change_doc:
            # For updates, we get the update description
            enhanced_event['change_data'] = change_doc['updateDescription']
        else:
            # For deletes, we might not get much
            enhanced_event['change_data'] = {}
        
        # Add collection-specific business context
        # This part is really important for the BI requirements
        if collection_name == 'payment_headers' and 'fullDocument' in change_doc:
            doc = change_doc['fullDocument']
            
            # Get financial month info using ClearVue's system
            payment_date = doc.get('payment_date', datetime.now())
            financial_info = self.get_clearvue_financial_month(payment_date)
            
            enhanced_event['business_context'] = {
                'financial_month': financial_info['financial_month'],
                'financial_year': financial_info['financial_year'],
                'financial_quarter': financial_info['financial_quarter'],
                'region': doc.get('region'),
                'customer_id': doc.get('customer_id'),
                'total_amount': doc.get('total_amount'),
                'currency': doc.get('currency', 'ZAR'),  # Default to South African Rand
                'payment_method': doc.get('payment_method'),
                'transaction_type': 'payment_header'  # For filtering later
            }
            
        elif collection_name == 'payment_lines' and 'fullDocument' in change_doc:
            doc = change_doc['fullDocument']
            enhanced_event['business_context'] = {
                'payment_id': doc.get('payment_id'),
                'product_id': doc.get('product_id'),
                'line_total': doc.get('line_total'),
                'quantity': doc.get('quantity'),
                'transaction_type': 'payment_line'
            }
            
        elif collection_name == 'customers' and 'fullDocument' in change_doc:
            doc = change_doc['fullDocument']
            enhanced_event['business_context'] = {
                'customer_id': doc.get('customer_id'),
                'region': doc.get('address', {}).get('region') if doc.get('address') else None,
                'status': doc.get('status'),
                'transaction_type': 'customer_change'
            }
            
        elif collection_name == 'product_brands' and 'fullDocument' in change_doc:
            doc = change_doc['fullDocument']
            enhanced_event['business_context'] = {
                'product_id': doc.get('product_id'),
                'brand': doc.get('brand'),
                'category': doc.get('category'),
                'transaction_type': 'product_change'
            }
        
        return enhanced_event
    
    def send_to_kafka(self, topic: str, key: str, message: Dict[str, Any]):
        """
        Send a message to Kafka
        Returns True if successful, False otherwise
        Added extra error handling because Kafka can be finicky
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Send the message
                future = self.kafka_producer.send(topic, key=key, value=message)
                
                # Wait for it to be sent (with timeout)
                record_metadata = future.get(timeout=10)
                
                # Log success
                logger.info(
                    f"Message sent to Kafka - Topic: {topic}, "
                    f"Partition: {record_metadata.partition}, "
                    f"Offset: {record_metadata.offset}"
                )
                return True
                
            except KafkaError as e:
                retry_count += 1
                logger.error(f"Kafka error (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    time.sleep(1)  # Wait a bit before retrying
                
            except Exception as e:
                logger.error(f"Unexpected error sending to Kafka: {e}")
                return False
        
        # If we get here, all retries failed
        logger.error(f"Failed to send message to Kafka after {max_retries} attempts")
        return False
    
    def process_change_event(self, change_doc: Dict[str, Any], collection_name: str):
        """
        Process a single change event from MongoDB
        This is where the rubber meets the road
        """
        try:
            # First, enhance the change event with business context
            enhanced_event = self.enhance_change_event(change_doc, collection_name)
            
            # Figure out which Kafka topic to send to
            topic = self.topic_mapping.get(collection_name)
            if not topic:
                logger.warning(f"No topic mapping found for collection: {collection_name}")
                print(f"Warning: Unknown collection {collection_name}")
                return
            
            # Create a message key for partitioning
            # Using document ID so related changes go to same partition
            doc_key = change_doc.get('documentKey', {})
            message_key = doc_key.get('_id', enhanced_event['event_id'])
            
            # Send to Kafka
            success = self.send_to_kafka(topic, str(message_key), enhanced_event)
            
            if success:
                # Update our statistics
                self.stats['total_changes'] += 1
                if collection_name not in self.stats['changes_by_collection']:
                    self.stats['changes_by_collection'][collection_name] = 0
                self.stats['changes_by_collection'][collection_name] += 1
                self.stats['last_processed'] = datetime.now().isoformat()
                
                # Log what we processed
                operation = change_doc['operationType']
                logger.info(
                    f"Processed {operation} on {collection_name} "
                    f"(Total processed: {self.stats['total_changes']})"
                )
            else:
                print(f"Failed to process change event for {collection_name}")
        
        except Exception as e:
            logger.error(f"Error processing change event: {e}")
            print(f"Something went wrong processing a change event: {e}")
    
    def watch_collection(self, collection_name: str):
        """
        Watch a specific MongoDB collection for changes
        This runs in its own thread for each collection
        """
        collection = self.db[collection_name]
        
        # Set up the change stream pipeline
        # We only care about insert, update, and delete operations
        pipeline = [
            {
                '$match': {
                    'operationType': {'$in': ['insert', 'update', 'delete']}
                }
            }
        ]
        
        # Change stream options
        options = {
            'full_document': 'updateLookup',  # Include full document for updates
            'max_await_time_ms': 1000  # Don't wait too long for changes
        }
        
        logger.info(f"Starting change stream watcher for: {collection_name}")
        print(f"Watching {collection_name} for changes...")
        
        try:
            # Open the change stream
            with collection.watch(pipeline, **options) as stream:
                # Store the stream so we can close it later
                self.change_streams[collection_name] = stream
                
                # Process changes as they come in
                for change in stream:
                    # Check if we should still be running
                    if not self.is_running:
                        print(f"Stopping watcher for {collection_name}")
                        break
                    
                    # Process this change
                    self.process_change_event(change, collection_name)
                    
        except Exception as e:
            logger.error(f"Error in change stream for {collection_name}: {e}")
            print(f"Change stream error for {collection_name}: {e}")
        finally:
            # Clean up
            if collection_name in self.change_streams:
                del self.change_streams[collection_name]
            logger.info(f"Change stream closed for: {collection_name}")
    
    def start_pipeline(self, collections=None):
        """
        Start the streaming pipeline
        This is the main method that gets everything running
        """
        if self.is_running:
            logger.warning("Pipeline is already running!")
            print("Pipeline is already running!")
            return
        
        # Use default collections if none specified
        if collections is None:
            collections = list(self.topic_mapping.keys())
        
        logger.info("Starting ClearVue Streaming Pipeline")
        print("Starting ClearVue Streaming Pipeline...")
        print(f"Monitoring collections: {collections}")
        
        # Create Kafka topics first
        print("Setting up Kafka topics...")
        self.create_kafka_topics()
        
        # Mark as running and record start time
        self.is_running = True
        self.stats['start_time'] = datetime.now().isoformat()
        
        # Start a change stream watcher for each collection
        # Each one runs in its own thread
        threads = []
        for collection_name in collections:
            if collection_name in self.topic_mapping:
                print(f"Starting thread for {collection_name}...")
                thread = threading.Thread(
                    target=self.watch_collection, 
                    args=(collection_name,),
                    name=f"ChangeStream-{collection_name}"
                )
                thread.daemon = True  # Thread dies when main program dies
                thread.start()
                threads.append(thread)
            else:
                print(f"Warning: No topic mapping for {collection_name}")
        
        logger.info(f"Started {len(threads)} change stream watchers")
        print(f"Started {len(threads)} change stream watchers")
        
        # Set up signal handlers for clean shutdown
        def signal_handler(signum, frame):
            print("Shutdown signal received")
            logger.info("Shutdown signal received")
            self.stop_pipeline()
        
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Kill command
        
        # Keep the main thread alive and show stats periodically
        print("Pipeline is running! Press Ctrl+C to stop")
        print("Waiting for database changes...")
        
        try:
            stats_interval = 30  # Show stats every 30 seconds
            last_stats_time = datetime.now()
            
            while self.is_running and any(t.is_alive() for t in threads):
                time.sleep(1)  # Short sleep to avoid busy waiting
                
                # Show stats periodically
                now = datetime.now()
                if (now - last_stats_time).seconds >= stats_interval:
                    self.print_stats()
                    last_stats_time = now
                    
        except KeyboardInterrupt:
            print("Manual shutdown requested")
            logger.info("Manual shutdown requested")
            self.stop_pipeline()
        
        # Wait for all threads to finish
        print("Waiting for threads to finish...")
        for thread in threads:
            thread.join(timeout=5)  # Wait max 5 seconds for each thread
        
        logger.info("ClearVue Streaming Pipeline stopped")
        print("Pipeline stopped successfully")
    
    def stop_pipeline(self):
        """
        Stop the streaming pipeline cleanly
        """
        print("Stopping pipeline...")
        logger.info("Stopping pipeline...")
        self.is_running = False
        
        # Close all change streams
        print("Closing change streams...")
        for collection_name, stream in self.change_streams.items():
            try:
                stream.close()
                print(f"Closed stream for {collection_name}")
            except:
                print(f"Error closing stream for {collection_name}")
        
        # Close Kafka producer
        print("Closing Kafka producer...")
        try:
            self.kafka_producer.close()
            print("Kafka producer closed")
        except:
            print("Error closing Kafka producer")
        
        # Close MongoDB connection
        print("Closing MongoDB connection...")
        try:
            self.mongo_client.close()
            print("MongoDB connection closed")
        except:
            print("Error closing MongoDB connection")
    
    def print_stats(self):
        """
        Print current pipeline statistics
        Useful for monitoring what's happening
        """
        uptime = "Unknown"
        if self.stats['start_time']:
            start = datetime.fromisoformat(self.stats['start_time'])
            uptime = str(datetime.now() - start).split('.')[0]  # Remove microseconds
        
        print("\n" + "="*50)
        print("PIPELINE STATISTICS")
        print("="*50)
        print(f"Uptime: {uptime}")
        print(f"Total changes processed: {self.stats['total_changes']}")
        print(f"Last processed: {self.stats['last_processed']}")
        
        if self.stats['changes_by_collection']:
            print("Changes by collection:")
            for collection, count in self.stats['changes_by_collection'].items():
                print(f"  {collection}: {count}")
        else:
            print("No changes processed yet")
        
        print("="*50 + "\n")

# Main execution block
if __name__ == "__main__":
    print("ClearVue Streaming Pipeline - Student Implementation")
    print("=" * 60)
    
    # Create the pipeline object
    pipeline = None
    
    try:
        print("Initializing pipeline...")
        pipeline = ClearVueStreamingPipeline()
        print("Pipeline initialized successfully!")
        
        # Start the pipeline with all collections
        print("Starting pipeline with all collections...")
        pipeline.start_pipeline()
        
    except KeyboardInterrupt:
        print("Program interrupted by user")
    except Exception as e:
        print(f"Pipeline failed with error: {e}")
        logger.error(f"Pipeline failed: {e}")
    finally:
        # Always try to clean up
        if pipeline:
            print("Cleaning up...")
            pipeline.stop_pipeline()
        print("Program finished")