"""
ClearVue Kafka Consumer
Test consumer to verify the streaming pipeline is working
"""
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ClearVueConsumer')

class ClearVueKafkaConsumer:
    def __init__(self, topics=None, kafka_servers=['localhost:29092']):
        # Default topics to consume
        if topics is None:
            topics = [
                'clearvue.payments.realtime',
                'clearvue.customers.changes',
                'clearvue.payments.lines',
                'clearvue.products.updates'
            ]
        
        self.topics = topics
        self.consumer = None
        self.stats = {
            'messages_processed': 0,
            'messages_by_topic': {},
            'last_message_time': None
        }
        
        # Initialize Kafka Consumer
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id='clearvue_bi_dashboard_group',
                enable_auto_commit=True,
                auto_offset_reset='latest',  # Start from newest messages
                consumer_timeout_ms=10000  # Timeout after 10 seconds of no messages
            )
            logger.info(f"✅ Kafka Consumer initialized for topics: {topics}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka Consumer: {e}")
            raise
    
    def format_payment_message(self, message):
        """Format payment-related messages for display"""
        change_data = message.get('change_data', {})
        business_context = message.get('business_context', {})
        
        if message['collection'] == 'payment_headers':
            return (
                f"💳 Payment {change_data.get('payment_id', 'Unknown')}: "
                f"R{business_context.get('total_amount', 0):,.2f} "
                f"({business_context.get('region', 'Unknown')}) "
                f"[{change_data.get('status', 'Unknown')}]"
            )
        elif message['collection'] == 'payment_lines':
            return (
                f"📋 Payment Line: Payment {change_data.get('payment_id', 'Unknown')} "
                f"- {change_data.get('product_name', 'Unknown Product')} "
                f"(Qty: {change_data.get('quantity', 0)}, "
                f"Total: R{change_data.get('line_total', 0):,.2f})"
            )
        return "💳 Payment event"
    
    def format_customer_message(self, message):
        """Format customer-related messages for display"""
        change_data = message.get('change_data', {})
        business_context = message.get('business_context', {})
        
        return (
            f"👤 Customer {business_context.get('customer_id', 'Unknown')}: "
            f"{change_data.get('company_name', 'Unknown Company')} "
            f"({business_context.get('region', 'Unknown')})"
        )
    
    def format_product_message(self, message):
        """Format product-related messages for display"""
        change_data = message.get('change_data', {})
        
        return (
            f"📦 Product {change_data.get('product_id', 'Unknown')}: "
            f"{change_data.get('product_name', 'Unknown Product')} "
            f"(Brand: {change_data.get('brand', 'Unknown')})"
        )
    
    def format_message(self, topic, message):
        """Format message for display based on topic"""
        operation = message.get('operation', 'unknown')
        timestamp = message.get('timestamp', 'unknown')
        collection = message.get('collection', 'unknown')
        
        # Get operation emoji
        op_emoji = {
            'insert': '➕',
            'update': '✏️',
            'delete': '🗑️'
        }.get(operation, '❓')
        
        # Format based on collection type
        if 'payment' in topic:
            detail = self.format_payment_message(message)
        elif 'customer' in topic:
            detail = self.format_customer_message(message)
        elif 'product' in topic:
            detail = self.format_product_message(message)
        else:
            detail = f"📄 {collection} document"
        
        return f"{op_emoji} {detail} | {timestamp[:19]}"
    
    def process_message(self, topic, message):
        """Process a single Kafka message"""
        try:
            # Update stats
            self.stats['messages_processed'] += 1
            self.stats['messages_by_topic'][topic] = \
                self.stats['messages_by_topic'].get(topic, 0) + 1
            self.stats['last_message_time'] = datetime.now().isoformat()
            
            # Format and display message
            formatted_msg = self.format_message(topic, message)
            logger.info(f"📨 {formatted_msg}")
            
            # Show business insights for payments
            if topic == 'clearvue.payments.realtime' and message.get('operation') == 'insert':
                business_context = message.get('business_context', {})
                if business_context.get('total_amount', 0) > 50000:
                    logger.info(f"🔔 HIGH VALUE TRANSACTION ALERT: R{business_context['total_amount']:,.2f}")
            
        except Exception as e:
            logger.error(f"❌ Error processing message from {topic}: {e}")
    
    def start_consuming(self, show_stats_interval=30):
        """Start consuming messages"""
        if not self.consumer:
            logger.error("❌ Consumer not initialized")
            return
        
        logger.info("🚀 Starting ClearVue Kafka Consumer")
        logger.info("   Waiting for messages... (Press Ctrl+C to stop)")
        
        message_count = 0
        last_stats_time = datetime.now()
        
        try:
            for message in self.consumer:
                self.process_message(message.topic, message.value)
                message_count += 1
                
                # Show stats periodically
                now = datetime.now()
                if (now - last_stats_time).seconds >= show_stats_interval:
                    self.print_stats()
                    last_stats_time = now
                
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}")
        finally:
            self.close()
    
    def print_stats(self):
        """Print consumer statistics"""
        logger.info("📊 Consumer Statistics:")
        logger.info(f"   Total messages processed: {self.stats['messages_processed']}")
        logger.info(f"   Last message time: {self.stats['last_message_time']}")
        
        if self.stats['messages_by_topic']:
            logger.info("   Messages by topic:")
            for topic, count in self.stats['messages_by_topic'].items():
                topic_short = topic.replace('clearvue.', '')
                logger.info(f"     {topic_short}: {count}")
    
    def close(self):
        """Close the consumer"""
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Consumer closed")

if __name__ == "__main__":
    # Test different consumption modes
    print("ClearVue Kafka Consumer")
    print("=" * 40)
    print("1. Monitor all topics")
    print("2. Monitor payments only")
    print("3. Monitor customers only")
    
    choice = input("Select option (1-3, default=1): ").strip() or "1"
    
    if choice == "2":
        topics = ['clearvue.payments.realtime', 'clearvue.payments.lines']
    elif choice == "3":
        topics = ['clearvue.customers.changes']
    else:
        topics = None  # All topics
    
    try:
        consumer = ClearVueKafkaConsumer(topics=topics)
        consumer.start_consuming()
    except Exception as e:
        logger.error(f"❌ Consumer failed: {e}")