"""
Kafka Consumer Test Script
For Power BI team to verify Kafka streaming data

This script consumes messages from ClearVue Kafka topics and displays them
in a readable format to understand the data structure before connecting Power BI.

Author: ClearVue Streaming Team
Date: October 2025
"""

import json
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys

class ClearVueKafkaConsumerTest:
    """Test consumer for verifying Kafka messages"""
    
    def __init__(self, bootstrap_servers=None):
        """Initialize Kafka consumer"""
        
        # Use config if not provided
        if bootstrap_servers is None:
            from config import ClearVueConfig
            bootstrap_servers = ClearVueConfig.get_kafka_servers()
        
        print("\n" + "="*70)
        print("ClearVue Kafka Consumer Test")
        print("="*70)
        print("\nAvailable Topics:")
        print("1. clearvue.sales.realtime (Sales transactions)")
        print("2. clearvue.payments.realtime (Payment transactions)")
        print("3. clearvue.purchases.realtime (Purchase orders)")
        print("4. clearvue.customers.changes (Customer updates)")
        print("5. clearvue.products.changes (Product updates)")
        print("6. clearvue.suppliers.changes (Supplier updates)")
        print("7. ALL topics (combined)")
        
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.message_count = 0
    
    def connect(self, topics):
        """Connect to Kafka topics"""
        try:
            print(f"\n🔌 Connecting to Kafka...")
            print(f"   Bootstrap servers: {self.bootstrap_servers}")
            print(f"   Topics: {', '.join(topics)}")
            
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',  # Only new messages
                enable_auto_commit=True,
                group_id='powerbi_test_consumer'
            )
            
            print("✅ Connected to Kafka successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def display_sales_message(self, message):
        """Display sales message in readable format"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("🛒 SALES TRANSACTION")
        print("="*70)
        print(f"Event ID: {data.get('event_id')}")
        print(f"Timestamp: {data.get('timestamp')}")
        print(f"Operation: {data.get('operation').upper()}")
        print(f"\nProduct Details:")
        print(f"  Inventory Code: {ctx.get('inventory_code')}")
        print(f"  Brand: {ctx.get('brand')}")
        print(f"  Category: {ctx.get('category')}")
        print(f"  Last Cost: R{ctx.get('last_cost', 0):,.2f}")
        print(f"  In Stock: {ctx.get('in_stock')}")
        print("="*70)
    
    def display_supplier_message(self, message):
        """Display supplier update message"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("🏭 SUPPLIER UPDATE")
        print("="*70)
        print(f"Event ID: {data.get('event_id')}")
        print(f"Timestamp: {data.get('timestamp')}")
        print(f"Operation: {data.get('operation').upper()}")
        print(f"\nSupplier Details:")
        print(f"  Supplier ID: {ctx.get('supplier_id')}")
        print(f"  Supplier Name: {ctx.get('supplier_name')}")
        print(f"  Exclusive: {ctx.get('exclusive')}")
        print(f"  Credit Limit: R{ctx.get('credit_limit', 0):,.2f}")
        print(f"  Payment Terms: {ctx.get('payment_terms')} days")
        print("="*70)
    
    def display_generic_message(self, message):
        """Display generic message"""
        data = message.value
        
        print("\n" + "="*70)
        print(f"📨 {data.get('collection', 'UNKNOWN').upper()} - {data.get('operation', 'unknown').upper()}")
        print("="*70)
        print(f"Event ID: {data.get('event_id')}")
        print(f"Timestamp: {data.get('timestamp')}")
        print(f"Collection: {data.get('collection')}")
        print(f"Operation: {data.get('operation')}")
        print(f"Priority: {data.get('priority')}")
        print("\nBusiness Context:")
        print(json.dumps(data.get('business_context', {}), indent=2))
        print("="*70)
    
    def start_consuming(self):
        """Start consuming messages"""
        if not self.consumer:
            print("❌ Not connected to Kafka")
            return
        
        print("\n" + "="*70)
        print("🟢 CONSUMER STARTED - Waiting for messages...")
        print("Press Ctrl+C to stop")
        print("="*70)
        
        try:
            for message in self.consumer:
                self.message_count += 1
                
                # Route to appropriate display function based on topic
                topic = message.topic
                
                if 'sales' in topic:
                    self.display_sales_message(message)
                elif 'payments' in topic:
                    self.display_payment_message(message)
                elif 'purchases' in topic:
                    self.display_purchase_message(message)
                elif 'customers' in topic:
                    self.display_customer_message(message)
                elif 'products' in topic:
                    self.display_product_message(message)
                elif 'suppliers' in topic:
                    self.display_supplier_message(message)
                else:
                    self.display_generic_message(message)
                
                print(f"\n📊 Messages consumed: {self.message_count}")
                
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping consumer...")
        except Exception as e:
            print(f"\n❌ Error consuming messages: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                print("✅ Consumer closed")
            print(f"\n📊 Final count: {self.message_count} messages consumed")
    
    def export_sample_schema(self):
        """Export sample message schema for Power BI team"""
        schema = {
            "sales_message_example": {
                "event_id": "507f1f77bcf86cd799439011",
                "timestamp": "2025-10-07T14:30:00.123456",
                "source": "mongodb_atlas",
                "database": "Nova_Analytics",
                "collection": "sales",
                "operation": "insert",
                "priority": "HIGH",
                "business_context": {
                    "transaction_type": "sales",
                    "doc_number": "SALE123456",
                    "customer_id": "CUST001",
                    "rep_id": "REP01",
                    "trans_type": "Tax Invoice",
                    "fin_period": 202510,
                    "financial_year": 2025,
                    "financial_quarter": "Q4",
                    "total_amount": 15000.00,
                    "line_count": 3,
                    "metric_category": "revenue"
                }
            },
            "payment_message_example": {
                "event_id": "507f1f77bcf86cd799439012",
                "timestamp": "2025-10-07T14:35:00.123456",
                "source": "mongodb_atlas",
                "database": "Nova_Analytics",
                "collection": "payments",
                "operation": "insert",
                "priority": "HIGH",
                "business_context": {
                    "transaction_type": "payment",
                    "customer_id": "CUST001",
                    "deposit_ref": "PAY789456",
                    "fin_period": 202510,
                    "financial_year": 2025,
                    "financial_quarter": "Q4",
                    "total_payment": 14250.00,
                    "total_discount": 750.00,
                    "payment_count": 1,
                    "metric_category": "cash_flow"
                }
            },
            "power_bi_fields": {
                "required_fields": [
                    "timestamp",
                    "collection",
                    "operation",
                    "business_context.fin_period",
                    "business_context.financial_year",
                    "business_context.financial_quarter"
                ],
                "numeric_fields": [
                    "business_context.total_amount",
                    "business_context.total_payment",
                    "business_context.total_cost",
                    "business_context.credit_limit"
                ],
                "dimension_fields": [
                    "business_context.customer_id",
                    "business_context.supplier_id",
                    "business_context.region",
                    "business_context.category"
                ]
            }
        }
        
        filename = "kafka_message_schema.json"
        with open(filename, 'w') as f:
            json.dump(schema, f, indent=2)
        
        print(f"\n✅ Schema exported to: {filename}")
        print("   Share this file with the Power BI team")


def main():
    """Main entry point"""
    test_consumer = ClearVueKafkaConsumerTest()
    
    choice = input("\nSelect topic (1-7) [default: 7]: ").strip() or "7"
    
    # Topic mapping
    topic_map = {
        '1': ['clearvue.sales.realtime'],
        '2': ['clearvue.payments.realtime'],
        '3': ['clearvue.purchases.realtime'],
        '4': ['clearvue.customers.changes'],
        '5': ['clearvue.products.changes'],
        '6': ['clearvue.suppliers.changes'],
        '7': [
            'clearvue.sales.realtime',
            'clearvue.payments.realtime',
            'clearvue.purchases.realtime',
            'clearvue.customers.changes',
            'clearvue.products.changes',
            'clearvue.suppliers.changes'
        ]
    }
    
    topics = topic_map.get(choice, topic_map['7'])
    
    # Ask if user wants to export schema
    export = input("\nExport message schema for Power BI? (y/n) [n]: ").strip().lower()
    if export == 'y':
        test_consumer.export_sample_schema()
    
    # Connect and start consuming
    if test_consumer.connect(topics):
        test_consumer.start_consuming()
    else:
        print("\n❌ Failed to connect to Kafka")
        print("Make sure:")
        print("  1. Docker services are running (docker-compose up -d)")
        print("  2. Kafka is healthy (check localhost:8080)")
        print("  3. Streaming pipeline is running")
        sys.exit(1)


if __name__ == "__main__":
    main()

    print(f"\nTransaction Details:")
    print(f"  Document Number: {ctx.get('doc_number')}")
    print(f"  Customer ID: {ctx.get('customer_id')}")
    print(f"  Rep ID: {ctx.get('rep_id')}")
    print(f"  Total Amount: R{ctx.get('total_amount', 0):,.2f}")
    print(f"  Line Items: {ctx.get('line_count', 0)}")
    print(f"\nFinancial Info:")
    print(f"  Period: {ctx.get('fin_period')}")
    print(f"  Year: {ctx.get('financial_year')}")
    print(f"  Quarter: {ctx.get('financial_quarter')}")
    print(f"\nPriority: {data.get('priority')}")
    print("="*70)
    
    def display_payment_message(self, message):
        """Display payment message in readable format"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("💳 PAYMENT TRANSACTION")
        print("="*70)
        print(f"Event ID: {data.get('event_id')}")
        print(f"Timestamp: {data.get('timestamp')}")
        print(f"Operation: {data.get('operation').upper()}")
        print(f"\nPayment Details:")
        print(f"  Customer ID: {ctx.get('customer_id')}")
        print(f"  Deposit Ref: {ctx.get('deposit_ref')}")
        print(f"  Total Payment: R{ctx.get('total_payment', 0):,.2f}")
        print(f"  Discount: R{ctx.get('total_discount', 0):,.2f}")
        print(f"\nFinancial Info:")
        print(f"  Period: {ctx.get('fin_period')}")
        print(f"  Year: {ctx.get('financial_year')}")
        print(f"  Quarter: {ctx.get('financial_quarter')}")
        print(f"\nPriority: {data.get('priority')}")
        print("="*70)
    
    def display_purchase_message(self, message):
        """Display purchase message in readable format"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("📦 PURCHASE ORDER")
        print("="*70)
        print(f"Event ID: {data.get('event_id')}")
        print(f"Timestamp: {data.get('timestamp')}")
        print(f"Operation: {data.get('operation').upper()}")
        print(f"\nPurchase Details:")
        print(f"  PO Number: {ctx.get('purch_doc_no')}")
        print(f"  Supplier ID: {ctx.get('supplier_id')}")
        print(f"  Total Cost: R{ctx.get('total_cost', 0):,.2f}")
        print(f"  Line Items: {ctx.get('line_count', 0)}")
        print(f"\nFinancial Info:")
        print(f"  Period: {ctx.get('fin_period')}")
        print(f"  Year: {ctx.get('financial_year')}")
        print(f"  Quarter: {ctx.get('financial_quarter')}")
        print(f"\nPriority: {data.get('priority')}")
        print("="*70)
    
    def display_customer_message(self, message):
        """Display customer update message"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("👤 CUSTOMER UPDATE")
        print("="*70)
        print(f"Event ID: {data.get('event_id')}")
        print(f"Timestamp: {data.get('timestamp')}")
        print(f"Operation: {data.get('operation').upper()}")
        print(f"\nCustomer Details:")
        print(f"  Customer ID: {ctx.get('customer_id')}")
        print(f"  Customer Name: {ctx.get('customer_name')}")
        print(f"  Region: {ctx.get('region')}")
        print(f"  Category: {ctx.get('category')}")
        print(f"  Credit Limit: R{ctx.get('credit_limit', 0):,.2f}")
        print(f"  Total Due: R{ctx.get('total_due', 0):,.2f}")
        print("="*70)
    
    def display_product_message(self, message):
        """Display product update message"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("📦 PRODUCT UPDATE")
        print("="*70)
        print(f"Event ID: {data.get('event_id')}")
        print(f"Timestamp: {data.get('timestamp')}")
        print(f"Operation: {data.get('operation').upper()}")