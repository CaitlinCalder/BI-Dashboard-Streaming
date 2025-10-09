"""
ClearVue Kafka Consumer Test Script
For Power BI team to verify Kafka streaming data

This script consumes messages from ClearVue Kafka topics and displays them
in a readable format to understand the data structure before connecting Power BI.

Author: ClearVue Streaming Team  
Date: October 2025
Version: 2.0
"""

import json
import sys
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# Import configuration
try:
    from config import ClearVueConfig
except ImportError:
    print("❌ Error: config.py not found")
    sys.exit(1)


class ClearVueKafkaConsumerTest:
    """Test consumer for verifying Kafka messages before Power BI integration"""
    
    def __init__(self):
        """Initialize Kafka consumer"""
        
        print("\n" + "="*70)
        print("🔍 CLEARVUE KAFKA CONSUMER TEST")
        print("   Power BI Integration Verification")
        print("="*70)
        
        self.bootstrap_servers = ClearVueConfig.get_kafka_servers()
        self.consumer = None
        self.message_count = 0
        self.messages_by_collection = {}
        
        print("\n📡 Available Topics:")
        print("="*70)
        for i, (coll, config) in enumerate(ClearVueConfig.KAFKA_TOPICS.items(), 1):
            priority_icon = "⚡" if config['priority'] == 'HIGH' else "📊"
            print(f"{i}. {priority_icon} {config['topic']}")
            print(f"   Collection: {coll} | Priority: {config['priority']} | "
                  f"Partitions: {config['partitions']}")
        print(f"\n{len(ClearVueConfig.KAFKA_TOPICS) + 1}. 🌐 ALL topics (combined)")
        print("="*70)
    
    def connect(self, topics: list) -> bool:
        """
        Connect to Kafka topics
        
        Args:
            topics: List of topic names to subscribe to
        
        Returns:
            bool: True if connected successfully
        """
        try:
            print(f"\n🔌 Connecting to Kafka...")
            print(f"   Bootstrap servers: {', '.join(self.bootstrap_servers)}")
            print(f"   Topics: {len(topics)}")
            
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',  # Only new messages (change to 'earliest' for historical)
                enable_auto_commit=True,
                group_id='powerbi_test_consumer_' + datetime.now().strftime('%Y%m%d_%H%M%S'),
                consumer_timeout_ms=1000  # Timeout for iteration
            )
            
            print("✅ Connected to Kafka successfully!")
            print(f"   Consumer group: {self.consumer.config['group_id']}")
            print(f"   Subscribed topics: {', '.join(topics)}")
            
            return True
        
        except NoBrokersAvailable:
            print("❌ Connection failed: No Kafka brokers available")
            print("   Make sure Kafka is running: docker-compose up -d")
            return False
        
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def display_sales_message(self, message):
        """Display sales transaction message"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("🛒 SALES TRANSACTION")
        print("="*70)
        print(f"Event ID:       {data.get('event_id')}")
        print(f"Timestamp:      {data.get('timestamp')}")
        print(f"Operation:      {data.get('operation').upper()}")
        print(f"Priority:       {data.get('priority')}")
        
        print(f"\n📋 Transaction Details:")
        print(f"  Document:     {ctx.get('doc_number')}")
        print(f"  Customer ID:  {ctx.get('customer_id')}")
        print(f"  Rep ID:       {ctx.get('rep_id')}")
        print(f"  Rep Name:     {ctx.get('rep_name')}")
        print(f"  Trans Type:   {ctx.get('trans_type')}")
        
        print(f"\n💰 Financial:")
        print(f"  Total Amount:   R{ctx.get('total_amount', 0):,.2f}")
        print(f"  Total Cost:     R{ctx.get('total_cost', 0):,.2f}")
        print(f"  Total Profit:   R{ctx.get('total_profit', 0):,.2f}")
        print(f"  Profit Margin:  {ctx.get('profit_margin_pct', 0):.2f}%")
        print(f"  Line Items:     {ctx.get('line_count', 0)}")
        
        print(f"\n📅 Financial Period:")
        print(f"  Period:   {ctx.get('fin_period')}")
        print(f"  Year:     {ctx.get('financial_year')}")
        print(f"  Quarter:  {ctx.get('financial_quarter')}")
        print(f"  Month:    {ctx.get('financial_month')}")
        
        print(f"\n🎯 BI Metrics:")
        print(f"  Category:  {ctx.get('metric_category')}")
        print(f"  KPI Type:  {ctx.get('kpi_type')}")
        print("="*70)
    
    def display_payment_message(self, message):
        """Display payment transaction message"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("💳 PAYMENT TRANSACTION")
        print("="*70)
        print(f"Event ID:       {data.get('event_id')}")
        print(f"Timestamp:      {data.get('timestamp')}")
        print(f"Operation:      {data.get('operation').upper()}")
        print(f"Priority:       {data.get('priority')}")
        
        print(f"\n📋 Payment Details:")
        print(f"  Customer ID:   {ctx.get('customer_id')}")
        print(f"  Deposit Ref:   {ctx.get('deposit_ref')}")
        print(f"  Deposit Date:  {ctx.get('deposit_date')}")
        
        print(f"\n💰 Financial:")
        print(f"  Bank Amount:    R{ctx.get('total_bank_amount', 0):,.2f}")
        print(f"  Discount:       R{ctx.get('total_discount', 0):,.2f} ({ctx.get('discount_pct', 0):.2f}%)")
        print(f"  Net Payment:    R{ctx.get('total_payment', 0):,.2f}")
        print(f"  Payment Lines:  {ctx.get('payment_count', 0)}")
        
        print(f"\n📅 Financial Period:")
        print(f"  Period:   {ctx.get('fin_period')}")
        print(f"  Year:     {ctx.get('financial_year')}")
        print(f"  Quarter:  {ctx.get('financial_quarter')}")
        
        print(f"\n🎯 BI Metrics:")
        print(f"  Category:  {ctx.get('metric_category')}")
        print(f"  KPI Type:  {ctx.get('kpi_type')}")
        print("="*70)
    
    def display_purchase_message(self, message):
        """Display purchase order message"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("📦 PURCHASE ORDER")
        print("="*70)
        print(f"Event ID:       {data.get('event_id')}")
        print(f"Timestamp:      {data.get('timestamp')}")
        print(f"Operation:      {data.get('operation').upper()}")
        print(f"Priority:       {data.get('priority')}")
        
        print(f"\n📋 Purchase Details:")
        print(f"  PO Number:     {ctx.get('purch_doc_no')}")
        print(f"  Supplier ID:   {ctx.get('supplier_id')}")
        print(f"  Purchase Date: {ctx.get('purch_date')}")
        
        print(f"\n💰 Financial:")
        print(f"  Total Cost:  R{ctx.get('total_cost', 0):,.2f}")
        print(f"  Line Items:  {ctx.get('line_count', 0)}")
        
        print(f"\n📅 Financial Period:")
        print(f"  Period:   {ctx.get('fin_period')}")
        print(f"  Year:     {ctx.get('financial_year')}")
        print(f"  Quarter:  {ctx.get('financial_quarter')}")
        
        print(f"\n🎯 BI Metrics:")
        print(f"  Category:  {ctx.get('metric_category')}")
        print(f"  KPI Type:  {ctx.get('kpi_type')}")
        print("="*70)
    
    def display_customer_message(self, message):
        """Display customer master data update"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("👤 CUSTOMER UPDATE")
        print("="*70)
        print(f"Event ID:       {data.get('event_id')}")
        print(f"Timestamp:      {data.get('timestamp')}")
        print(f"Operation:      {data.get('operation').upper()}")
        print(f"Priority:       {data.get('priority')}")
        
        print(f"\n📋 Customer Details:")
        print(f"  Customer ID:    {ctx.get('customer_id')}")
        print(f"  Customer Name:  {ctx.get('customer_name')}")
        print(f"  Region:         {ctx.get('region_name')} ({ctx.get('region_id')})")
        print(f"  Category:       {ctx.get('category_name')} ({ctx.get('category_id')})")
        
        print(f"\n💳 Account Info:")
        print(f"  Status:         {ctx.get('account_status')}")
        print(f"  Type:           {ctx.get('account_type')}")
        print(f"  Credit Limit:   R{ctx.get('credit_limit', 0):,.2f}")
        print(f"  Discount:       {ctx.get('discount_pct', 0)}%")
        print(f"  Payment Terms:  {ctx.get('payment_terms_days', 0)} days")
        
        print(f"\n💰 Age Analysis:")
        print(f"  Total Due:   R{ctx.get('total_due', 0):,.2f}")
        print(f"  Current:     R{ctx.get('amt_current', 0):,.2f}")
        print(f"  30 Days:     R{ctx.get('amt_30_days', 0):,.2f}")
        print(f"  60 Days:     R{ctx.get('amt_60_days', 0):,.2f}")
        print(f"  90+ Days:    R{ctx.get('amt_90_days', 0):,.2f}")
        
        print(f"\n🎯 BI Metrics:")
        print(f"  Category:  {ctx.get('metric_category')}")
        print(f"  KPI Type:  {ctx.get('kpi_type')}")
        print("="*70)
    
    def display_product_message(self, message):
        """Display product master data update"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("📦 PRODUCT UPDATE")
        print("="*70)
        print(f"Event ID:       {data.get('event_id')}")
        print(f"Timestamp:      {data.get('timestamp')}")
        print(f"Operation:      {data.get('operation').upper()}")
        print(f"Priority:       {data.get('priority')}")
        
        print(f"\n📋 Product Details:")
        print(f"  Product ID:      {ctx.get('product_id')}")
        print(f"  Inventory Code:  {ctx.get('inventory_code')}")
        print(f"  Brand:           {ctx.get('brand_name')} ({ctx.get('brand_id')})")
        print(f"  Category:        {ctx.get('category_name')} ({ctx.get('category_id')})")
        print(f"  Range:           {ctx.get('range_name')} ({ctx.get('range_id')})")
        
        print(f"\n🎨 Style Attributes:")
        print(f"  Gender:    {ctx.get('gender')}")
        print(f"  Material:  {ctx.get('material')}")
        print(f"  Colour:    {ctx.get('colour')}")
        
        print(f"\n💰 Inventory:")
        print(f"  Last Cost:  R{ctx.get('last_cost', 0):,.2f}")
        print(f"  In Stock:   {ctx.get('in_stock')}")
        
        print(f"\n🎯 BI Metrics:")
        print(f"  Category:  {ctx.get('metric_category')}")
        print(f"  KPI Type:  {ctx.get('kpi_type')}")
        print("="*70)
    
    def display_supplier_message(self, message):
        """Display supplier master data update"""
        data = message.value
        ctx = data.get('business_context', {})
        
        print("\n" + "="*70)
        print("🏭 SUPPLIER UPDATE")
        print("="*70)
        print(f"Event ID:       {data.get('event_id')}")
        print(f"Timestamp:      {data.get('timestamp')}")
        print(f"Operation:      {data.get('operation').upper()}")
        print(f"Priority:       {data.get('priority')}")
        
        print(f"\n📋 Supplier Details:")
        print(f"  Supplier ID:    {ctx.get('supplier_id')}")
        print(f"  Supplier Name:  {ctx.get('supplier_name')}")
        print(f"  Exclusive:      {ctx.get('exclusive')}")
        
        print(f"\n💳 Terms:")
        print(f"  Credit Limit:   R{ctx.get('credit_limit', 0):,.2f}")
        print(f"  Payment Terms:  {ctx.get('payment_terms_days', 0)} days")
        
        print(f"\n🎯 BI Metrics:")
        print(f"  Category:  {ctx.get('metric_category')}")
        print(f"  KPI Type:  {ctx.get('kpi_type')}")
        print("="*70)
    
    def display_generic_message(self, message):
        """Display generic message (fallback)"""
        data = message.value
        
        print("\n" + "="*70)
        print(f"📨 {data.get('collection', 'UNKNOWN').upper()} - {data.get('operation', 'unknown').upper()}")
        print("="*70)
        print(f"Event ID:    {data.get('event_id')}")
        print(f"Timestamp:   {data.get('timestamp')}")
        print(f"Collection:  {data.get('collection')}")
        print(f"Operation:   {data.get('operation')}")
        print(f"Priority:    {data.get('priority')}")
        
        print("\n📋 Business Context:")
        print(json.dumps(data.get('business_context', {}), indent=2))
        print("="*70)
    
    def start_consuming(self, max_messages: int = None, timeout_seconds: int = 30):
        """
        Start consuming messages from Kafka
        
        Args:
            max_messages: Maximum number of messages to consume (None = unlimited)
            timeout_seconds: Timeout after no messages (for testing)
        """
        if not self.consumer:
            print("❌ Not connected to Kafka")
            return
        
        print("\n" + "="*70)
        print("🟢 CONSUMER STARTED - Listening for messages...")
        print("="*70)
        
        if max_messages:
            print(f"   Will consume up to {max_messages} messages")
        print(f"   Timeout: {timeout_seconds}s of inactivity")
        print("   Press Ctrl+C to stop")
        print("="*70)
        
        last_message_time = datetime.now()
        
        try:
            for message in self.consumer:
                self.message_count += 1
                last_message_time = datetime.now()
                
                # Track messages by collection
                collection = message.value.get('collection', 'unknown')
                self.messages_by_collection[collection] = self.messages_by_collection.get(collection, 0) + 1
                
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
                
                # Print running summary
                print(f"\n📊 Messages consumed: {self.message_count}")
                
                # Check if we've reached max messages
                if max_messages and self.message_count >= max_messages:
                    print(f"\n✅ Reached maximum message limit ({max_messages})")
                    break
                
                # Check for timeout
                if (datetime.now() - last_message_time).seconds > timeout_seconds:
                    print(f"\n⏱️  No messages for {timeout_seconds}s - stopping")
                    break
        
        except KeyboardInterrupt:
            print("\n\n🛑 Stopped by user")
        
        except Exception as e:
            print(f"\n❌ Error consuming messages: {e}")
        
        finally:
            self._print_summary()
            if self.consumer:
                self.consumer.close()
                print("✅ Consumer closed")
    
    def _print_summary(self):
        """Print consumption summary"""
        print("\n" + "="*70)
        print("📊 CONSUMPTION SUMMARY")
        print("="*70)
        print(f"Total messages consumed: {self.message_count}")
        
        if self.messages_by_collection:
            print("\nMessages by collection:")
            for coll, count in sorted(self.messages_by_collection.items(), key=lambda x: x[1], reverse=True):
                print(f"  {coll:12} : {count:,}")
        
        print("="*70)
    
    def export_sample_schema(self, filename: str = "kafka_message_schema.json"):
        """Export sample message schema for Power BI team"""
        
        schema = {
            "metadata": {
                "title": "ClearVue Kafka Message Schema",
                "version": "2.0",
                "date": datetime.now().isoformat(),
                "description": "Message structure for Power BI real-time dashboard"
            },
            
            "common_structure": {
                "event_id": "string - Unique event identifier",
                "timestamp": "string - ISO 8601 timestamp",
                "source": "string - Always 'mongodb_atlas'",
                "database": "string - Database name",
                "collection": "string - Collection name (sales, payments, etc.)",
                "operation": "string - Operation type (insert, update, delete)",
                "priority": "string - Priority level (HIGH, MEDIUM, LOW)",
                "document_key": "string - Document ID",
                "change_data": "object - The actual changed data",
                "business_context": "object - Enriched business intelligence",
                "metadata": "object - Pipeline metadata"
            },
            
            "examples": {
                "sales_transaction": {
                    "event_id": "507f1f77bcf86cd799439011",
                    "timestamp": "2025-10-09T14:30:00.123456",
                    "source": "mongodb_atlas",
                    "database": "Nova_Analytics",
                    "collection": "sales",
                    "operation": "insert",
                    "priority": "HIGH",
                    "document_key": "SALE123456",
                    "business_context": {
                        "transaction_type": "sales",
                        "doc_number": "SALE123456",
                        "customer_id": "CUST001",
                        "rep_id": "REP01",
                        "rep_name": "John Smith",
                        "trans_type": "Tax Invoice",
                        "trans_date": "2025-10-09",
                        "fin_period": 202510,
                        "financial_year": 2025,
                        "financial_month": 10,
                        "financial_quarter": "Q4",
                        "total_amount": 15000.00,
                        "total_cost": 9000.00,
                        "total_profit": 6000.00,
                        "profit_margin_pct": 40.00,
                        "line_count": 3,
                        "metric_category": "revenue",
                        "kpi_type": "sales_revenue"
                    }
                },
                
                "payment_transaction": {
                    "event_id": "507f1f77bcf86cd799439012",
                    "timestamp": "2025-10-09T14:35:00.123456",
                    "source": "mongodb_atlas",
                    "database": "Nova_Analytics",
                    "collection": "payments",
                    "operation": "insert",
                    "priority": "HIGH",
                    "business_context": {
                        "transaction_type": "payment",
                        "customer_id": "CUST001",
                        "deposit_ref": "DEP789456",
                        "deposit_date": "2025-10-09",
                        "fin_period": 202510,
                        "financial_year": 2025,
                        "financial_month": 10,
                        "financial_quarter": "Q4",
                        "total_bank_amount": 15000.00,
                        "total_discount": 750.00,
                        "total_payment": 14250.00,
                        "discount_pct": 5.00,
                        "payment_count": 1,
                        "metric_category": "cash_flow",
                        "kpi_type": "payments_received"
                    }
                },
                
                "customer_update": {
                    "event_id": "507f1f77bcf86cd799439013",
                    "timestamp": "2025-10-09T14:40:00.123456",
                    "source": "mongodb_atlas",
                    "database": "Nova_Analytics",
                    "collection": "customers",
                    "operation": "update",
                    "priority": "MEDIUM",
                    "business_context": {
                        "entity_type": "customer",
                        "customer_id": "CUST001",
                        "customer_name": "ABC Fashion",
                        "region_id": "REG05",
                        "region_name": "Cape Town",
                        "category_id": "CAT01",
                        "category_name": "Specialty Stores",
                        "credit_limit": 50000.00,
                        "total_due": 25000.00,
                        "metric_category": "customer_master",
                        "kpi_type": "customer_credit"
                    }
                }
            },
            
            "power_bi_integration": {
                "recommended_approach": "Power BI Streaming Dataset or DirectQuery to Kafka",
                
                "key_fields_for_bi": {
                    "dimensions": [
                        "collection",
                        "operation",
                        "priority",
                        "business_context.customer_id",
                        "business_context.supplier_id",
                        "business_context.region_name",
                        "business_context.category_name",
                        "business_context.financial_quarter"
                    ],
                    "measures": [
                        "business_context.total_amount",
                        "business_context.total_payment",
                        "business_context.total_cost",
                        "business_context.total_profit",
                        "business_context.profit_margin_pct",
                        "business_context.credit_limit",
                        "business_context.total_due"
                    ],
                    "time_fields": [
                        "timestamp",
                        "business_context.trans_date",
                        "business_context.deposit_date",
                        "business_context.fin_period",
                        "business_context.financial_year",
                        "business_context.financial_month",
                        "business_context.financial_quarter"
                    ]
                },
                
                "sample_dax_measures": {
                    "total_revenue": "SUM(business_context.total_amount)",
                    "total_profit": "SUM(business_context.total_profit)",
                    "avg_profit_margin": "AVERAGE(business_context.profit_margin_pct)",
                    "total_payments": "SUM(business_context.total_payment)",
                    "outstanding_receivables": "SUM(business_context.total_due)"
                }
            },
            
            "data_types": {
                "string_fields": [
                    "event_id", "timestamp", "source", "database", "collection",
                    "operation", "priority", "document_key", "customer_id",
                    "supplier_id", "product_id", "region_name", "category_name"
                ],
                "numeric_fields": [
                    "total_amount", "total_payment", "total_cost", "total_profit",
                    "profit_margin_pct", "credit_limit", "total_due", "discount_pct",
                    "fin_period", "financial_year", "financial_month"
                ],
                "boolean_fields": [
                    "in_stock", "exclusive"
                ],
                "datetime_fields": [
                    "timestamp", "trans_date", "deposit_date", "purch_date"
                ]
            }
        }
        
        # Write to file
        with open(filename, 'w') as f:
            json.dump(schema, f, indent=2)
        
        print(f"\n✅ Schema exported to: {filename}")
        print("   Share this file with the Power BI team")
        print("\n📋 Schema includes:")
        print("   - Message structure documentation")
        print("   - Sample messages for each collection type")
        print("   - Power BI integration recommendations")
        print("   - Field mappings and data types")
        print("   - Sample DAX measures")


def main():
    """Main entry point for consumer test"""
    
    test_consumer = ClearVueKafkaConsumerTest()
    
    # Get user choice
    choice = input(f"\n👉 Select topic (1-{len(ClearVueConfig.KAFKA_TOPICS) + 1}) [default: ALL]: ").strip() or str(len(ClearVueConfig.KAFKA_TOPICS) + 1)
    
    # Map choice to topics
    topics = []
    if choice == str(len(ClearVueConfig.KAFKA_TOPICS) + 1):
        # All topics
        topics = [config['topic'] for config in ClearVueConfig.KAFKA_TOPICS.values()]
    else:
        # Individual topic
        try:
            idx = int(choice) - 1
            collection = list(ClearVueConfig.KAFKA_TOPICS.keys())[idx]
            topics = [ClearVueConfig.KAFKA_TOPICS[collection]['topic']]
        except (ValueError, IndexError):
            print("❌ Invalid choice")
            sys.exit(1)
    
    # Ask about schema export
    print("\n" + "="*70)
    export = input("📄 Export message schema for Power BI team? (y/n) [y]: ").strip().lower() or 'y'
    if export == 'y':
        test_consumer.export_sample_schema()
    
    # Ask about message limit
    print("\n" + "="*70)
    limit_input = input("🔢 Maximum messages to consume (blank = unlimited): ").strip()
    max_messages = int(limit_input) if limit_input else None
    
    # Connect and start consuming
    print("\n" + "="*70)
    print("🚀 STARTING KAFKA CONSUMER")
    print("="*70)
    
    if test_consumer.connect(topics):
        test_consumer.start_consuming(max_messages=max_messages)
    else:
        print("\n❌ Failed to connect to Kafka")
        print("\n💡 Troubleshooting:")
        print("   1. Make sure Docker services are running:")
        print("      docker-compose up -d")
        print("   2. Check Kafka health:")
        print("      docker-compose ps")
        print("   3. Check Kafka logs:")
        print("      docker-compose logs kafka")
        print("   4. Verify Kafka UI at: http://localhost:8080")
        print("   5. Make sure streaming pipeline is running")
        sys.exit(1)


if __name__ == "__main__":
    main()