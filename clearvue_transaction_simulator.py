"""
ClearVue Real-Time Transaction Simulator
Simulates live payment transactions for streaming pipeline testing
"""
import json
import random
import time
import threading
from datetime import datetime, timedelta
from pymongo import MongoClient
from faker import Faker
import uuid

fake = Faker()

class ClearVueTransactionSimulator:
    def __init__(self):
        # Connect to MongoDB
        self.client = MongoClient('mongodb://admin:clearvue123@localhost:27017/')
        self.db = self.client['clearvue_bi']
        
        # Cache reference data for faster simulation
        self.customers = []
        self.products = []
        self.regions = ['Gauteng', 'Western Cape', 'KwaZulu-Natal', 'Eastern Cape']
        
        # Simulation control
        self.is_running = False
        self.transaction_count = 0
        
        # Load reference data
        self.load_reference_data()
    
    def load_reference_data(self):
        """Load customers and products for realistic transactions"""
        print("📚 Loading reference data...")
        
        # Load customers
        self.customers = list(self.db.customers.find(
            {'status': 'Active'}, 
            {'customer_id': 1, 'company_name': 1, 'address.region': 1}
        ))
        
        # Load products
        self.products = list(self.db.product_brands.find(
            {}, 
            {'product_id': 1, 'product_name': 1, 'unit_price': 1, 'category': 1}
        ))
        
        print(f"   ✅ Loaded {len(self.customers)} customers")
        print(f"   ✅ Loaded {len(self.products)} products")
    
    def get_financial_month_info(self, date=None):
        """Get ClearVue financial month info for a given date"""
        if date is None:
            date = datetime.now()
        
        # Simplified financial month calculation
        # In real implementation, you'd use ClearVue's exact calendar logic
        return {
            'financial_month': date.month,
            'financial_year': date.year if date.month >= 3 else date.year - 1,
            'financial_quarter': ((date.month - 1) // 3) + 1
        }
    
    def generate_realistic_transaction(self):
        """Generate a realistic payment transaction"""
        if not self.customers or not self.products:
            print("❌ No reference data available")
            return None
        
        # Select random customer and region
        customer = random.choice(self.customers)
        customer_region = customer.get('address', {}).get('region', random.choice(self.regions))
        
        # Generate payment header
        payment_date = datetime.now()
        financial_info = self.get_financial_month_info(payment_date)
        
        payment_header = {
            '_id': str(uuid.uuid4()),
            'payment_id': fake.unique.random_int(min=100000, max=999999),
            'customer_id': customer['customer_id'],
            'customer_name': customer.get('company_name', 'Unknown'),
            'payment_date': payment_date,
            'financial_month': financial_info['financial_month'],
            'financial_year': financial_info['financial_year'],
            'financial_quarter': financial_info['financial_quarter'],
            'region': customer_region,
            'payment_method': random.choice(['Credit Card', 'EFT', 'Cash', 'Debit Card']),
            'currency': 'ZAR',
            'status': random.choices(
                ['Completed', 'Pending', 'Failed'], 
                weights=[85, 10, 5]  # Most transactions succeed
            )[0],
            'reference': f"TXN{fake.uuid4()[:8].upper()}",
            'created_at': payment_date,
            'updated_at': payment_date
        }
        
        # Generate 1-3 payment lines
        payment_lines = []
        total_amount = 0
        num_lines = random.randint(1, 3)
        
        for line_num in range(1, num_lines + 1):
            product = random.choice(self.products)
            quantity = random.randint(1, 5)
            unit_price = product['unit_price']
            discount_percent = random.choices(
                [0, 5, 10, 15], 
                weights=[70, 15, 10, 5]  # Most items have no discount
            )[0]
            
            line_subtotal = quantity * unit_price
            discount_amount = line_subtotal * (discount_percent / 100)
            line_total = line_subtotal - discount_amount
            total_amount += line_total
            
            payment_line = {
                '_id': str(uuid.uuid4()),
                'payment_id': payment_header['payment_id'],
                'line_number': line_num,
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': unit_price,
                'discount_percent': discount_percent,
                'discount_amount': round(discount_amount, 2),
                'line_total': round(line_total, 2),
                'vat_rate': 15.0,
                'created_at': payment_date,
                'updated_at': payment_date
            }
            payment_lines.append(payment_line)
        
        # Update header with totals
        vat_amount = round(total_amount * 0.15 / 1.15, 2)
        payment_header.update({
            'subtotal': round(total_amount - vat_amount, 2),
            'vat_amount': vat_amount,
            'total_amount': round(total_amount, 2),
            'line_count': len(payment_lines)
        })
        
        return {
            'payment_header': payment_header,
            'payment_lines': payment_lines
        }
    
    def insert_transaction(self, transaction_data):
        """Insert transaction into MongoDB"""
        try:
            # Insert payment header
            self.db.payment_headers.insert_one(transaction_data['payment_header'])
            
            # Insert payment lines
            if transaction_data['payment_lines']:
                self.db.payment_lines.insert_many(transaction_data['payment_lines'])
            
            self.transaction_count += 1
            return True
            
        except Exception as e:
            print(f"❌ Error inserting transaction: {e}")
            return False
    
    def simulate_transaction_burst(self, count=5, delay_range=(1, 5)):
        """Simulate a burst of transactions"""
        print(f"💥 Simulating burst of {count} transactions...")
        
        for i in range(count):
            transaction = self.generate_realistic_transaction()
            if transaction:
                if self.insert_transaction(transaction):
                    payment_id = transaction['payment_header']['payment_id']
                    amount = transaction['payment_header']['total_amount']
                    region = transaction['payment_header']['region']
                    print(f"   ✅ Transaction {payment_id}: R{amount:,.2f} ({region})")
                
                # Random delay between transactions
                if i < count - 1:
                    delay = random.uniform(delay_range[0], delay_range[1])
                    time.sleep(delay)
    
    def start_continuous_simulation(self, transactions_per_minute=10):
        """Start continuous transaction simulation"""
        if self.is_running:
            print("⚠️ Simulation already running")
            return
        
        self.is_running = True
        interval = 60 / transactions_per_minute  # seconds between transactions
        
        print(f"🚀 Starting continuous simulation ({transactions_per_minute} tx/min)")
        print("   Press Ctrl+C to stop")
        
        def simulation_loop():
            while self.is_running:
                try:
                    transaction = self.generate_realistic_transaction()
                    if transaction:
                        if self.insert_transaction(transaction):
                            payment_id = transaction['payment_header']['payment_id']
                            amount = transaction['payment_header']['total_amount']
                            status = transaction['payment_header']['status']
                            print(f"💳 {payment_id}: R{amount:,.2f} ({status}) [Total: {self.transaction_count}]")
                    
                    time.sleep(interval)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(f"❌ Simulation error: {e}")
                    time.sleep(1)
        
        try:
            simulation_thread = threading.Thread(target=simulation_loop)
            simulation_thread.daemon = True
            simulation_thread.start()
            simulation_thread.join()
        except KeyboardInterrupt:
            print("\n🛑 Stopping simulation...")
            self.is_running = False
    
    def stop_simulation(self):
        """Stop the continuous simulation"""
        self.is_running = False
        print(f"✅ Simulation stopped. Generated {self.transaction_count} transactions.")
    
    def get_recent_transactions(self, minutes=5):
        """Get transactions from the last N minutes"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        recent_txns = list(self.db.payment_headers.find(
            {'created_at': {'$gte': cutoff_time}},
            sort=[('created_at', -1)]
        ))
        
        return recent_txns

if __name__ == "__main__":
    simulator = ClearVueTransactionSimulator()
    
    print("ClearVue Transaction Simulator")
    print("=" * 40)
    print("1. Single transaction")
    print("2. Transaction burst (5 transactions)")
    print("3. Continuous simulation (10 tx/min)")
    print("4. View recent transactions")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    if choice == "1":
        transaction = simulator.generate_realistic_transaction()
        if transaction and simulator.insert_transaction(transaction):
            print("✅ Single transaction generated successfully!")
    
    elif choice == "2":
        simulator.simulate_transaction_burst()
    
    elif choice == "3":
        try:
            simulator.start_continuous_simulation()
        except KeyboardInterrupt:
            simulator.stop_simulation()
    
    elif choice == "4":
        recent = simulator.get_recent_transactions(10)
        print(f"\n📊 Recent transactions (last 10 minutes): {len(recent)}")
        for txn in recent[:5]:  # Show last 5
            print(f"   {txn['payment_id']}: R{txn['total_amount']:,.2f} ({txn['region']})")
    
    else:
        print("Invalid option")