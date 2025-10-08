"""
ClearVue Enhanced Transaction Simulator
Generates realistic transactions for ALL 6 collections
Tests the complete streaming pipeline with varied data

Collections Tested:
1. Sales (with embedded lines)
2. Payments (with embedded lines)
3. Purchases (with embedded lines)
4. Customers (master data updates)
5. Products (master data updates)
6. Suppliers (master data updates)

Author: ClearVue Streaming Team
Date: October 2025
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
fake.seed_instance(42)

class ClearVueEnhancedSimulator:
    """Enhanced simulator for testing all 6 ClearVue collections"""
    
    def __init__(self,
                 mongo_uri=None,
                 database_name=None):
        """Initialize simulator with MongoDB Atlas connection"""
        
        print("\n" + "="*70)
        print("ClearVue Enhanced Transaction Simulator")
        print("="*70)
        
        try:
            print("🔌 Connecting to MongoDB Atlas...")
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            print("✅ Connected to MongoDB Atlas!")
            
            self.db = self.client[database_name]
            
            # Simulation control
            self.is_running = False
            self.transaction_count = 0
            
            # Cache for reference data
            self.sample_customers = []
            self.sample_products = []
            self.sample_suppliers = []
            
            # Load or create sample reference data
            self.initialize_reference_data()
            
            print("="*70 + "\n")
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise
    
    def initialize_reference_data(self):
        """Load existing reference data or create samples"""
        print("📚 Initializing reference data...")
        
        # Check what exists
        customer_count = self.db.customers.count_documents({})
        product_count = self.db.products.count_documents({})
        supplier_count = self.db.suppliers.count_documents({})
        
        print(f"   Existing: {customer_count} customers, {product_count} products, {supplier_count} suppliers")
        
        # Load samples if they exist
        if customer_count > 0:
            self.sample_customers = list(self.db.customers.find().limit(100))
        if product_count > 0:
            self.sample_products = list(self.db.products.find().limit(50))
        if supplier_count > 0:
            self.sample_suppliers = list(self.db.suppliers.find().limit(20))
        
        # Create minimal samples if needed
        if not self.sample_customers:
            print("   Creating sample customers...")
            self.sample_customers = self.create_sample_customers(10)
        if not self.sample_products:
            print("   Creating sample products...")
            self.sample_products = self.create_sample_products(10)
        if not self.sample_suppliers:
            print("   Creating sample suppliers...")
            self.sample_suppliers = self.create_sample_suppliers(5)
        
        print(f"✅ Ready with {len(self.sample_customers)} customers, "
              f"{len(self.sample_products)} products, {len(self.sample_suppliers)} suppliers")
    
    def create_sample_customers(self, count=10):
        """Create sample customer documents"""
        customers = []
        regions = ['Gauteng', 'Western Cape', 'KwaZulu-Natal', 'Eastern Cape']
        categories = ['Retail', 'Wholesale', 'Specialty Stores', 'Online']
        
        for i in range(count):
            customer_id = f"CUST{1000 + i}"
            customer = {
                '_id': customer_id,
                'name': fake.company(),
                'category': {'id': f"CAT{random.randint(1,5):02d}", 'desc': random.choice(categories)},
                'region': {'id': f"REG{random.randint(1,9):02d}", 'desc': random.choice(regions)},
                'account': {'status': 'Open', 'type': 'Credit'},
                'settle_terms': random.choice(['30 days', '60 days', '90 days']),
                'normal_payterms': random.choice([30, 60, 90]),
                'discount': random.choice([0, 5, 10, 15]),
                'credit_limit': random.randint(10000, 100000),
                'age_analysis': {
                    'total_due': 0,
                    'amt_current': 0,
                    'amt_30_days': 0,
                    'amt_60_days': 0,
                    'amt_90_days': 0
                },
                '_created_by': 'simulator',
                '_created_at': datetime.now()
            }
            customers.append(customer)
        
        self.db.customers.insert_many(customers)
        return customers
    
    def create_sample_products(self, count=10):
        """Create sample product documents"""
        products = []
        brands = ['Nike', 'Adidas', 'Puma', 'Reebok', 'Under Armour']
        categories = ['Shoes', 'Apparel', 'Accessories', 'Equipment']
        ranges = ['Running', 'Training', 'Casual', 'Sport']
        
        for i in range(count):
            inventory_code = f"INV{2000 + i}"
            product = {
                '_id': f"PROD{1000 + i}",
                'brand': {'id': f"BR{random.randint(1,10):02d}", 'desc': random.choice(brands)},
                'category': {'id': f"CAT{random.randint(20,30)}", 'desc': random.choice(categories)},
                'range': {'id': f"RNG{random.randint(1,5):02d}", 'desc': random.choice(ranges)},
                'style': {
                    'gender': random.choice(['Male', 'Female', 'Unisex']),
                    'material': random.choice(['Leather', 'Synthetic', 'Cotton', 'Mesh']),
                    'style': random.choice(['Casual', 'Sport', 'Streetwear', 'Professional']),
                    'colour': random.choice(['Black', 'White', 'Blue', 'Red', 'Grey']),
                    'branding': random.choice(brands),
                    'qual_probs': None
                },
                'inventory_code': inventory_code,
                'last_cost': round(random.uniform(200, 2000), 2),
                'stock_ind': random.choice([True, False]),
                '_created_by': 'simulator',
                '_created_at': datetime.now()
            }
            products.append(product)
        
        self.db.products.insert_many(products)
        return products
    
    def create_sample_suppliers(self, count=5):
        """Create sample supplier documents"""
        suppliers = []
        
        for i in range(count):
            supplier_id = f"SUPP{100 + i}"
            supplier = {
                '_id': supplier_id,
                'description': fake.company() + " Ltd",
                'exclusive': random.choice([True, False]),
                'normal_payterms': random.choice([30, 60, 90]),
                'credit_limit': random.randint(50000, 500000),
                '_created_by': 'simulator',
                '_created_at': datetime.now()
            }
            suppliers.append(supplier)
        
        self.db.suppliers.insert_many(suppliers)
        return suppliers
    
    def calculate_financial_period(self, date=None):
        """Calculate financial period in YYYYMM format"""
        if date is None:
            date = datetime.now()
        return int(f"{date.year}{date.month:02d}")
    
    def generate_sales_transaction(self):
        """Generate a realistic sales transaction with embedded lines"""
        if not self.sample_customers or not self.sample_products:
            print("❌ Missing reference data")
            return None
        
        customer = random.choice(self.sample_customers)
        trans_date = datetime.now()
        fin_period = self.calculate_financial_period(trans_date)
        
        # Generate sales document with embedded lines
        doc_number = f"SALE{int(time.time() * 1000) % 1000000}"
        
        # Generate 1-5 line items
        lines = []
        for _ in range(random.randint(1, 5)):
            product = random.choice(self.sample_products)
            quantity = random.randint(1, 10)
            unit_price = product.get('last_cost', 500) * random.uniform(1.3, 2.0)  # Markup
            total = quantity * unit_price
            
            line = {
                'product_id': product['_id'],
                'quantity': quantity,
                'unit_sell_price': round(unit_price, 2),
                'total_line_cost': round(total, 2),
                'last_cost': product.get('last_cost', 500)
            }
            lines.append(line)
        
        sales_doc = {
            '_id': doc_number,
            'customer_id': customer['_id'],
            'rep': {'id': f"REP{random.randint(1,20):02d}", 'desc': fake.name(), 'commission': random.choice([5, 10, 15])},
            'trans_type': random.choice(['Tax Invoice', 'Cash Sale', 'Credit Note']),
            'trans_date': trans_date,
            'fin_period': fin_period,
            'lines': lines,
            '_source': 'simulator',
            '_created_at': datetime.now()
        }
        
        return sales_doc
    
    def generate_payment_transaction(self):
        """Generate a realistic payment transaction with embedded lines"""
        if not self.sample_customers:
            print("❌ No customers available")
            return None
        
        customer = random.choice(self.sample_customers)
        deposit_date = datetime.now()
        fin_period = self.calculate_financial_period(deposit_date)
        deposit_ref = f"PAY{int(time.time() * 1000) % 1000000}"
        
        # Generate 1-3 payment lines
        lines = []
        for _ in range(random.randint(1, 3)):
            bank_amt = round(random.uniform(1000, 50000), 2)
            discount = round(bank_amt * customer.get('discount', 0) / 100, 2)
            tot_payment = bank_amt - discount
            
            line = {
                'fin_period': fin_period,
                'deposit_date': deposit_date,
                'bank_amt': bank_amt,
                'discount': discount,
                'tot_payment': tot_payment
            }
            lines.append(line)
        
        payment_doc = {
            '_id': f"PAYM{int(time.time() * 1000) % 1000000}",
            'customer_id': customer['_id'],
            'deposit_ref': deposit_ref,
            'lines': lines,
            '_source': 'simulator',
            '_created_at': datetime.now()
        }
        
        return payment_doc
    
    def generate_purchase_transaction(self):
        """Generate a realistic purchase transaction with embedded lines"""
        if not self.sample_suppliers or not self.sample_products:
            print("❌ Missing reference data")
            return None
        
        supplier = random.choice(self.sample_suppliers)
        purch_date = datetime.now()
        purch_doc_no = f"PO{int(time.time() * 1000) % 1000000}"
        
        # Generate 1-5 purchase lines
        lines = []
        for _ in range(random.randint(1, 5)):
            product = random.choice(self.sample_products)
            quantity = random.randint(10, 200)
            unit_cost = product.get('last_cost', 500)
            total_cost = quantity * unit_cost
            
            line = {
                'product_id': product['_id'],
                'quantity': quantity,
                'unit_cost_price': unit_cost,
                'total_line_cost': round(total_cost, 2)
            }
            lines.append(line)
        
        purchase_doc = {
            '_id': purch_doc_no,
            'supplier_id': supplier['_id'],
            'purch_date': purch_date,
            'lines': lines,
            '_source': 'simulator',
            '_created_at': datetime.now()
        }
        
        return purchase_doc
    
    def update_customer(self):
        """Update an existing customer's information"""
        if not self.sample_customers:
            return None
        
        customer = random.choice(self.sample_customers)
        
        # Random update types
        update_type = random.choice(['credit_limit', 'age_analysis', 'discount'])
        
        if update_type == 'credit_limit':
            new_limit = random.randint(10000, 150000)
            self.db.customers.update_one(
                {'_id': customer['_id']},
                {
                    '$set': {
                        'credit_limit': new_limit,
                        '_updated_at': datetime.now()
                    }
                }
            )
            return {'type': 'customer_update', 'customer_id': customer['_id'], 'field': 'credit_limit', 'value': new_limit}
        
        elif update_type == 'age_analysis':
            self.db.customers.update_one(
                {'_id': customer['_id']},
                {
                    '$set': {
                        'age_analysis.total_due': random.randint(5000, 50000),
                        'age_analysis.amt_current': random.randint(1000, 20000),
                        'age_analysis.amt_30_days': random.randint(0, 10000),
                        '_updated_at': datetime.now()
                    }
                }
            )
            return {'type': 'customer_update', 'customer_id': customer['_id'], 'field': 'age_analysis'}
        
        else:
            new_discount = random.choice([0, 5, 10, 15])
            self.db.customers.update_one(
                {'_id': customer['_id']},
                {
                    '$set': {
                        'discount': new_discount,
                        '_updated_at': datetime.now()
                    }
                }
            )
            return {'type': 'customer_update', 'customer_id': customer['_id'], 'field': 'discount', 'value': new_discount}
    
    def update_product(self):
        """Update an existing product's information"""
        if not self.sample_products:
            return None
        
        product = random.choice(self.sample_products)
        
        # Update cost or stock status
        update_type = random.choice(['cost', 'stock'])
        
        if update_type == 'cost':
            new_cost = round(random.uniform(200, 2500), 2)
            self.db.products.update_one(
                {'_id': product['_id']},
                {
                    '$set': {
                        'last_cost': new_cost,
                        '_updated_at': datetime.now()
                    }
                }
            )
            return {'type': 'product_update', 'product_id': product['_id'], 'field': 'last_cost', 'value': new_cost}
        
        else:
            new_stock = random.choice([True, False])
            self.db.products.update_one(
                {'_id': product['_id']},
                {
                    '$set': {
                        'stock_ind': new_stock,
                        '_updated_at': datetime.now()
                    }
                }
            )
            return {'type': 'product_update', 'product_id': product['_id'], 'field': 'stock_ind', 'value': new_stock}
    
    def update_supplier(self):
        """Update an existing supplier's information"""
        if not self.sample_suppliers:
            return None
        
        supplier = random.choice(self.sample_suppliers)
        
        # Update credit limit
        new_limit = random.randint(50000, 600000)
        self.db.suppliers.update_one(
            {'_id': supplier['_id']},
            {
                '$set': {
                    'credit_limit': new_limit,
                    '_updated_at': datetime.now()
                }
            }
        )
        return {'type': 'supplier_update', 'supplier_id': supplier['_id'], 'field': 'credit_limit', 'value': new_limit}
    
    def insert_transaction(self, transaction_type='mixed'):
        """Insert a transaction into MongoDB"""
        try:
            if transaction_type == 'sales' or (transaction_type == 'mixed' and random.random() < 0.4):
                sales_doc = self.generate_sales_transaction()
                if sales_doc:
                    self.db.sales.insert_one(sales_doc)
                    total = sum(line['total_line_cost'] for line in sales_doc['lines'])
                    print(f"🛒 SALES: {sales_doc['_id']} | R{total:,.2f} | {len(sales_doc['lines'])} items")
                    self.transaction_count += 1
                    return True
            
            elif transaction_type == 'payments' or (transaction_type == 'mixed' and random.random() < 0.4):
                payment_doc = self.generate_payment_transaction()
                if payment_doc:
                    self.db.payments.insert_one(payment_doc)
                    total = sum(line['tot_payment'] for line in payment_doc['lines'])
                    print(f"💳 PAYMENT: {payment_doc['deposit_ref']} | R{total:,.2f}")
                    self.transaction_count += 1
                    return True
            
            elif transaction_type == 'purchases' or (transaction_type == 'mixed' and random.random() < 0.3):
                purchase_doc = self.generate_purchase_transaction()
                if purchase_doc:
                    self.db.purchases.insert_one(purchase_doc)
                    total = sum(line['total_line_cost'] for line in purchase_doc['lines'])
                    print(f"📦 PURCHASE: {purchase_doc['_id']} | R{total:,.2f}")
                    self.transaction_count += 1
                    return True
            
            elif transaction_type == 'customer_update' or (transaction_type == 'mixed' and random.random() < 0.15):
                result = self.update_customer()
                if result:
                    print(f"👤 CUSTOMER UPDATE: {result['customer_id']} | {result['field']}")
                    self.transaction_count += 1
                    return True
            
            elif transaction_type == 'product_update' or (transaction_type == 'mixed' and random.random() < 0.1):
                result = self.update_product()
                if result:
                    print(f"📦 PRODUCT UPDATE: {result['product_id']} | {result['field']}")
                    self.transaction_count += 1
                    return True
            
            elif transaction_type == 'supplier_update' or (transaction_type == 'mixed' and random.random() < 0.05):
                result = self.update_supplier()
                if result:
                    print(f"🏭 SUPPLIER UPDATE: {result['supplier_id']} | {result['field']}")
                    self.transaction_count += 1
                    return True
            
            return False
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def run_burst(self, count=20, delay_range=(0.5, 2)):
        """Run a burst of mixed transactions"""
        print(f"\n💥 Simulating {count} mixed transactions...\n")
        
        for i in range(count):
            self.insert_transaction('mixed')
            if i < count - 1:
                time.sleep(random.uniform(delay_range[0], delay_range[1]))
        
        print(f"\n✅ Burst complete: {count} transactions generated")
    
    def run_continuous(self, transactions_per_minute=10):
        """Run continuous transaction simulation"""
        if self.is_running:
            print("⚠️ Simulation already running")
            return
        
        self.is_running = True
        interval = 60 / transactions_per_minute
        
        print(f"\n🚀 Starting continuous simulation ({transactions_per_minute} tx/min)")
        print("   Press Ctrl+C to stop\n")
        
        def simulation_loop():
            while self.is_running:
                try:
                    self.insert_transaction('mixed')
                    print(f"   [Total: {self.transaction_count}]")
                    time.sleep(interval)
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(f"❌ Error: {e}")
                    time.sleep(1)
        
        try:
            thread = threading.Thread(target=simulation_loop, daemon=True)
            thread.start()
            thread.join()
        except KeyboardInterrupt:
            print("\n🛑 Stopping simulation...")
            self.is_running = False
    
    def stop(self):
        """Stop simulation"""
        self.is_running = False
        print(f"\n✅ Simulation stopped. Generated {self.transaction_count} transactions.")
    
    def close(self):
        """Close database connection"""
        self.client.close()
        print("✅ Connection closed")


def main():
    """Main entry point"""
    print("\n" + "="*70)
    print("CLEARVUE ENHANCED TRANSACTION SIMULATOR")
    print("Tests ALL 6 Collections with Realistic Data")
    print("="*70)
    print("\nSimulation Modes:")
    print("1. Mixed burst (20 transactions - all types)")
    print("2. Continuous simulation (10 tx/min)")
    print("3. Heavy load test (30 tx/min)")
    print("4. Sales only (10 transactions)")
    print("5. Payments only (10 transactions)")
    print("6. Master data updates only")
    
    choice = input("\nSelect mode (1-6) [default: 1]: ").strip() or "1"
    
    simulator = None
    try:
        simulator = ClearVueEnhancedSimulator()
        
        if choice == "1":
            simulator.run_burst(20)
        elif choice == "2":
            simulator.run_continuous(10)
        elif choice == "3":
            print("⚡ HEAVY LOAD MODE...")
            simulator.run_continuous(30)
        elif choice == "4":
            print("🛒 Sales transactions only...")
            for _ in range(10):
                simulator.insert_transaction('sales')
                time.sleep(1)
        elif choice == "5":
            print("💳 Payment transactions only...")
            for _ in range(10):
                simulator.insert_transaction('payments')
                time.sleep(1)
        elif choice == "6":
            print("📊 Master data updates...")
            for _ in range(5):
                simulator.insert_transaction('customer_update')
                time.sleep(0.5)
            for _ in range(5):
                simulator.insert_transaction('product_update')
                time.sleep(0.5)
        else:
            print("Invalid option")
        
        print(f"\n{'='*70}")
        print(f"SIMULATION COMPLETE")
        print(f"Total transactions: {simulator.transaction_count}")
        print(f"{'='*70}\n")
        
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
        if simulator:
            simulator.stop()
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        if simulator:
            simulator.close()


if __name__ == "__main__":
    main()