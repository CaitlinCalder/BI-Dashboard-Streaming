"""
ClearVue Enhanced Transaction Simulator - Version 2.0
Generates realistic transactions directly into MongoDB (6 collections)

NO DATA TRANSFORMATION - Just realistic data generation!
The data cleaning team already transformed the initial 17 CSV files.
This simulator creates NEW transactions in the correct format.

Collections Generated:
1. Sales (with embedded lines) - HIGH PRIORITY
2. Payments (with embedded lines) - HIGH PRIORITY  
3. Purchases (with embedded lines) - MEDIUM PRIORITY
4. Customers (master data updates) - MEDIUM PRIORITY
5. Products (master data updates) - MEDIUM PRIORITY
6. Suppliers (master data updates) - LOW PRIORITY

Author: ClearVue Streaming Team
Date: October 2025
"""

import json
import random
import time
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from faker import Faker

# Import configuration
try:
    from config import ClearVueConfig, print_startup_banner
except ImportError:
    print("❌ Error: config.py not found")
    exit(1)

# Initialize Faker for realistic data
fake = Faker()
fake.seed_instance(42)  # Reproducible random data

# South African specific data
SA_REGIONS = ['Gauteng', 'Western Cape', 'KwaZulu-Natal', 'Eastern Cape', 
              'Mpumalanga', 'Limpopo', 'North West', 'Free State', 'Northern Cape']
SA_CITIES = ['Johannesburg', 'Cape Town', 'Durban', 'Pretoria', 'Port Elizabeth',
             'Bloemfontein', 'Nelspruit', 'Polokwane']

# Business categories
CUSTOMER_CATEGORIES = ['Specialty Stores', 'Retail Chains', 'Wholesalers', 
                       'Online Retailers', 'Department Stores', 'Boutiques']
PRODUCT_BRANDS = ['Nike', 'Adidas', 'Puma', 'Reebok', 'Under Armour', 'New Balance',
                  'Asics', 'Converse', 'Vans', 'Fila']
PRODUCT_CATEGORIES = ['Shoes', 'Apparel', 'Accessories', 'Equipment', 'Sportswear']
PRODUCT_RANGES = ['Running', 'Training', 'Casual', 'Sport', 'Lifestyle', 'Performance']


class ClearVueTransactionSimulator:
    """
    Enhanced simulator for generating realistic ClearVue transactions
    Inserts directly into MongoDB (already in correct format)
    """
    
    def __init__(self):
        """Initialize simulator with MongoDB connection"""
        
        print_startup_banner()
        
        print("🎲 CLEARVUE TRANSACTION SIMULATOR v2.0")
        print("="*70)
        
        # Get MongoDB connection
        self.mongo_uri = ClearVueConfig.get_mongo_uri()
        self.db_name = ClearVueConfig.get_database_name()
        
        # Connect to MongoDB
        self._connect_mongodb()
        
        # Simulation state
        self.is_running = False
        self.transaction_count = 0
        
        # Cache for reference data (loaded from existing DB)
        self.sample_customers = []
        self.sample_products = []
        self.sample_suppliers = []
        
        # Load or create reference data
        self._initialize_reference_data()
        
        print("="*70 + "\n")
    
    def _connect_mongodb(self):
        """Connect to MongoDB Atlas"""
        print("\n🔌 Connecting to MongoDB...")
        
        try:
            self.client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=20000
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            
            print(f"✅ Connected to MongoDB: {self.db_name}")
            
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
            raise
    
    def _initialize_reference_data(self):
        """Load existing reference data or create minimal samples"""
        print("\n📚 Initializing reference data...")
        
        # Check what exists in database
        customer_count = self.db.customers.count_documents({})
        product_count = self.db.products.count_documents({})
        supplier_count = self.db.suppliers.count_documents({})
        
        print(f"   Found: {customer_count} customers, {product_count} products, {supplier_count} suppliers")
        
        # Load samples from database if they exist
        if customer_count > 0:
            self.sample_customers = list(self.db.customers.find().limit(100))
            print(f"   ✓ Loaded {len(self.sample_customers)} sample customers")
        
        if product_count > 0:
            self.sample_products = list(self.db.products.find().limit(100))
            print(f"   ✓ Loaded {len(self.sample_products)} sample products")
        
        if supplier_count > 0:
            self.sample_suppliers = list(self.db.suppliers.find().limit(50))
            print(f"   ✓ Loaded {len(self.sample_suppliers)} sample suppliers")
        
        # Create minimal samples if database is empty
        if not self.sample_customers:
            print("   ⚠️  No customers found - creating 20 sample customers...")
            self.sample_customers = self._create_sample_customers(20)
        
        if not self.sample_products:
            print("   ⚠️  No products found - creating 30 sample products...")
            self.sample_products = self._create_sample_products(30)
        
        if not self.sample_suppliers:
            print("   ⚠️  No suppliers found - creating 10 sample suppliers...")
            self.sample_suppliers = self._create_sample_suppliers(10)
        
        print(f"\n✅ Reference data ready:")
        print(f"   {len(self.sample_customers)} customers")
        print(f"   {len(self.sample_products)} products")
        print(f"   {len(self.sample_suppliers)} suppliers")
    
    def _create_sample_customers(self, count: int = 20) -> List[Dict]:
        """Create sample customer documents in correct format"""
        customers = []
        
        for i in range(count):
            customer_id = f"CUST{1000 + i}"
            region = random.choice(SA_REGIONS)
            category = random.choice(CUSTOMER_CATEGORIES)
            
            customer = {
                '_id': customer_id,
                'name': fake.company(),
                'category': {
                    'id': f"CAT{random.randint(1, 20):02d}",
                    'desc': category
                },
                'region': {
                    'id': f"REG{random.randint(1, 9):02d}",
                    'desc': region
                },
                'account': {
                    'status': random.choice(['Open', 'Open', 'Open', 'Suspended']),
                    'type': random.choice(['Credit', 'Cash'])
                },
                'settle_terms': random.choice(['30 days', '60 days', '90 days', 'COD']),
                'normal_payterms': random.choice([0, 30, 60, 90]),
                'discount': random.choice([0, 0, 5, 10, 15]),
                'credit_limit': random.randint(10000, 200000),
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
        
        # Insert into database
        self.db.customers.insert_many(customers)
        print(f"   ✅ Created {count} sample customers")
        
        return customers
    
    def _create_sample_products(self, count: int = 30) -> List[Dict]:
        """Create sample product documents in correct format"""
        products = []
        
        for i in range(count):
            product_id = f"PROD{2000 + i}"
            inventory_code = f"INV{5000 + i}"
            brand = random.choice(PRODUCT_BRANDS)
            category = random.choice(PRODUCT_CATEGORIES)
            range_name = random.choice(PRODUCT_RANGES)
            
            product = {
                '_id': product_id,
                'brand': {
                    'id': f"BR{random.randint(1, 15):02d}",
                    'desc': brand
                },
                'category': {
                    'id': f"CAT{random.randint(20, 40):02d}",
                    'desc': category
                },
                'range': {
                    'id': f"RNG{random.randint(1, 10):02d}",
                    'desc': range_name
                },
                'style': {
                    'gender': random.choice(['Male', 'Female', 'Unisex']),
                    'material': random.choice(['Leather', 'Synthetic', 'Cotton', 'Mesh', 'Canvas']),
                    'style': random.choice(['Casual', 'Sport', 'Streetwear', 'Professional', 'Athletic']),
                    'colour': random.choice(['Black', 'White', 'Blue', 'Red', 'Grey', 'Navy', 'Green']),
                    'branding': brand,
                    'qual_probs': None
                },
                'inventory_code': inventory_code,
                'last_cost': round(random.uniform(150, 2500), 2),
                'stock_ind': random.choice([True, True, True, False]),  # 75% in stock
                '_created_by': 'simulator',
                '_created_at': datetime.now()
            }
            products.append(product)
        
        # Insert into database
        self.db.products.insert_many(products)
        print(f"   ✅ Created {count} sample products")
        
        return products
    
    def _create_sample_suppliers(self, count: int = 10) -> List[Dict]:
        """Create sample supplier documents in correct format"""
        suppliers = []
        
        for i in range(count):
            supplier_id = f"SUPP{100 + i}"
            
            supplier = {
                '_id': supplier_id,
                'description': fake.company() + random.choice([' Ltd', ' (Pty) Ltd', ' Inc', ' International']),
                'exclusive': random.choice([True, False, False, False]),  # 25% exclusive
                'normal_payterms': random.choice([30, 60, 90]),
                'credit_limit': random.randint(50000, 1000000),
                '_created_by': 'simulator',
                '_created_at': datetime.now()
            }
            suppliers.append(supplier)
        
        # Insert into database
        self.db.suppliers.insert_many(suppliers)
        print(f"   ✅ Created {count} sample suppliers")
        
        return suppliers
    
    def _calculate_financial_period(self, date_obj: Optional[datetime] = None) -> int:
        """Calculate financial period in YYYYMM format"""
        if date_obj is None:
            date_obj = datetime.now()
        return int(f"{date_obj.year}{date_obj.month:02d}")
    
    def generate_sales_transaction(self) -> Dict:
        """Generate realistic sales transaction with embedded lines"""
        
        if not self.sample_customers or not self.sample_products:
            raise ValueError("No reference data available")
        
        customer = random.choice(self.sample_customers)
        trans_date = datetime.now() - timedelta(days=random.randint(0, 7))
        fin_period = self._calculate_financial_period(trans_date)
        doc_number = f"SALE{int(time.time() * 1000) % 1000000}"
        
        # Generate 1-5 sales line items
        lines = []
        num_lines = random.randint(1, 5)
        
        for _ in range(num_lines):
            product = random.choice(self.sample_products)
            quantity = random.randint(1, 20)
            
            # Calculate price with markup (30% - 100% above cost)
            base_cost = product.get('last_cost', 500)
            markup = random.uniform(1.3, 2.0)
            unit_price = base_cost * markup
            total = quantity * unit_price
            
            line = {
                'product_id': product['_id'],
                'quantity': quantity,
                'unit_sell_price': round(unit_price, 2),
                'total_line_cost': round(total, 2),
                'last_cost': base_cost
            }
            lines.append(line)
        
        # Create sales document (matches your collection structure exactly)
        sales_doc = {
            '_id': doc_number,
            'customer_id': customer['_id'],
            'rep': {
                'id': f"REP{random.randint(1, 25):02d}",
                'desc': fake.name(),
                'commission': random.choice([5, 10, 15])
            },
            'trans_type': random.choice(['Tax Invoice', 'Tax Invoice', 'Tax Invoice', 'Cash Sale', 'Credit Note']),
            'trans_date': trans_date,
            'fin_period': fin_period,
            'lines': lines,
            '_source': 'simulator',
            '_created_at': datetime.now()
        }
        
        return sales_doc
    
    def generate_payment_transaction(self) -> Dict:
        """Generate realistic payment transaction with embedded lines"""
        
        if not self.sample_customers:
            raise ValueError("No customers available")
        
        customer = random.choice(self.sample_customers)
        deposit_date = datetime.now() - timedelta(days=random.randint(0, 3))
        fin_period = self._calculate_financial_period(deposit_date)
        deposit_ref = f"DEP{int(time.time() * 1000) % 1000000}"
        payment_id = f"PAYM{int(time.time() * 1000) % 1000000}"
        
        # Generate 1-3 payment lines
        lines = []
        num_lines = random.randint(1, 3)
        
        for _ in range(num_lines):
            bank_amt = round(random.uniform(1000, 100000), 2)
            discount_pct = customer.get('discount', 0)
            discount = round(bank_amt * discount_pct / 100, 2)
            tot_payment = bank_amt - discount
            
            line = {
                'fin_period': fin_period,
                'deposit_date': deposit_date,
                'bank_amt': bank_amt,
                'discount': discount,
                'tot_payment': tot_payment
            }
            lines.append(line)
        
        # Create payment document
        payment_doc = {
            '_id': payment_id,
            'customer_id': customer['_id'],
            'deposit_ref': deposit_ref,
            'lines': lines,
            '_source': 'simulator',
            '_created_at': datetime.now()
        }
        
        return payment_doc
    
    def generate_purchase_transaction(self) -> Dict:
        """Generate realistic purchase order with embedded lines"""
        
        if not self.sample_suppliers or not self.sample_products:
            raise ValueError("No reference data available")
        
        supplier = random.choice(self.sample_suppliers)
        purch_date = datetime.now() - timedelta(days=random.randint(0, 14))
        purch_doc_no = f"PO{int(time.time() * 1000) % 1000000}"
        
        # Generate 1-5 purchase lines
        lines = []
        num_lines = random.randint(1, 5)
        
        for _ in range(num_lines):
            product = random.choice(self.sample_products)
            quantity = random.randint(10, 500)
            
            # Cost with some variance
            unit_cost = product.get('last_cost', 500) * random.uniform(0.95, 1.05)
            total_cost = quantity * unit_cost
            
            line = {
                'product_id': product['_id'],
                'quantity': quantity,
                'unit_cost_price': round(unit_cost, 2),
                'total_line_cost': round(total_cost, 2)
            }
            lines.append(line)
        
        # Create purchase document
        purchase_doc = {
            '_id': purch_doc_no,
            'supplier_id': supplier['_id'],
            'purch_date': purch_date,
            'lines': lines,
            '_source': 'simulator',
            '_created_at': datetime.now()
        }
        
        return purchase_doc
    
    def update_customer(self) -> Dict:
        """Update existing customer (master data change)"""
        
        if not self.sample_customers:
            raise ValueError("No customers available")
        
        customer = random.choice(self.sample_customers)
        update_type = random.choice(['credit_limit', 'age_analysis', 'discount', 'status'])
        
        if update_type == 'credit_limit':
            new_limit = random.randint(10000, 250000)
            self.db.customers.update_one(
                {'_id': customer['_id']},
                {
                    '$set': {
                        'credit_limit': new_limit,
                        '_updated_at': datetime.now(),
                        '_updated_by': 'simulator'
                    }
                }
            )
            return {
                'type': 'customer_update',
                'customer_id': customer['_id'],
                'field': 'credit_limit',
                'value': new_limit
            }
        
        elif update_type == 'age_analysis':
            # Simulate aging of receivables
            total_due = random.randint(5000, 80000)
            current = round(total_due * random.uniform(0.4, 0.7), 2)
            amt_30 = round(total_due * random.uniform(0.1, 0.3), 2)
            amt_60 = round(total_due * random.uniform(0.05, 0.15), 2)
            amt_90 = round(total_due - current - amt_30 - amt_60, 2)
            
            self.db.customers.update_one(
                {'_id': customer['_id']},
                {
                    '$set': {
                        'age_analysis.total_due': total_due,
                        'age_analysis.amt_current': current,
                        'age_analysis.amt_30_days': amt_30,
                        'age_analysis.amt_60_days': amt_60,
                        'age_analysis.amt_90_days': max(0, amt_90),
                        '_updated_at': datetime.now(),
                        '_updated_by': 'simulator'
                    }
                }
            )
            return {
                'type': 'customer_update',
                'customer_id': customer['_id'],
                'field': 'age_analysis',
                'total_due': total_due
            }
        
        elif update_type == 'discount':
            new_discount = random.choice([0, 5, 10, 15, 20])
            self.db.customers.update_one(
                {'_id': customer['_id']},
                {
                    '$set': {
                        'discount': new_discount,
                        '_updated_at': datetime.now(),
                        '_updated_by': 'simulator'
                    }
                }
            )
            return {
                'type': 'customer_update',
                'customer_id': customer['_id'],
                'field': 'discount',
                'value': new_discount
            }
        
        else:  # status
            new_status = random.choice(['Open', 'Suspended'])
            self.db.customers.update_one(
                {'_id': customer['_id']},
                {
                    '$set': {
                        'account.status': new_status,
                        '_updated_at': datetime.now(),
                        '_updated_by': 'simulator'
                    }
                }
            )
            return {
                'type': 'customer_update',
                'customer_id': customer['_id'],
                'field': 'account_status',
                'value': new_status
            }
    
    def update_product(self) -> Dict:
        """Update existing product (master data change)"""
        
        if not self.sample_products:
            raise ValueError("No products available")
        
        product = random.choice(self.sample_products)
        update_type = random.choice(['cost', 'stock', 'cost'])  # Cost changes more often
        
        if update_type == 'cost':
            # Simulate cost fluctuation
            old_cost = product.get('last_cost', 500)
            new_cost = round(old_cost * random.uniform(0.90, 1.15), 2)
            
            self.db.products.update_one(
                {'_id': product['_id']},
                {
                    '$set': {
                        'last_cost': new_cost,
                        '_updated_at': datetime.now(),
                        '_updated_by': 'simulator'
                    }
                }
            )
            return {
                'type': 'product_update',
                'product_id': product['_id'],
                'field': 'last_cost',
                'old_value': old_cost,
                'new_value': new_cost
            }
        
        else:  # stock
            new_stock = random.choice([True, False])
            self.db.products.update_one(
                {'_id': product['_id']},
                {
                    '$set': {
                        'stock_ind': new_stock,
                        '_updated_at': datetime.now(),
                        '_updated_by': 'simulator'
                    }
                }
            )
            return {
                'type': 'product_update',
                'product_id': product['_id'],
                'field': 'stock_ind',
                'value': new_stock
            }
    
    def update_supplier(self) -> Dict:
        """Update existing supplier (master data change)"""
        
        if not self.sample_suppliers:
            raise ValueError("No suppliers available")
        
        supplier = random.choice(self.sample_suppliers)
        update_type = random.choice(['credit_limit', 'payment_terms'])
        
        if update_type == 'credit_limit':
            new_limit = random.randint(50000, 1500000)
            self.db.suppliers.update_one(
                {'_id': supplier['_id']},
                {
                    '$set': {
                        'credit_limit': new_limit,
                        '_updated_at': datetime.now(),
                        '_updated_by': 'simulator'
                    }
                }
            )
            return {
                'type': 'supplier_update',
                'supplier_id': supplier['_id'],
                'field': 'credit_limit',
                'value': new_limit
            }
        
        else:  # payment_terms
            new_terms = random.choice([30, 60, 90])
            self.db.suppliers.update_one(
                {'_id': supplier['_id']},
                {
                    '$set': {
                        'normal_payterms': new_terms,
                        '_updated_at': datetime.now(),
                        '_updated_by': 'simulator'
                    }
                }
            )
            return {
                'type': 'supplier_update',
                'supplier_id': supplier['_id'],
                'field': 'payment_terms',
                'value': new_terms
            }
    
    def insert_transaction(self, transaction_type: str = 'mixed') -> bool:
        """
        Generate and insert a transaction into MongoDB
        
        Args:
            transaction_type: Type of transaction to generate
                - 'mixed': Random mix of all types
                - 'sales', 'payments', 'purchases': Specific transaction types
                - 'customer_update', 'product_update', 'supplier_update': Master data updates
        
        Returns:
            bool: True if successful
        """
        try:
            # Determine what to generate based on type and probabilities
            if transaction_type == 'sales' or (transaction_type == 'mixed' and random.random() < 0.35):
                # SALES TRANSACTION (35% probability in mixed mode)
                sales_doc = self.generate_sales_transaction()
                self.db.sales.insert_one(sales_doc)
                
                total = sum(line['total_line_cost'] for line in sales_doc['lines'])
                print(f"🛒 SALES: {sales_doc['_id']} | Customer: {sales_doc['customer_id']} | "
                      f"R{total:,.2f} | {len(sales_doc['lines'])} items")
                
                self.transaction_count += 1
                return True
            
            elif transaction_type == 'payments' or (transaction_type == 'mixed' and random.random() < 0.30):
                # PAYMENT TRANSACTION (30% probability in mixed mode)
                payment_doc = self.generate_payment_transaction()
                self.db.payments.insert_one(payment_doc)
                
                total = sum(line['tot_payment'] for line in payment_doc['lines'])
                print(f"💳 PAYMENT: {payment_doc['deposit_ref']} | Customer: {payment_doc['customer_id']} | "
                      f"R{total:,.2f}")
                
                self.transaction_count += 1
                return True
            
            elif transaction_type == 'purchases' or (transaction_type == 'mixed' and random.random() < 0.20):
                # PURCHASE ORDER (20% probability in mixed mode)
                purchase_doc = self.generate_purchase_transaction()
                self.db.purchases.insert_one(purchase_doc)
                
                total = sum(line['total_line_cost'] for line in purchase_doc['lines'])
                print(f"📦 PURCHASE: {purchase_doc['_id']} | Supplier: {purchase_doc['supplier_id']} | "
                      f"R{total:,.2f} | {len(purchase_doc['lines'])} items")
                
                self.transaction_count += 1
                return True
            
            elif transaction_type == 'customer_update' or (transaction_type == 'mixed' and random.random() < 0.08):
                # CUSTOMER UPDATE (8% probability in mixed mode)
                result = self.update_customer()
                print(f"👤 CUSTOMER: {result['customer_id']} | Updated: {result['field']}")
                
                self.transaction_count += 1
                return True
            
            elif transaction_type == 'product_update' or (transaction_type == 'mixed' and random.random() < 0.05):
                # PRODUCT UPDATE (5% probability in mixed mode)
                result = self.update_product()
                print(f"📦 PRODUCT: {result['product_id']} | Updated: {result['field']}")
                
                self.transaction_count += 1
                return True
            
            elif transaction_type == 'supplier_update' or (transaction_type == 'mixed' and random.random() < 0.02):
                # SUPPLIER UPDATE (2% probability in mixed mode)
                result = self.update_supplier()
                print(f"🏭 SUPPLIER: {result['supplier_id']} | Updated: {result['field']}")
                
                self.transaction_count += 1
                return True
            
            return False
        
        except PyMongoError as e:
            print(f"❌ MongoDB Error: {e}")
            return False
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def run_burst(self, count: int = 20, delay_range: tuple = (0.5, 2.0), transaction_type: str = 'mixed'):
        """
        Run a burst of transactions
        
        Args:
            count: Number of transactions to generate
            delay_range: Min and max delay between transactions (seconds)
            transaction_type: Type of transactions to generate
        """
        print(f"\n💥 Starting burst simulation...")
        print(f"   Transactions: {count}")
        print(f"   Type: {transaction_type}")
        print(f"   Delay: {delay_range[0]}-{delay_range[1]}s")
        print("-" * 70)
        
        successful = 0
        start_time = time.time()
        
        for i in range(count):
            if self.insert_transaction(transaction_type):
                successful += 1
            
            # Add delay between transactions (except for last one)
            if i < count - 1:
                delay = random.uniform(delay_range[0], delay_range[1])
                time.sleep(delay)
        
        elapsed = time.time() - start_time
        
        print("-" * 70)
        print(f"✅ Burst complete!")
        print(f"   Successful: {successful}/{count}")
        print(f"   Time: {elapsed:.1f}s")
        print(f"   Rate: {successful/elapsed:.2f} tx/s\n")
    
    def run_continuous(self, transactions_per_minute: int = 10, transaction_type: str = 'mixed'):
        """
        Run continuous transaction simulation
        
        Args:
            transactions_per_minute: Target transaction rate
            transaction_type: Type of transactions to generate
        """
        if self.is_running:
            print("⚠️  Simulation already running")
            return
        
        self.is_running = True
        interval = 60 / transactions_per_minute
        
        print(f"\n🚀 Starting continuous simulation...")
        print(f"   Rate: {transactions_per_minute} transactions/minute")
        print(f"   Type: {transaction_type}")
        print(f"   Interval: ~{interval:.1f}s between transactions")
        print("   Press Ctrl+C to stop")
        print("=" * 70 + "\n")
        
        start_time = time.time()
        
        def simulation_loop():
            """Background simulation thread"""
            while self.is_running:
                try:
                    self.insert_transaction(transaction_type)
                    
                    # Print periodic summary
                    if self.transaction_count % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = self.transaction_count / elapsed if elapsed > 0 else 0
                        print(f"\n📊 Progress: {self.transaction_count} transactions | "
                              f"Rate: {rate:.2f} tx/s | Uptime: {elapsed:.0f}s\n")
                    
                    time.sleep(interval)
                
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(f"❌ Error in simulation loop: {e}")
                    time.sleep(1)
        
        try:
            thread = threading.Thread(target=simulation_loop, daemon=True)
            thread.start()
            thread.join()
        
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping simulation...")
            self.is_running = False
            time.sleep(1)
    
    def run_stress_test(self, duration_seconds: int = 60):
        """
        Run high-volume stress test
        
        Args:
            duration_seconds: How long to run the test
        """
        print(f"\n⚡ STRESS TEST MODE")
        print(f"   Duration: {duration_seconds}s")
        print(f"   Target: Maximum throughput")
        print("=" * 70 + "\n")
        
        self.is_running = True
        start_time = time.time()
        
        while self.is_running and (time.time() - start_time) < duration_seconds:
            try:
                self.insert_transaction('mixed')
                
                # Brief status update every 5 seconds
                if self.transaction_count % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = self.transaction_count / elapsed
                    print(f"⚡ {self.transaction_count} tx | {rate:.1f} tx/s")
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ Error: {e}")
        
        self.is_running = False
        elapsed = time.time() - start_time
        
        print("\n" + "=" * 70)
        print("⚡ STRESS TEST COMPLETE")
        print("=" * 70)
        print(f"Total Transactions: {self.transaction_count}")
        print(f"Duration: {elapsed:.1f}s")
        print(f"Average Rate: {self.transaction_count/elapsed:.2f} tx/s")
        print("=" * 70 + "\n")
    
    def get_statistics(self):
        """Print current database statistics"""
        print("\n" + "=" * 70)
        print("📊 DATABASE STATISTICS")
        print("=" * 70)
        
        try:
            print(f"Customers:  {self.db.customers.count_documents({}):,}")
            print(f"Products:   {self.db.products.count_documents({}):,}")
            print(f"Suppliers:  {self.db.suppliers.count_documents({}):,}")
            print(f"Sales:      {self.db.sales.count_documents({}):,}")
            print(f"Payments:   {self.db.payments.count_documents({}):,}")
            print(f"Purchases:  {self.db.purchases.count_documents({}):,}")
        except Exception as e:
            print(f"❌ Error getting statistics: {e}")
        
        print("=" * 70 + "\n")
    
    def stop(self):
        """Stop simulation"""
        self.is_running = False
        print(f"\n✅ Simulation stopped")
        print(f"   Total transactions generated: {self.transaction_count}")
    
    def close(self):
        """Close database connection"""
        self.client.close()
        print("✅ Database connection closed")


def main():
    """Main entry point for the simulator"""
    
    print("\n" + "=" * 70)
    print("🎲 CLEARVUE TRANSACTION SIMULATOR")
    print("   Generate Realistic Test Data for Streaming Pipeline")
    print("=" * 70)
    
    print("\n📋 SIMULATION MODES:")
    print("=" * 70)
    print("1. Quick burst (20 mixed transactions)")
    print("2. Continuous low-volume (10 tx/min - for testing)")
    print("3. Continuous medium-volume (30 tx/min - realistic)")
    print("4. Continuous high-volume (60 tx/min - busy period)")
    print("5. Stress test (maximum throughput, 60s)")
    print("6. Sales only (20 transactions)")
    print("7. Payments only (20 transactions)")
    print("8. Master data updates only (30 updates)")
    print("9. Custom configuration")
    print("10. View database statistics")
    print("=" * 70)
    
    choice = input("\n👉 Select mode (1-10) [default: 1]: ").strip() or "1"
    
    simulator = None
    
    try:
        # Initialize simulator
        print("\n🔧 Initializing simulator...")
        simulator = ClearVueTransactionSimulator()
        
        # Execute based on choice
        if choice == "1":
            # Quick burst
            simulator.run_burst(20, delay_range=(0.5, 1.5), transaction_type='mixed')
        
        elif choice == "2":
            # Low volume continuous
            print("\n📊 Low-volume mode - ideal for testing change streams")
            simulator.run_continuous(transactions_per_minute=10, transaction_type='mixed')
        
        elif choice == "3":
            # Medium volume continuous
            print("\n📊 Medium-volume mode - realistic business day")
            simulator.run_continuous(transactions_per_minute=30, transaction_type='mixed')
        
        elif choice == "4":
            # High volume continuous
            print("\n⚡ High-volume mode - peak business hours")
            simulator.run_continuous(transactions_per_minute=60, transaction_type='mixed')
        
        elif choice == "5":
            # Stress test
            duration = input("Duration in seconds [60]: ").strip() or "60"
            simulator.run_stress_test(duration_seconds=int(duration))
        
        elif choice == "6":
            # Sales only
            print("\n🛒 Sales transactions only")
            count = input("Number of sales [20]: ").strip() or "20"
            simulator.run_burst(int(count), delay_range=(0.5, 1.5), transaction_type='sales')
        
        elif choice == "7":
            # Payments only
            print("\n💳 Payment transactions only")
            count = input("Number of payments [20]: ").strip() or "20"
            simulator.run_burst(int(count), delay_range=(0.5, 1.5), transaction_type='payments')
        
        elif choice == "8":
            # Master data updates
            print("\n📚 Master data updates")
            print("   - Customer updates")
            simulator.run_burst(10, delay_range=(0.3, 0.8), transaction_type='customer_update')
            print("\n   - Product updates")
            simulator.run_burst(15, delay_range=(0.3, 0.8), transaction_type='product_update')
            print("\n   - Supplier updates")
            simulator.run_burst(5, delay_range=(0.3, 0.8), transaction_type='supplier_update')
        
        elif choice == "9":
            # Custom configuration
            print("\n⚙️  Custom Configuration")
            print("Transaction types: mixed, sales, payments, purchases,")
            print("                   customer_update, product_update, supplier_update")
            
            tx_type = input("Transaction type [mixed]: ").strip() or "mixed"
            count = input("Number of transactions [50]: ").strip() or "50"
            min_delay = input("Minimum delay (seconds) [0.5]: ").strip() or "0.5"
            max_delay = input("Maximum delay (seconds) [2.0]: ").strip() or "2.0"
            
            simulator.run_burst(
                int(count),
                delay_range=(float(min_delay), float(max_delay)),
                transaction_type=tx_type
            )
        
        elif choice == "10":
            # Statistics
            simulator.get_statistics()
        
        else:
            print("❌ Invalid option")
        
        # Final summary
        if choice != "10":
            print("\n" + "=" * 70)
            print("✅ SIMULATION COMPLETE")
            print("=" * 70)
            print(f"Total transactions generated: {simulator.transaction_count}")
            simulator.get_statistics()
    
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user")
        if simulator:
            simulator.stop()
    
    except Exception as e:
        print(f"\n❌ Simulation failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if simulator:
            simulator.close()
        print("\n👋 Goodbye!\n")


if __name__ == "__main__":
    main()