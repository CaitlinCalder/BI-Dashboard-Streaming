"""
ClearVue Dummy Data Generator
Creates realistic sample data matching the 17 files mentioned in the RFP
"""
import json
import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from faker import Faker
import uuid

fake = Faker()
fake.seed_instance(42)  # For consistent test data

class ClearVueDummyData:
    def __init__(self):
        # Connect to MongoDB
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['clearvue_bi']
        
        # ClearVue regions (South African focused)
        self.regions = ['Gauteng', 'Western Cape', 'KwaZulu-Natal', 'Eastern Cape', 
                       'Free State', 'Limpopo', 'Mpumalanga', 'Northern Cape', 'North West']
        
        # Product categories for ClearVue
        self.product_brands = ['TechVision', 'ClearTech', 'ViewMaster', 'DataSight', 
                              'InfoVision', 'SmartView', 'TechClear', 'VisionPro']
        
        # Financial year helper (ClearVue's custom calendar)
        self.current_financial_month = self.get_current_financial_month()
    
    def get_current_financial_month(self):
        """Calculate current financial month based on ClearVue's calendar"""
        today = datetime.now()
        # Simplified: assume current month for demo
        return {
            'month': today.month,
            'year': today.year,
            'start_date': today.replace(day=1),
            'end_date': (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        }
    
    def generate_customers(self, count=1000):
        """Generate customer data"""
        customers = []
        for _ in range(count):
            customer = {
                '_id': str(uuid.uuid4()),
                'customer_id': fake.unique.random_int(min=1000, max=99999),
                'company_name': fake.company(),
                'contact_person': fake.name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'address': {
                    'street': fake.street_address(),
                    'city': fake.city(),
                    'region': random.choice(self.regions),
                    'postal_code': fake.postcode(),
                    'country': 'South Africa'
                },
                'registration_date': fake.date_time_between(start_date='-2y', end_date='now'),
                'status': random.choice(['Active', 'Inactive', 'Pending']),
                'credit_limit': round(random.uniform(10000, 500000), 2),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            customers.append(customer)
        return customers
    
    def generate_product_brands(self, count=100):
        """Generate product brands data"""
        products = []
        for _ in range(count):
            product = {
                '_id': str(uuid.uuid4()),
                'product_id': fake.unique.random_int(min=1000, max=99999),
                'brand': random.choice(self.product_brands),
                'category': random.choice(['Hardware', 'Software', 'Services', 'Consulting']),
                'subcategory': random.choice(['Networking', 'Security', 'Analytics', 'Cloud']),
                'product_name': fake.catch_phrase(),
                'unit_price': round(random.uniform(100, 50000), 2),
                'cost_price': round(random.uniform(50, 25000), 2),
                'in_stock': random.randint(0, 1000),
                'supplier': fake.company(),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            products.append(product)
        return products
    
    def generate_payment_headers(self, count=5000):
        """Generate payment header data"""
        payments = []
        customers = list(self.db.customers.find({}, {'customer_id': 1}))
        
        for _ in range(count):
            customer = random.choice(customers) if customers else {'customer_id': random.randint(1000, 9999)}
            
            payment = {
                '_id': str(uuid.uuid4()),
                'payment_id': fake.unique.random_int(min=100000, max=999999),
                'customer_id': customer['customer_id'],
                'payment_date': fake.date_time_between(start_date='-6M', end_date='now'),
                'financial_month': random.randint(1, 12),
                'financial_year': random.choice([2023, 2024, 2025]),
                'region': random.choice(self.regions),
                'payment_method': random.choice(['Credit Card', 'EFT', 'Cash', 'Cheque']),
                'currency': 'ZAR',
                'exchange_rate': 1.0,
                'total_amount': round(random.uniform(1000, 100000), 2),
                'vat_amount': 0.0,  # Will calculate from lines
                'status': random.choice(['Completed', 'Pending', 'Failed']),
                'reference': fake.uuid4()[:8].upper(),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Calculate VAT (15% in SA)
            payment['vat_amount'] = round(payment['total_amount'] * 0.15 / 1.15, 2)
            payments.append(payment)
        
        return payments
    
    def generate_payment_lines(self, payment_headers_count=5000):
        """Generate payment lines for each payment header"""
        lines = []
        payment_headers = list(self.db.payment_headers.find({}, {'payment_id': 1}))
        products = list(self.db.product_brands.find({}, {'product_id': 1, 'unit_price': 1}))
        
        for header in payment_headers[:payment_headers_count]:
            # Each payment has 1-5 line items
            num_lines = random.randint(1, 5)
            
            for line_num in range(1, num_lines + 1):
                product = random.choice(products) if products else {
                    'product_id': random.randint(1000, 9999), 
                    'unit_price': random.uniform(100, 5000)
                }
                
                quantity = random.randint(1, 10)
                unit_price = product['unit_price']
                line_total = round(quantity * unit_price, 2)
                
                line = {
                    '_id': str(uuid.uuid4()),
                    'payment_id': header['payment_id'],
                    'line_number': line_num,
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'discount_percent': round(random.uniform(0, 15), 2),
                    'line_total': line_total,
                    'vat_rate': 15.0,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                lines.append(line)
        
        return lines
    
    def populate_database(self):
        """Populate MongoDB with dummy data"""
        print("🏗️  Populating ClearVue database with dummy data...")
        
        # Clear existing data
        collections = ['customers', 'product_brands', 'payment_headers', 'payment_lines']
        for collection in collections:
            self.db[collection].delete_many({})
            print(f"   Cleared {collection}")
        
        # Generate and insert data
        print("\n📊 Generating customers...")
        customers = self.generate_customers(1000)
        self.db.customers.insert_many(customers)
        print(f"   ✅ Inserted {len(customers)} customers")
        
        print("\n📦 Generating product brands...")
        products = self.generate_product_brands(100)
        self.db.product_brands.insert_many(products)
        print(f"   ✅ Inserted {len(products)} products")
        
        print("\n💳 Generating payment headers...")
        payment_headers = self.generate_payment_headers(2000)
        self.db.payment_headers.insert_many(payment_headers)
        print(f"   ✅ Inserted {len(payment_headers)} payment headers")
        
        print("\n📋 Generating payment lines...")
        payment_lines = self.generate_payment_lines(2000)
        self.db.payment_lines.insert_many(payment_lines)
        print(f"   ✅ Inserted {len(payment_lines)} payment lines")
        
        print(f"\n🎉 Database populated successfully!")
        print(f"   Database: clearvue_bi")
        print(f"   Collections: {collections}")
        
        # Create indexes for better performance
        self.create_indexes()
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        print("\n🔍 Creating indexes...")
        
        # Customer indexes
        self.db.customers.create_index("customer_id", unique=True)
        self.db.customers.create_index("region")
        
        # Product indexes
        self.db.product_brands.create_index("product_id", unique=True)
        self.db.product_brands.create_index("brand")
        self.db.product_brands.create_index("category")
        
        # Payment indexes
        self.db.payment_headers.create_index("payment_id", unique=True)
        self.db.payment_headers.create_index("customer_id")
        self.db.payment_headers.create_index("payment_date")
        self.db.payment_headers.create_index("financial_month")
        self.db.payment_headers.create_index("region")
        
        # Payment lines indexes
        self.db.payment_lines.create_index("payment_id")
        self.db.payment_lines.create_index("product_id")
        
        print("   ✅ Indexes created")

if __name__ == "__main__":
    generator = ClearVueDummyData()
    generator.populate_database()