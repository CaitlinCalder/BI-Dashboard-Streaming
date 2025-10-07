"""
ClearVue MongoDB Atlas Data Loader
Loads cleaned data from local files to MongoDB Atlas
Replaces the dummy data generator with real ClearVue data

Author: ClearVue Implementation Team
Date: October 2025
"""

import json
import csv
import logging
from datetime import datetime
from pymongo import MongoClient
from typing import Dict, List, Any
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('MongoDBAtlasLoader')

class ClearVueAtlasLoader:
    def __init__(self, 
                 atlas_uri='mongodb+srv://Tyra:1234@novacluster.1re1a4e.mongodb.net/?authSource=admin&retryWrites=true&w=majority',
                 database_name='Nova_Analytics',
                 data_directory='./cleaned_data'):
        """
        Initialize connection to MongoDB Atlas
        
        Args:
            atlas_uri: MongoDB Atlas connection string
            database_name: Target database name
            data_directory: Directory containing cleaned CSV/JSON files
        """
        try:
            print("🔌 Connecting to MongoDB Atlas...")
            self.client = MongoClient(atlas_uri, serverSelectionTimeoutMS=5000)
            
            # Test connection
            self.client.admin.command('ping')
            print("✅ Successfully connected to MongoDB Atlas!")
            
            self.db = self.client[database_name]
            self.data_dir = data_directory
            
            # Collection mapping - maps file names to collection names
            self.collection_mapping = {
                'Trans_Types': 'trans_types',
                'Suppliers': 'suppliers',
                'Representatives': 'representatives',
                'Purchases_Headers': 'purchases_headers',
                'Product_Ranges': 'product_ranges',
                'Product_Categories': 'product_categories',
                'Purchases_Lines': 'purchases_lines',
                'Products_Styles': 'products_styles',
                'Products': 'products',
                'Sales_Header': 'sales_headers',
                'Product_Brands': 'product_brands',
                'Customer_Regions': 'customer_regions',
                'Customer_Categories': 'customer_categories',
                'Payment_Header': 'payment_headers',
                'Payment_Lines': 'payment_lines',
                'Customer': 'customers',
                'Age_Analysis': 'age_analysis'
            }
            
            logger.info(f"Connected to database: {database_name}")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB Atlas: {e}")
            raise
    
    def load_csv_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Load data from CSV file"""
        try:
            documents = []
            with open(file_path, 'r', encoding='utf-8') as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    # Clean up the row - remove empty strings and convert types
                    cleaned_row = {}
                    for key, value in row.items():
                        if value and value.strip():  # Only add non-empty values
                            # Try to convert to appropriate type
                            cleaned_value = value.strip()
                            
                            # Try numeric conversion
                            try:
                                if '.' in cleaned_value:
                                    cleaned_row[key] = float(cleaned_value)
                                else:
                                    cleaned_row[key] = int(cleaned_value)
                            except ValueError:
                                # Keep as string
                                cleaned_row[key] = cleaned_value
                        else:
                            cleaned_row[key] = None
                    
                    # Add metadata
                    cleaned_row['_loaded_at'] = datetime.now()
                    documents.append(cleaned_row)
            
            logger.info(f"Loaded {len(documents)} documents from {file_path}")
            return documents
            
        except Exception as e:
            logger.error(f"Error loading CSV file {file_path}: {e}")
            return []
    
    def load_json_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Load data from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Handle both array and single object
                if isinstance(data, list):
                    documents = data
                else:
                    documents = [data]
                
                # Add metadata
                for doc in documents:
                    doc['_loaded_at'] = datetime.now()
                
                logger.info(f"Loaded {len(documents)} documents from {file_path}")
                return documents
                
        except Exception as e:
            logger.error(f"Error loading JSON file {file_path}: {e}")
            return []
    
    def load_collection(self, table_name: str, collection_name: str) -> bool:
        """
        Load a single collection from file
        
        Args:
            table_name: Original table name (e.g., 'Trans_Types')
            collection_name: MongoDB collection name (e.g., 'trans_types')
        """
        try:
            print(f"\n📦 Loading {table_name} → {collection_name}...")
            
            # Try to find the file (CSV or JSON)
            csv_path = os.path.join(self.data_dir, f"{table_name}.csv")
            json_path = os.path.join(self.data_dir, f"{table_name}.json")
            
            documents = []
            
            if os.path.exists(csv_path):
                documents = self.load_csv_file(csv_path)
            elif os.path.exists(json_path):
                documents = self.load_json_file(json_path)
            else:
                logger.warning(f"No file found for {table_name}")
                print(f"⚠️  File not found: {table_name}.csv or {table_name}.json")
                return False
            
            if not documents:
                print(f"⚠️  No documents to load for {table_name}")
                return False
            
            # Clear existing data
            result = self.db[collection_name].delete_many({})
            if result.deleted_count > 0:
                print(f"   🗑️  Cleared {result.deleted_count} existing documents")
            
            # Insert new data
            result = self.db[collection_name].insert_many(documents)
            print(f"   ✅ Inserted {len(result.inserted_ids)} documents")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading collection {collection_name}: {e}")
            print(f"   ❌ Error: {e}")
            return False
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        print("\n🔍 Creating indexes...")
        
        try:
            # Trans_Types indexes
            self.db.trans_types.create_index("TRANSTYPE_CODE", unique=True)
            
            # Suppliers indexes
            self.db.suppliers.create_index("SUPPLIER_CODE", unique=True)
            
            # Representatives indexes
            self.db.representatives.create_index("REP_CODE", unique=True)
            
            # Product indexes
            self.db.products.create_index("INVENTORY_CODE", unique=True)
            self.db.products.create_index("PRODCAT_CODE")
            
            # Product Categories indexes
            self.db.product_categories.create_index("PRODCAT_CODE", unique=True)
            self.db.product_categories.create_index("BRAND_CODE")
            self.db.product_categories.create_index("PRAN_CODE")
            
            # Product Brands indexes
            self.db.product_brands.create_index("PRODBRA_CODE", unique=True)
            
            # Product Ranges indexes
            self.db.product_ranges.create_index("PRAN_CODE", unique=True)
            
            # Customer indexes
            self.db.customers.create_index("CUSTOMER_NUMBER", unique=True)
            self.db.customers.create_index("REGION_CODE")
            self.db.customers.create_index("REP_CODE")
            self.db.customers.create_index("CCAT_CODE")
            
            # Customer Regions indexes
            self.db.customer_regions.create_index("REGION_CODE", unique=True)
            
            # Customer Categories indexes
            self.db.customer_categories.create_index("CCAT_CODE", unique=True)
            
            # Sales Header indexes
            self.db.sales_headers.create_index("DOC_NUMBER", unique=True)
            self.db.sales_headers.create_index("CUSTOMER_NUMBER")
            self.db.sales_headers.create_index("TRANS_DATE")
            self.db.sales_headers.create_index("FIN_PERIOD")
            self.db.sales_headers.create_index("REP_CODE")
            
            # Payment Header indexes
            self.db.payment_headers.create_index("CUSTOMER_NUMBER")
            self.db.payment_headers.create_index("DEPOSIT_REF", unique=True)
            
            # Payment Lines indexes (composite)
            self.db.payment_lines.create_index([
                ("CUSTOMER_NUMBER", 1),
                ("DEPOSIT_REF", 1)
            ])
            self.db.payment_lines.create_index("FIN_PERIOD")
            self.db.payment_lines.create_index("DEPOSIT_DATE")
            
            # Purchases Headers indexes
            self.db.purchases_headers.create_index("PURCH_DOC_NO", unique=True)
            self.db.purchases_headers.create_index("SUPPLIER_CODE")
            self.db.purchases_headers.create_index("PURCH_DATE")
            
            # Purchases Lines indexes
            self.db.purchases_lines.create_index("PURCH_DOC_NO")
            self.db.purchases_lines.create_index("INVENTORY_CODE")
            
            # Age Analysis indexes (composite)
            self.db.age_analysis.create_index([
                ("CUSTOMER_NUMBER", 1),
                ("FIN_PERIOD", 1)
            ])
            
            print("   ✅ All indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            print(f"   ⚠️  Some indexes may have failed: {e}")
    
    def validate_data(self):
        """Validate loaded data - check record counts and relationships"""
        print("\n🔎 Validating loaded data...")
        
        validation_results = {}
        
        for table_name, collection_name in self.collection_mapping.items():
            count = self.db[collection_name].count_documents({})
            validation_results[collection_name] = count
            print(f"   {collection_name}: {count:,} documents")
        
        # Check key relationships
        print("\n🔗 Checking relationships...")
        
        # Check if all products have valid categories
        products_count = self.db.products.count_documents({})
        products_with_categories = self.db.products.count_documents({
            "PRODCAT_CODE": {"$exists": True, "$ne": None}
        })
        print(f"   Products with categories: {products_with_categories}/{products_count}")
        
        # Check if all sales have valid customers
        sales_count = self.db.sales_headers.count_documents({})
        sales_with_customers = self.db.sales_headers.count_documents({
            "CUSTOMER_NUMBER": {"$exists": True, "$ne": None}
        })
        print(f"   Sales with customers: {sales_with_customers}/{sales_count}")
        
        return validation_results
    
    def load_all_data(self):
        """Load all collections from data directory"""
        print("="*60)
        print("ClearVue Data Loader - MongoDB Atlas")
        print("="*60)
        
        start_time = datetime.now()
        success_count = 0
        total_count = len(self.collection_mapping)
        
        # Load each collection
        for table_name, collection_name in self.collection_mapping.items():
            if self.load_collection(table_name, collection_name):
                success_count += 1
        
        # Create indexes
        self.create_indexes()
        
        # Validate data
        validation_results = self.validate_data()
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*60)
        print("LOADING COMPLETE")
        print("="*60)
        print(f"✅ Successfully loaded: {success_count}/{total_count} collections")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"📊 Total documents loaded: {sum(validation_results.values()):,}")
        print(f"🗄️  Database: {self.db.name}")
        print("="*60)
        
        return success_count == total_count

    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        print("✅ Connection closed")


if __name__ == "__main__":
    import sys
    
    # Check if data directory is provided
    data_dir = './cleaned_data'
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]
    
    if not os.path.exists(data_dir):
        print(f"❌ Data directory not found: {data_dir}")
        print(f"Usage: python {sys.argv[0]} [data_directory]")
        sys.exit(1)
    
    print(f"📂 Using data directory: {data_dir}\n")
    
    loader = None
    try:
        loader = ClearVueAtlasLoader(data_directory=data_dir)
        success = loader.load_all_data()
        
        if success:
            print("\n🎉 All data loaded successfully!")
            sys.exit(0)
        else:
            print("\n⚠️  Some collections failed to load")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Loading failed: {e}")
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if loader:
            loader.close()