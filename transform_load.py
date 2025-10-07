"""
ClearVue Data Transformation and Loading Script
Transforms 17 relational CSV files into 6 NoSQL collections

Transformation Logic:
1. Customers: Merges Customer + Customer_Regions + Customer_Categories + Age_Analysis
2. Suppliers: Direct load from Suppliers
3. Products: Merges Products + Product_Brands + Product_Categories + Product_Ranges + Products_Styles
4. Sales: Merges Sales_Header + Representatives + Trans_Types (no line items in source)
5. Payments: Merges Payment_Header + Payment_Lines
6. Purchases: Merges Purchases_Headers + Purchases_Lines

Author: ClearVue MongoDB Team
Date: October 2025
"""

import pandas as pd
import json
import logging
from datetime import datetime
from pymongo import MongoClient
from pathlib import Path
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ClearVueTransform')

class ClearVueDataTransformer:
    """Transform relational data to NoSQL collections"""
    
    def __init__(self, 
                 data_dir='./cleaned_data',
                 mongo_uri='mongodb+srv://Tyra:1234@novacluster.1re1a4e.mongodb.net/?authSource=admin&retryWrites=true&w=majority',
                 database_name='Nova_Analytics'):
        """Initialize transformer"""
        
        print("\n" + "="*70)
        print("CLEARVUE DATA TRANSFORMATION PIPELINE")
        print("17 Relational Tables → 6 NoSQL Collections")
        print("="*70 + "\n")
        
        self.data_dir = Path(data_dir)
        
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
        
        # Connect to MongoDB Atlas
        print("🔌 Connecting to MongoDB Atlas...")
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            print("✅ Connected to MongoDB Atlas!\n")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise
        
        self.db = self.client[database_name]
        
        # Store loaded dataframes
        self.dataframes = {}
        
        # Statistics
        self.stats = {
            'files_loaded': 0,
            'total_rows': 0,
            'collections_created': 0,
            'documents_inserted': 0
        }
    
    def load_csv(self, filename):
        """Load CSV file into pandas DataFrame"""
        filepath = self.data_dir / filename
        
        if not filepath.exists():
            logger.warning(f"File not found: {filename}")
            return None
        
        try:
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                try:
                    df = pd.read_csv(filepath, encoding=encoding)
                    logger.info(f"Loaded {filename}: {len(df)} rows")
                    self.stats['files_loaded'] += 1
                    self.stats['total_rows'] += len(df)
                    return df
                except UnicodeDecodeError:
                    continue
            
            logger.error(f"Failed to read {filename} with any encoding")
            return None
            
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return None
    
    def load_all_source_files(self):
        """Load all 17 source CSV files"""
        print("📂 Loading source files...")
        
        files = [
            'Trans_Types.csv',
            'Suppliers.csv',
            'Representatives.csv',
            'Purchases_Headers.csv',
            'Product_Ranges.csv',
            'Product_Categories.csv',
            'Purchases_Lines.csv',
            'Products_Styles.csv',
            'Products.csv',
            'Sales_Header.csv',
            'Product_Brands.csv',
            'Customer_Regions.csv',
            'Customer_Categories.csv',
            'Payment_Header.csv',
            'Payment_Lines.csv',
            'Customer.csv',
            'Age_Analysis.csv'
        ]
        
        for filename in files:
            name = filename.replace('.csv', '')
            df = self.load_csv(filename)
            if df is not None:
                self.dataframes[name] = df
        
        print(f"\n✅ Loaded {self.stats['files_loaded']}/17 files")
        print(f"📊 Total rows: {self.stats['total_rows']:,}\n")
    
    def transform_customers(self):
        """Transform Customer + related tables → customers collection"""
        print("👥 Transforming CUSTOMERS collection...")
        
        # Required dataframes
        customers_df = self.dataframes.get('Customer')
        regions_df = self.dataframes.get('Customer_Regions')
        categories_df = self.dataframes.get('Customer_Categories')
        age_df = self.dataframes.get('Age_Analysis')
        
        if customers_df is None:
            logger.error("Customer.csv not found")
            return []
        
        # Create lookup dictionaries
        regions_dict = {}
        if regions_df is not None:
            regions_dict = regions_df.set_index('REGION_CODE')[['REGION_DESC']].to_dict('index')
        
        categories_dict = {}
        if categories_df is not None:
            categories_dict = categories_df.set_index('CCAT_CODE')[['CCAT_DESC']].to_dict('index')
        
        age_dict = {}
        if age_df is not None:
            # Get most recent age analysis per customer
            age_df_sorted = age_df.sort_values('FIN_PERIOD', ascending=False)
            age_dict = age_df_sorted.groupby('CUSTOMER_NUMBER').first().to_dict('index')
        
        # Transform each customer
        customers = []
        for _, row in customers_df.iterrows():
            customer_num = row['CUSTOMER_NUMBER']
            
            # Build embedded region
            region_code = row.get('REGION_CODE')
            region = None
            if region_code and region_code in regions_dict:
                region = {
                    'id': region_code,
                    'desc': regions_dict[region_code].get('REGION_DESC')
                }
            
            # Build embedded category
            cat_code = row.get('CCAT_CODE')
            category = None
            if cat_code and cat_code in categories_dict:
                category = {
                    'id': cat_code,
                    'desc': categories_dict[cat_code].get('CCAT_DESC')
                }
            
            # Build embedded age analysis
            age_analysis = None
            if customer_num in age_dict:
                age_data = age_dict[customer_num]
                age_analysis = {
                    'total_due': float(age_data.get('TOTAL_DUE', 0)) if pd.notna(age_data.get('TOTAL_DUE')) else 0,
                    'amt_current': float(age_data.get('AMT_CURRENT', 0)) if pd.notna(age_data.get('AMT_CURRENT')) else 0,
                    'amt_30_days': float(age_data.get('AMT_30_DAYS', 0)) if pd.notna(age_data.get('AMT_30_DAYS')) else 0,
                    'amt_60_days': float(age_data.get('AMT_60_DAYS', 0)) if pd.notna(age_data.get('AMT_60_DAYS')) else 0,
                    'amt_90_days': float(age_data.get('AMT_90_DAYS', 0)) if pd.notna(age_data.get('AMT_90_DAYS')) else 0,
                    'amt_120_days': float(age_data.get('AMT_120_DAYS', 0)) if pd.notna(age_data.get('AMT_120_DAYS')) else 0
                }
            
            customer_doc = {
                '_id': customer_num,
                'name': f"Customer {customer_num}",  # Name not in source, use placeholder
                'region': region,
                'category': category,
                'account': {
                    'status': 'Open',
                    'type': 'Credit'
                },
                'settle_terms': row.get('SETTLE_TERMS'),
                'normal_payterms': int(row.get('NORMAL_PAYTERMS')) if pd.notna(row.get('NORMAL_PAYTERMS')) else None,
                'discount': float(row.get('DISCOUNT', 0)) if pd.notna(row.get('DISCOUNT')) else 0,
                'credit_limit': float(row.get('CREDIT_LIMIT', 0)) if pd.notna(row.get('CREDIT_LIMIT')) else 0,
                'age_analysis': age_analysis,
                'rep_code': row.get('REP_CODE'),
                '_loaded_at': datetime.now()
            }
            
            customers.append(customer_doc)
        
        print(f"   ✅ Transformed {len(customers)} customers")
        return customers
    
    def transform_suppliers(self):
        """Transform Suppliers → suppliers collection"""
        print("🏭 Transforming SUPPLIERS collection...")
        
        suppliers_df = self.dataframes.get('Suppliers')
        if suppliers_df is None:
            logger.error("Suppliers.csv not found")
            return []
        
        suppliers = []
        for _, row in suppliers_df.iterrows():
            supplier_doc = {
                '_id': row['SUPPLIER_CODE'],
                'description': row.get('SUPPLIER_DESC'),
                'exclusive': bool(row.get('EXCLSV')) if pd.notna(row.get('EXCLSV')) else False,
                'normal_payterms': int(row.get('NORMAL_PAYTERMS')) if pd.notna(row.get('NORMAL_PAYTERMS')) else None,
                'credit_limit': float(row.get('CREDIT_LIMIT', 0)) if pd.notna(row.get('CREDIT_LIMIT')) else 0,
                '_loaded_at': datetime.now()
            }
            suppliers.append(supplier_doc)
        
        print(f"   ✅ Transformed {len(suppliers)} suppliers")
        return suppliers
    
    def transform_products(self):
        """Transform Products + related tables → products collection"""
        print("📦 Transforming PRODUCTS collection...")
        
        products_df = self.dataframes.get('Products')
        brands_df = self.dataframes.get('Product_Brands')
        categories_df = self.dataframes.get('Product_Categories')
        ranges_df = self.dataframes.get('Product_Ranges')
        styles_df = self.dataframes.get('Products_Styles')
        
        if products_df is None:
            logger.error("Products.csv not found")
            return []
        
        # Create lookup dictionaries
        brands_dict = {}
        if brands_df is not None:
            brands_dict = brands_df.set_index('PRODBRA_CODE')[['PRODBRA_DESC']].to_dict('index')
        
        categories_dict = {}
        if categories_df is not None:
            # Include brand and range references
            categories_dict = categories_df.set_index('PRODCAT_CODE').to_dict('index')
        
        ranges_dict = {}
        if ranges_df is not None:
            ranges_dict = ranges_df.set_index('PRAN_CODE')[['PRAN_DESC']].to_dict('index')
        
        styles_dict = {}
        if styles_df is not None:
            styles_dict = styles_df.set_index('INVENTORY_CODE').to_dict('index')
        
        # Transform each product
        products = []
        for _, row in products_df.iterrows():
            inventory_code = row['INVENTORY_CODE']
            
            # Get category info
            cat_code = row.get('PRODCAT_CODE')
            brand = None
            category = None
            range_info = None
            
            if cat_code and cat_code in categories_dict:
                cat_data = categories_dict[cat_code]
                
                # Build brand
                brand_code = cat_data.get('BRAND_CODE')
                if brand_code and brand_code in brands_dict:
                    brand = {
                        'id': brand_code,
                        'desc': brands_dict[brand_code].get('PRODBRA_DESC')
                    }
                
                # Build category
                category = {
                    'id': cat_code,
                    'desc': cat_data.get('PRODCAT_DESC')
                }
                
                # Build range
                range_code = cat_data.get('PRAN_CODE')
                if range_code and range_code in ranges_dict:
                    range_info = {
                        'id': range_code,
                        'desc': ranges_dict[range_code].get('PRAN_DESC')
                    }
            
            # Get style info
            style = None
            if inventory_code in styles_dict:
                style_data = styles_dict[inventory_code]
                style = {
                    'gender': style_data.get('GENDER'),
                    'material': style_data.get('MATERIAL'),
                    'style': style_data.get('STYLE'),
                    'colour': style_data.get('COLOUR'),
                    'branding': style_data.get('BRANDING'),
                    'qual_probs': style_data.get('QUAL_PROBS')
                }
            
            product_doc = {
                '_id': f"PROD_{inventory_code}",
                'inventory_code': inventory_code,
                'brand': brand,
                'category': category,
                'range': range_info,
                'style': style,
                'last_cost': float(row.get('LAST_COST', 0)) if pd.notna(row.get('LAST_COST')) else 0,
                'stock_ind': row.get('STOCK_IND') == 'Y' if pd.notna(row.get('STOCK_IND')) else False,
                '_loaded_at': datetime.now()
            }
            
            products.append(product_doc)
        
        print(f"   ✅ Transformed {len(products)} products")
        return products
    
    def transform_sales(self):
        """Transform Sales_Header → sales collection (no line items in source)"""
        print("🛒 Transforming SALES collection...")
        
        sales_df = self.dataframes.get('Sales_Header')
        reps_df = self.dataframes.get('Representatives')
        types_df = self.dataframes.get('Trans_Types')
        
        if sales_df is None:
            logger.error("Sales_Header.csv not found")
            return []
        
        # Create lookup dictionaries
        reps_dict = {}
        if reps_df is not None:
            reps_dict = reps_df.set_index('REP_CODE').to_dict('index')
        
        types_dict = {}
        if types_df is not None:
            types_dict = types_df.set_index('TRANSTYPE_CODE')[['TRANSTYPE_DESC']].to_dict('index')
        
        # Transform each sale
        sales = []
        for _, row in sales_df.iterrows():
            doc_number = row['DOC_NUMBER']
            
            # Build rep info
            rep_code = row.get('REP_CODE')
            rep = None
            if rep_code and rep_code in reps_dict:
                rep_data = reps_dict[rep_code]
                rep = {
                    'id': rep_code,
                    'desc': rep_data.get('REP_DESC'),
                    'commission': float(rep_data.get('COMMISSION', 0)) if pd.notna(rep_data.get('COMMISSION')) else 0
                }
            
            # Get transaction type description
            trans_type_code = row.get('TRANSTYPE_CODE')
            trans_type = trans_type_code
            if trans_type_code and trans_type_code in types_dict:
                trans_type = types_dict[trans_type_code].get('TRANSTYPE_DESC', trans_type_code)
            
            # Parse transaction date
            trans_date = None
            if pd.notna(row.get('TRANS_DATE')):
                try:
                    trans_date = pd.to_datetime(row['TRANS_DATE'])
                except:
                    trans_date = None
            
            sales_doc = {
                '_id': f"SALE_{doc_number}",
                'doc_number': doc_number,
                'customer_id': row.get('CUSTOMER_NUMBER'),
                'rep': rep,
                'trans_type': trans_type,
                'trans_date': trans_date,
                'fin_period': int(row.get('FIN_PERIOD')) if pd.notna(row.get('FIN_PERIOD')) else None,
                'lines': [],  # No line items in source data
                '_loaded_at': datetime.now()
            }
            
            sales.append(sales_doc)
        
        print(f"   ✅ Transformed {len(sales)} sales")
        return sales
    
    def transform_payments(self):
        """Transform Payment_Header + Payment_Lines → payments collection"""
        print("💳 Transforming PAYMENTS collection...")
        
        headers_df = self.dataframes.get('Payment_Header')
        lines_df = self.dataframes.get('Payment_Lines')
        
        if headers_df is None:
            logger.error("Payment_Header.csv not found")
            return []
        
        # Group lines by deposit_ref
        lines_dict = {}
        if lines_df is not None:
            for _, line_row in lines_df.iterrows():
                deposit_ref = line_row.get('DEPOSIT_REF')
                if not deposit_ref:
                    continue
                
                if deposit_ref not in lines_dict:
                    lines_dict[deposit_ref] = []
                
                # Parse deposit date
                deposit_date = None
                if pd.notna(line_row.get('DEPOSIT_DATE')):
                    try:
                        deposit_date = pd.to_datetime(line_row['DEPOSIT_DATE'])
                    except:
                        pass
                
                line = {
                    'fin_period': int(line_row.get('FIN_PERIOD')) if pd.notna(line_row.get('FIN_PERIOD')) else None,
                    'deposit_date': deposit_date,
                    'bank_amt': float(line_row.get('BANK_AMT', 0)) if pd.notna(line_row.get('BANK_AMT')) else 0,
                    'discount': float(line_row.get('DISCOUNT', 0)) if pd.notna(line_row.get('DISCOUNT')) else 0,
                    'tot_payment': float(line_row.get('TOT_PAYMENT', 0)) if pd.notna(line_row.get('TOT_PAYMENT')) else 0
                }
                lines_dict[deposit_ref].append(line)
        
        # Transform each payment
        payments = []
        for _, row in headers_df.iterrows():
            deposit_ref = row.get('DEPOSIT_REF')
            
            payment_doc = {
                '_id': f"PAY_{deposit_ref}" if deposit_ref else f"PAY_{row.get('CUSTOMER_NUMBER')}_{len(payments)}",
                'customer_id': row.get('CUSTOMER_NUMBER'),
                'deposit_ref': deposit_ref,
                'lines': lines_dict.get(deposit_ref, []),
                '_loaded_at': datetime.now()
            }
            
            payments.append(payment_doc)
        
        print(f"   ✅ Transformed {len(payments)} payments")
        return payments
    
    def transform_purchases(self):
        """Transform Purchases_Headers + Purchases_Lines → purchases collection"""
        print("📦 Transforming PURCHASES collection...")
        
        headers_df = self.dataframes.get('Purchases_Headers')
        lines_df = self.dataframes.get('Purchases_Lines')
        
        if headers_df is None:
            logger.error("Purchases_Headers.csv not found")
            return []
        
        # Group lines by purchase doc number
        lines_dict = {}
        if lines_df is not None:
            for _, line_row in lines_df.iterrows():
                purch_doc = line_row.get('PURCH_DOC_NO')
                if not purch_doc:
                    continue
                
                if purch_doc not in lines_dict:
                    lines_dict[purch_doc] = []
                
                line = {
                    'product_id': f"PROD_{line_row.get('INVENTORY_CODE')}" if line_row.get('INVENTORY_CODE') else None,
                    'inventory_code': line_row.get('INVENTORY_CODE'),
                    'quantity': int(line_row.get('QUANTITY')) if pd.notna(line_row.get('QUANTITY')) else 0,
                    'unit_cost_price': float(line_row.get('UNIT_COST_PRICE', 0)) if pd.notna(line_row.get('UNIT_COST_PRICE')) else 0,
                    'total_line_cost': float(line_row.get('TOTAL_LINE_COST', 0)) if pd.notna(line_row.get('TOTAL_LINE_COST')) else 0
                }
                lines_dict[purch_doc].append(line)
        
        # Transform each purchase
        purchases = []
        for _, row in headers_df.iterrows():
            purch_doc = row.get('PURCH_DOC_NO')
            
            # Parse purchase date
            purch_date = None
            if pd.notna(row.get('PURCH_DATE')):
                try:
                    purch_date = pd.to_datetime(row['PURCH_DATE'])
                except:
                    pass
            
            purchase_doc = {
                '_id': f"PURCH_{purch_doc}" if purch_doc else f"PURCH_{len(purchases)}",
                'purch_doc_no': purch_doc,
                'supplier_id': row.get('SUPPLIER_CODE'),
                'purch_date': purch_date,
                'lines': lines_dict.get(purch_doc, []),
                '_loaded_at': datetime.now()
            }
            
            purchases.append(purchase_doc)
        
        print(f"   ✅ Transformed {len(purchases)} purchases")
        return purchases
    
    def load_collection(self, collection_name, documents):
        """Load documents into MongoDB collection"""
        if not documents:
            logger.warning(f"No documents to load for {collection_name}")
            return
        
        print(f"\n📤 Loading {collection_name} collection...")
        
        # Clear existing data
        result = self.db[collection_name].delete_many({})
        if result.deleted_count > 0:
            print(f"   🗑️  Deleted {result.deleted_count} existing documents")
        
        # Insert new documents
        try:
            result = self.db[collection_name].insert_many(documents, ordered=False)
            print(f"   ✅ Inserted {len(result.inserted_ids)} documents")
            self.stats['collections_created'] += 1
            self.stats['documents_inserted'] += len(result.inserted_ids)
        except Exception as e:
            logger.error(f"Error loading {collection_name}: {e}")
    
    def create_indexes(self):
        """Create indexes on collections"""
        print("\n🔍 Creating indexes...")
        
        try:
            # Customers
            self.db.customers.create_index("region.id")
            self.db.customers.create_index("category.id")
            self.db.customers.create_index("rep_code")
            
            # Products
            self.db.products.create_index("inventory_code", unique=True)
            self.db.products.create_index("brand.id")
            self.db.products.create_index("category.id")
            
            # Sales
            self.db.sales.create_index("customer_id")
            self.db.sales.create_index("trans_date")
            self.db.sales.create_index("fin_period")
            self.db.sales.create_index("rep.id")
            
            # Payments
            self.db.payments.create_index("customer_id")
            self.db.payments.create_index("deposit_ref")
            self.db.payments.create_index("lines.deposit_date")
            
            # Purchases
            self.db.purchases.create_index("supplier_id")
            self.db.purchases.create_index("purch_date")
            
            print("   ✅ Indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    def run_transformation(self):
        """Run complete transformation pipeline"""
        print("\n" + "="*70)
        print("STARTING TRANSFORMATION PIPELINE")
        print("="*70 + "\n")
        
        start_time = datetime.now()
        
        # Load source files
        self.load_all_source_files()
        
        # Transform and load each collection
        print("\n" + "="*70)
        print("TRANSFORMING DATA")
        print("="*70 + "\n")
        
        customers = self.transform_customers()
        self.load_collection('customers', customers)
        
        suppliers = self.transform_suppliers()
        self.load_collection('suppliers', suppliers)
        
        products = self.transform_products()
        self.load_collection('products', products)
        
        sales = self.transform_sales()
        self.load_collection('sales', sales)
        
        payments = self.transform_payments()
        self.load_collection('payments', payments)
        
        purchases = self.transform_purchases()
        self.load_collection('purchases', purchases)
        
        # Create indexes
        self.create_indexes()
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("TRANSFORMATION COMPLETE")
        print("="*70)
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"📂 Files loaded: {self.stats['files_loaded']}/17")
        print(f"📊 Total source rows: {self.stats['total_rows']:,}")
        print(f"📦 Collections created: {self.stats['collections_created']}/6")
        print(f"📝 Documents inserted: {self.stats['documents_inserted']:,}")
        print(f"🗄️  Database: {self.db.name}")
        print("="*70 + "\n")
        
        print("✅ Data ready for streaming pipeline!")
        print("   Next: Run clearvue_streaming_pipeline_final.py\n")
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Transform 17 CSVs into 6 MongoDB collections')
    parser.add_argument('data_dir', nargs='?', default='./cleaned_data',
                       help='Directory containing the 17 CSV files')
    
    args = parser.parse_args()
    
    transformer = None
    try:
        transformer = ClearVueDataTransformer(data_dir=args.data_dir)
        transformer.run_transformation()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"\n❌ Transformation failed: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if transformer:
            transformer.close()


if __name__ == "__main__":
    main()