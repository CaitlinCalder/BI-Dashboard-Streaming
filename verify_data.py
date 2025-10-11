"""
ClearVue MongoDB Data Verification Script
Verify data is being correctly inserted into MongoDB Atlas

Usage: python verify_data.py
"""

import sys
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import PyMongoError

try:
    from ClearVueConfig import ClearVueConfig
except ImportError:
    print("❌ Error: ClearVueConfig.py not found")
    sys.exit(1)


class MongoDBVerifier:
    """Verify MongoDB Atlas data integrity"""
    
    def __init__(self):
        """Initialize connection to MongoDB Atlas"""
        print("\n" + "="*70)
        print("🔍 MONGODB ATLAS DATA VERIFICATION")
        print("="*70 + "\n")
        
        self.mongo_uri = ClearVueConfig.get_mongo_uri()
        self.db_name = ClearVueConfig.get_database_name()
        
        try:
            self.client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=10000
            )
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            print(f"✅ Connected to MongoDB Atlas: {self.db_name}\n")
        except Exception as e:
            print(f"❌ Connection failed: {e}\n")
            sys.exit(1)
    
    def check_collections_exist(self):
        """Verify all required collections exist"""
        print("="*70)
        print("📚 COLLECTION VERIFICATION")
        print("="*70)
        
        existing = set(self.db.list_collection_names())
        required = set(ClearVueConfig.get_all_collections())
        
        print(f"\nFound {len(existing)} collections in database:")
        for coll in sorted(existing):
            print(f"   ✓ {coll}")
        
        missing = required - existing
        if missing:
            print(f"\n⚠️  Missing collections: {', '.join(missing)}")
            print("   These will be created when simulator inserts data.")
        else:
            print("\n✅ All required collections exist!")
        
        return existing
    
    def get_document_counts(self):
        """Get document counts for all collections"""
        print("\n" + "="*70)
        print("📊 DOCUMENT COUNTS")
        print("="*70 + "\n")
        
        collections = ClearVueConfig.get_all_collections()
        counts = {}
        
        total = 0
        for coll in collections:
            try:
                count = self.db[coll].count_documents({})
                counts[coll] = count
                total += count
                
                icon = "✅" if count > 0 else "⚠️ "
                print(f"{icon} {coll:25} : {count:,} documents")
            except Exception as e:
                print(f"❌ {coll:25} : Error - {e}")
                counts[coll] = 0
        
        print(f"\n{'TOTAL':25} : {total:,} documents")
        
        if total == 0:
            print("\n⚠️  No data found! Run the simulator first:")
            print("   python ClearVue_Transaction_Simulator.py")
        
        return counts
    
    def check_recent_inserts(self, minutes=5):
        """Check for recently inserted documents"""
        print("\n" + "="*70)
        print(f"⏰ RECENT INSERTS (Last {minutes} minutes)")
        print("="*70 + "\n")
        
        cutoff = datetime.now() - timedelta(minutes=minutes)
        collections = ClearVueConfig.get_all_collections()
        
        found_recent = False
        
        for coll in collections:
            try:
                # Check for documents created in last N minutes
                count = self.db[coll].count_documents({
                    '_created_at': {'$gte': cutoff}
                })
                
                if count > 0:
                    found_recent = True
                    print(f"✅ {coll:25} : {count} new documents")
                    
                    # Show sample document
                    sample = self.db[coll].find_one(
                        {'_created_at': {'$gte': cutoff}},
                        sort=[('_created_at', -1)]
                    )
                    if sample:
                        created = sample.get('_created_at', 'Unknown')
                        doc_id = sample.get('_id', 'Unknown')
                        print(f"   Latest: {doc_id} at {created}")
            
            except Exception as e:
                print(f"⚠️  {coll:25} : Could not check - {e}")
        
        if not found_recent:
            print(f"⚠️  No documents inserted in last {minutes} minutes")
            print("   Try running the simulator to generate test data.")
    
    def verify_data_structure(self):
        """Verify document structure in each collection"""
        print("\n" + "="*70)
        print("🔍 DATA STRUCTURE VERIFICATION")
        print("="*70 + "\n")
        
        collections = ClearVueConfig.get_all_collections()
        
        for coll in collections:
            try:
                sample = self.db[coll].find_one()
                
                if not sample:
                    print(f"⚠️  {coll}: No documents to verify")
                    continue
                
                print(f"✅ {coll}:")
                
                # Check for required fields based on collection type
                if coll == 'Sales_flat':
                    self._verify_sales(sample)
                elif coll == 'Payments_flat':
                    self._verify_payments(sample)
                elif coll == 'Purchases_flat':
                    self._verify_purchases(sample)
                elif coll == 'Customer_flat_step2':
                    self._verify_customers(sample)
                elif coll == 'Products_flat':
                    self._verify_products(sample)
                elif coll == 'Suppliers':
                    self._verify_suppliers(sample)
                
                print()
            
            except Exception as e:
                print(f"❌ {coll}: Verification error - {e}\n")
    
    def _verify_sales(self, doc):
        """Verify sales document structure"""
        required = ['_id', 'customer_id', 'trans_date', 'lines']
        self._check_fields(doc, required)
        
        if 'lines' in doc and isinstance(doc['lines'], list) and len(doc['lines']) > 0:
            line = doc['lines'][0]
            line_fields = ['product_id', 'quantity', 'unit_sell_price', 'total_line_cost']
            print(f"   Line items: {len(doc['lines'])} (checking first)")
            self._check_fields(line, line_fields, indent=6)
    
    def _verify_payments(self, doc):
        """Verify payment document structure"""
        required = ['_id', 'customer_id', 'deposit_ref', 'lines']
        self._check_fields(doc, required)
        
        if 'lines' in doc and isinstance(doc['lines'], list) and len(doc['lines']) > 0:
            line = doc['lines'][0]
            line_fields = ['deposit_date', 'bank_amt', 'tot_payment']
            print(f"   Payment lines: {len(doc['lines'])} (checking first)")
            self._check_fields(line, line_fields, indent=6)
    
    def _verify_purchases(self, doc):
        """Verify purchase document structure"""
        required = ['_id', 'supplier_id', 'purch_date', 'lines']
        self._check_fields(doc, required)
        
        if 'lines' in doc and isinstance(doc['lines'], list) and len(doc['lines']) > 0:
            line = doc['lines'][0]
            line_fields = ['product_id', 'quantity', 'unit_cost_price', 'total_line_cost']
            print(f"   Purchase lines: {len(doc['lines'])} (checking first)")
            self._check_fields(line, line_fields, indent=6)
    
    def _verify_customers(self, doc):
        """Verify customer document structure"""
        required = ['_id', 'name', 'category', 'region', 'account']
        self._check_fields(doc, required)
    
    def _verify_products(self, doc):
        """Verify product document structure"""
        required = ['_id', 'brand', 'category', 'inventory_code', 'last_cost']
        self._check_fields(doc, required)
    
    def _verify_suppliers(self, doc):
        """Verify supplier document structure"""
        required = ['_id', 'description', 'credit_limit']
        self._check_fields(doc, required)
    
    def _check_fields(self, doc, required_fields, indent=3):
        """Check if required fields exist in document"""
        for field in required_fields:
            if field in doc:
                value = doc[field]
                value_str = str(value)[:50]  # Truncate long values
                print(f"{' '*indent}✓ {field}: {value_str}")
            else:
                print(f"{' '*indent}✗ {field}: MISSING")
    
    def show_sample_documents(self):
        """Show sample documents from each collection"""
        print("\n" + "="*70)
        print("📄 SAMPLE DOCUMENTS")
        print("="*70 + "\n")
        
        collections = ClearVueConfig.get_all_collections()
        
        for coll in collections:
            try:
                sample = self.db[coll].find_one()
                
                if sample:
                    print(f"{'='*70}")
                    print(f"{coll}")
                    print(f"{'='*70}")
                    self._pretty_print_doc(sample)
                    print()
                else:
                    print(f"{coll}: No documents\n")
            
            except Exception as e:
                print(f"{coll}: Error - {e}\n")
    
    def _pretty_print_doc(self, doc, indent=0):
        """Pretty print a document"""
        for key, value in list(doc.items())[:10]:  # Show first 10 fields
            spaces = ' ' * indent
            
            if isinstance(value, dict):
                print(f"{spaces}{key}:")
                self._pretty_print_doc(value, indent + 2)
            elif isinstance(value, list):
                print(f"{spaces}{key}: [{len(value)} items]")
                if len(value) > 0 and isinstance(value[0], dict):
                    print(f"{spaces}  First item:")
                    self._pretty_print_doc(value[0], indent + 4)
            else:
                value_str = str(value)
                if len(value_str) > 60:
                    value_str = value_str[:60] + "..."
                print(f"{spaces}{key}: {value_str}")
        
        if len(doc) > 10:
            print(f"{' ' * indent}... ({len(doc) - 10} more fields)")
    
    def check_simulator_activity(self):
        """Check for simulator-generated data"""
        print("\n" + "="*70)
        print("🎲 SIMULATOR ACTIVITY CHECK")
        print("="*70 + "\n")
        
        collections = ClearVueConfig.get_all_collections()
        
        for coll in collections:
            try:
                # Count documents created by simulator
                sim_count = self.db[coll].count_documents({
                    '_source': 'simulator'
                })
                
                # Count documents created by other sources
                other_count = self.db[coll].count_documents({
                    '_source': {'$ne': 'simulator'}
                })
                
                total = sim_count + other_count
                
                if total > 0:
                    print(f"✅ {coll:25}")
                    print(f"   Simulator data: {sim_count:,}")
                    print(f"   Other data:     {other_count:,}")
                    print(f"   Total:          {total:,}")
                else:
                    print(f"⚠️  {coll:25} : No data")
            
            except Exception as e:
                print(f"❌ {coll:25} : Error - {e}")
    
    def run_full_verification(self):
        """Run all verification checks"""
        try:
            self.check_collections_exist()
            counts = self.get_document_counts()
            
            if sum(counts.values()) > 0:
                self.check_recent_inserts(minutes=5)
                self.verify_data_structure()
                self.check_simulator_activity()
                
                # Ask if user wants to see samples
                print("\n" + "="*70)
                show_samples = input("Show sample documents? (y/n) [n]: ").strip().lower()
                if show_samples == 'y':
                    self.show_sample_documents()
            
            print("\n" + "="*70)
            print("✅ VERIFICATION COMPLETE")
            print("="*70 + "\n")
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Verification interrupted by user\n")
        except Exception as e:
            print(f"\n❌ Verification error: {e}\n")
        finally:
            self.client.close()


def main():
    """Main entry point"""
    verifier = MongoDBVerifier()
    verifier.run_full_verification()


if __name__ == "__main__":
    main()