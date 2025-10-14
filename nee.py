"""
ClearVue End-to-End Diagnostic Test Script
Tests the entire pipeline: MongoDB → Kafka → Power BI

Author: ClearVue Team
Date: October 2025
"""

import requests
import json
import time
from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import sys

try:
    from ClearVueConfig import ClearVueConfig
except ImportError:
    print("❌ Error: ClearVueConfig.py not found")
    sys.exit(1)


class ClearVueDiagnostics:
    """Comprehensive diagnostic tests for ClearVue pipeline"""
    
    def __init__(self):
        self.results = {
            'mongodb': {'status': 'unknown', 'details': {}},
            'kafka': {'status': 'unknown', 'details': {}},
            'fastapi': {'status': 'unknown', 'details': {}},
            'powerbi': {'status': 'unknown', 'details': {}},
            'end_to_end': {'status': 'unknown', 'details': {}}
        }
        self.test_doc_number = f"DIAG{int(time.time())}"
        
    def print_header(self, text):
        """Print section header"""
        print("\n" + "="*70)
        print(f"  {text}")
        print("="*70)
    
    def print_test(self, name, status, details=""):
        """Print test result"""
        icon = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
        print(f"{icon} {name:50} [{status}]")
        if details:
            print(f"   {details}")
    
    # ========================================================================
    # TEST 1: MongoDB Connection & Data
    # ========================================================================
    
    def test_mongodb(self):
        """Test MongoDB Atlas connection and data structure"""
        self.print_header("TEST 1: MONGODB CONNECTION & DATA")
        
        try:
            # Connect to MongoDB
            print("\n🔌 Connecting to MongoDB Atlas...")
            mongo_uri = ClearVueConfig.get_mongo_uri()
            db_name = ClearVueConfig.get_database_name()
            
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            db = client[db_name]
            
            # Test 1.1: Connection
            client.admin.command('ping')
            self.print_test("MongoDB connection", "PASS", f"Connected to {db_name}")
            self.results['mongodb']['connection'] = True
            
            # Test 1.2: Collections exist
            collections = db.list_collection_names()
            required = ['Sales_flat', 'Payments_flat', 'Purchases_flat']
            missing = [c for c in required if c not in collections]
            
            if missing:
                self.print_test("Required collections", "FAIL", f"Missing: {missing}")
                self.results['mongodb']['collections'] = False
            else:
                self.print_test("Required collections", "PASS", f"All {len(required)} collections exist")
                self.results['mongodb']['collections'] = True
            
            # Test 1.3: Check for simulator data
            sim_count = db.Sales_flat.count_documents({'_source': 'web_simulator'})
            self.print_test("Simulator sales count", "PASS" if sim_count > 0 else "WARN", 
                          f"{sim_count} documents found")
            self.results['mongodb']['simulator_data_count'] = sim_count
            
            # Test 1.4: Check data structure (flat vs nested)
            if sim_count > 0:
                latest = db.Sales_flat.find_one(
                    {'_source': 'web_simulator'},
                    sort=[('_created_at', -1)]
                )
                
                # Check if it's flat structure
                has_flat_fields = all(k in latest for k in ['DOC_NUMBER', 'REP_CODE', 'line_QUANTITY'])
                has_nested_fields = 'rep' in latest and isinstance(latest.get('rep'), dict)
                
                if has_flat_fields:
                    self.print_test("Data structure", "PASS", "✅ FLAT structure (correct for Power BI)")
                    self.results['mongodb']['structure'] = 'flat'
                elif has_nested_fields:
                    self.print_test("Data structure", "FAIL", "❌ NESTED structure (needs flattening)")
                    self.results['mongodb']['structure'] = 'nested'
                    print("\n⚠️  ACTION REQUIRED: Update server.py with flattened generate_sale()")
                else:
                    self.print_test("Data structure", "WARN", "Unknown structure")
                    self.results['mongodb']['structure'] = 'unknown'
                
                # Show sample fields
                print("\n📋 Sample document fields:")
                for key in list(latest.keys())[:15]:
                    value = latest[key]
                    if isinstance(value, (str, int, float, bool)):
                        print(f"   {key:25} : {value}")
            
            # Test 1.5: Recent data check
            recent_count = db.Sales_flat.count_documents({
                '_source': 'web_simulator',
                '_created_at': {'$gte': datetime.now().replace(hour=0, minute=0, second=0)}
            })
            self.print_test("Today's data", "PASS" if recent_count > 0 else "WARN",
                          f"{recent_count} documents today")
            self.results['mongodb']['recent_count'] = recent_count
            
            self.results['mongodb']['status'] = 'PASS'
            client.close()
            
        except Exception as e:
            self.print_test("MongoDB connection", "FAIL", str(e))
            self.results['mongodb']['status'] = 'FAIL'
            self.results['mongodb']['error'] = str(e)
    
    # ========================================================================
    # TEST 2: Kafka Infrastructure
    # ========================================================================
    
    def test_kafka(self):
        """Test Kafka connectivity and topics"""
        self.print_header("TEST 2: KAFKA INFRASTRUCTURE")
        
        try:
            kafka_servers = ClearVueConfig.get_kafka_servers()
            
            # Test 2.1: Kafka connection
            print("\n🔌 Connecting to Kafka...")
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_servers,
                request_timeout_ms=5000
            )
            self.print_test("Kafka connection", "PASS", f"Connected to {kafka_servers[0]}")
            self.results['kafka']['connection'] = True
            
            # Test 2.2: Topics exist
            existing_topics = admin_client.list_topics()
            required_topics = [config['topic'] for config in ClearVueConfig.KAFKA_TOPICS.values()]
            missing_topics = [t for t in required_topics if t not in existing_topics]
            
            if missing_topics:
                self.print_test("Kafka topics", "FAIL", f"Missing: {missing_topics}")
                self.results['kafka']['topics'] = False
            else:
                self.print_test("Kafka topics", "PASS", f"All {len(required_topics)} topics exist")
                self.results['kafka']['topics'] = True
            
            # Test 2.3: Check for recent messages
            print("\n📨 Checking for recent Kafka messages...")
            consumer = KafkaConsumer(
                'clearvue.sales',
                bootstrap_servers=kafka_servers,
                auto_offset_reset='latest',
                consumer_timeout_ms=2000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            # Try to consume one message
            message_found = False
            for msg in consumer:
                message_found = True
                break
            
            consumer.close()
            
            if message_found:
                self.print_test("Recent Kafka messages", "PASS", "Messages flowing")
                self.results['kafka']['messages_flowing'] = True
            else:
                self.print_test("Recent Kafka messages", "WARN", "No recent messages (pipeline might be stopped)")
                self.results['kafka']['messages_flowing'] = False
            
            self.results['kafka']['status'] = 'PASS'
            admin_client.close()
            
        except Exception as e:
            self.print_test("Kafka connection", "FAIL", str(e))
            self.results['kafka']['status'] = 'FAIL'
            self.results['kafka']['error'] = str(e)
            print("\n💡 Is Kafka running? Try: docker-compose up -d")
    
    # ========================================================================
    # TEST 3: FastAPI Server
    # ========================================================================
    
    def test_fastapi(self):
        """Test FastAPI server and data generation"""
        self.print_header("TEST 3: FASTAPI SERVER")
        
        base_url = "http://localhost:5000"
        
        try:
            # Test 3.1: Server is running
            print("\n🔌 Checking FastAPI server...")
            response = requests.get(f"{base_url}/", timeout=5)
            
            if response.status_code == 200:
                self.print_test("FastAPI server", "PASS", "Server is running on port 5000")
                self.results['fastapi']['running'] = True
            else:
                self.print_test("FastAPI server", "FAIL", f"Status code: {response.status_code}")
                self.results['fastapi']['running'] = False
                return
            
            # Test 3.2: Stats endpoint
            response = requests.get(f"{base_url}/api/stats", timeout=5)
            if response.status_code == 200:
                stats = response.json()
                self.print_test("Stats endpoint", "PASS", f"Total sales: {stats.get('sales_count', 0)}")
                self.results['fastapi']['stats'] = stats
            else:
                self.print_test("Stats endpoint", "FAIL", f"Status: {response.status_code}")
            
            # Test 3.3: Generate test sale
            print("\n🧪 Generating test sale...")
            response = requests.post(
                f"{base_url}/api/generate",
                json={"transaction_type": "sales", "quantity": 1},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    generated = result.get('generated', 0)
                    self.print_test("Data generation", "PASS", f"Generated {generated} transaction(s)")
                    self.results['fastapi']['generation'] = True
                    
                    # Store the doc number for later testing
                    if result.get('transactions'):
                        self.test_doc_number = result['transactions'][0].get('doc_id', self.test_doc_number)
                        print(f"   Test document: {self.test_doc_number}")
                else:
                    self.print_test("Data generation", "FAIL", result.get('error', 'Unknown error'))
                    self.results['fastapi']['generation'] = False
            else:
                self.print_test("Data generation", "FAIL", f"Status: {response.status_code}")
                self.results['fastapi']['generation'] = False
            
            # Test 3.4: Verify test endpoint exists
            try:
                response = requests.post(f"{base_url}/api/test-powerbi-push", timeout=10)
                if response.status_code == 200:
                    self.print_test("Power BI test endpoint", "PASS", "Test endpoint available")
                    self.results['fastapi']['powerbi_test_endpoint'] = True
                else:
                    self.print_test("Power BI test endpoint", "WARN", "Endpoint exists but returned error")
                    self.results['fastapi']['powerbi_test_endpoint'] = False
            except requests.exceptions.ConnectionError:
                self.print_test("Power BI test endpoint", "WARN", "Endpoint not found (add it to server.py)")
                self.results['fastapi']['powerbi_test_endpoint'] = False
            
            self.results['fastapi']['status'] = 'PASS'
            
        except requests.exceptions.ConnectionError:
            self.print_test("FastAPI server", "FAIL", "Cannot connect to localhost:5000")
            self.results['fastapi']['status'] = 'FAIL'
            self.results['fastapi']['error'] = "Server not running"
            print("\n💡 Start the server: python server.py")
        except Exception as e:
            self.print_test("FastAPI server", "FAIL", str(e))
            self.results['fastapi']['status'] = 'FAIL'
            self.results['fastapi']['error'] = str(e)
    
    # ========================================================================
    # TEST 4: Power BI Connection
    # ========================================================================
    
    def test_powerbi(self):
        """Test Power BI streaming dataset connection"""
        self.print_header("TEST 4: POWER BI CONNECTION")
        
        try:
            # Test 4.1: Power BI URL configured
            powerbi_url = ClearVueConfig.get_powerbi_push_url()
            
            if not powerbi_url:
                self.print_test("Power BI URL", "FAIL", "URL not configured in ClearVueConfig.py")
                self.results['powerbi']['configured'] = False
                self.results['powerbi']['status'] = 'FAIL'
                return
            else:
                self.print_test("Power BI URL", "PASS", "URL configured")
                self.results['powerbi']['configured'] = True
            
            # Test 4.2: URL format validation
            if 'api.powerbi.com' in powerbi_url and '/datasets/' in powerbi_url:
                self.print_test("URL format", "PASS", "Valid Power BI streaming URL")
                self.results['powerbi']['url_valid'] = True
            else:
                self.print_test("URL format", "FAIL", "Invalid URL format")
                self.results['powerbi']['url_valid'] = False
                return
            
            # Test 4.3: Send test payload
            print("\n🧪 Sending test payload to Power BI...")
            
            test_date = datetime.now()
            fin_period_date = datetime(test_date.year, test_date.month, 1)
            
            test_payload = {
                "_id": self.test_doc_number,
                "DOC_NUMBER": self.test_doc_number,
                "TRANSTYPE_CODE": "2",
                "REP_CODE": "DIAGTEST",
                "CUSTOMER_NUMBER": "DIAG_CUSTOMER",
                "TRANS_DATE": test_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "FIN_PERIOD": fin_period_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                " line_INVENTORY_CODE": "DIAG_PRODUCT",
                " line_QUANTITY": 777,
                "line_UNIT_SELL_PRICE": 7777,
                " line_TOTAL_LINE_PRICE": 777777,
                " line_LAST_COST": 5000
            }
            
            response = requests.post(
                powerbi_url,
                json=[test_payload],
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code in (200, 201):
                self.print_test("Power BI push", "PASS", f"Status {response.status_code} - Data sent successfully!")
                self.results['powerbi']['push_successful'] = True
                print(f"\n   🎯 Look for this in Power BI dashboard:")
                print(f"      DOC_NUMBER: {self.test_doc_number}")
                print(f"      CUSTOMER: DIAG_CUSTOMER")
                print(f"      AMOUNT: 777,777")
                print(f"      QUANTITY: 777")
            else:
                self.print_test("Power BI push", "FAIL", f"Status {response.status_code}: {response.text}")
                self.results['powerbi']['push_successful'] = False
                self.results['powerbi']['error_response'] = response.text
            
            self.results['powerbi']['status'] = 'PASS' if response.status_code in (200, 201) else 'FAIL'
            
        except requests.exceptions.Timeout:
            self.print_test("Power BI push", "FAIL", "Request timeout")
            self.results['powerbi']['status'] = 'FAIL'
            self.results['powerbi']['error'] = "Timeout"
        except Exception as e:
            self.print_test("Power BI connection", "FAIL", str(e))
            self.results['powerbi']['status'] = 'FAIL'
            self.results['powerbi']['error'] = str(e)
    
    # ========================================================================
    # TEST 5: End-to-End Flow
    # ========================================================================
    
    def test_end_to_end(self):
        """Test complete data flow: Generate → MongoDB → Kafka → Power BI"""
        self.print_header("TEST 5: END-TO-END DATA FLOW")
        
        try:
            print("\n🚀 Testing complete pipeline...\n")
            
            # Step 1: Generate data via FastAPI
            print("Step 1: Generating sale via FastAPI...")
            e2e_doc = f"E2E{int(time.time())}"
            
            # Note: We already generated in FastAPI test, so just verify it
            
            # Step 2: Verify in MongoDB
            print("Step 2: Checking MongoDB...")
            time.sleep(1)  # Give it a moment
            
            mongo_uri = ClearVueConfig.get_mongo_uri()
            db_name = ClearVueConfig.get_database_name()
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            db = client[db_name]
            
            doc_in_mongo = db.Sales_flat.find_one({'DOC_NUMBER': self.test_doc_number})
            
            if doc_in_mongo:
                self.print_test("Data in MongoDB", "PASS", f"Found {self.test_doc_number}")
                self.results['end_to_end']['mongodb'] = True
            else:
                self.print_test("Data in MongoDB", "FAIL", "Document not found")
                self.results['end_to_end']['mongodb'] = False
            
            client.close()
            
            # Step 3: Check if Kafka would detect it (we can't easily test change streams)
            print("Step 3: Kafka detection (requires streaming pipeline running)...")
            self.print_test("Kafka pipeline", "WARN", "Manual verification needed - check pipeline logs")
            
            # Step 4: Power BI (already tested above)
            print("Step 4: Power BI delivery...")
            if self.results['powerbi'].get('push_successful'):
                self.print_test("Power BI delivery", "PASS", "Test payload delivered")
                self.results['end_to_end']['powerbi'] = True
            else:
                self.print_test("Power BI delivery", "FAIL", "Check Power BI test results above")
                self.results['end_to_end']['powerbi'] = False
            
            # Overall E2E status
            all_pass = (
                self.results['end_to_end'].get('mongodb', False) and
                self.results['end_to_end'].get('powerbi', False)
            )
            
            self.results['end_to_end']['status'] = 'PASS' if all_pass else 'PARTIAL'
            
        except Exception as e:
            self.print_test("End-to-end test", "FAIL", str(e))
            self.results['end_to_end']['status'] = 'FAIL'
            self.results['end_to_end']['error'] = str(e)
    
    # ========================================================================
    # SUMMARY & RECOMMENDATIONS
    # ========================================================================
    
    def print_summary(self):
        """Print comprehensive summary and recommendations"""
        self.print_header("DIAGNOSTIC SUMMARY")
        
        print("\n📊 Component Status:")
        print("-" * 70)
        
        components = [
            ('MongoDB Atlas', self.results['mongodb']['status']),
            ('Kafka Infrastructure', self.results['kafka']['status']),
            ('FastAPI Server', self.results['fastapi']['status']),
            ('Power BI Connection', self.results['powerbi']['status']),
            ('End-to-End Flow', self.results['end_to_end']['status'])
        ]
        
        for name, status in components:
            icon = "✅" if status == "PASS" else "⚠️" if status == "PARTIAL" else "❌"
            print(f"  {icon} {name:30} : {status}")
        
        print("\n" + "="*70)
        print("🔍 RECOMMENDATIONS")
        print("="*70)
        
        # MongoDB recommendations
        if self.results['mongodb']['status'] != 'PASS':
            print("\n❌ MongoDB Issues:")
            print("   1. Check MongoDB Atlas connection string")
            print("   2. Verify IP whitelist in Atlas (allow 0.0.0.0/0 for testing)")
            print("   3. Check credentials in ClearVueConfig.py")
        elif self.results['mongodb'].get('structure') == 'nested':
            print("\n⚠️  MongoDB Data Structure:")
            print("   1. Update server.py with flattened generate_sale() function")
            print("   2. Clear old nested documents: db.Sales_flat.deleteMany({'_source': 'web_simulator'})")
            print("   3. Generate new flat documents")
        
        # Kafka recommendations
        if self.results['kafka']['status'] != 'PASS':
            print("\n❌ Kafka Issues:")
            print("   1. Start Kafka: docker-compose up -d")
            print("   2. Check Kafka health: docker-compose ps")
            print("   3. Check logs: docker-compose logs kafka")
        elif not self.results['kafka'].get('messages_flowing'):
            print("\n⚠️  Kafka Pipeline:")
            print("   1. Start streaming pipeline: python ClearVueStreamingPipeline.py")
            print("   2. Check pipeline logs for errors")
        
        # FastAPI recommendations
        if self.results['fastapi']['status'] != 'PASS':
            print("\n❌ FastAPI Issues:")
            print("   1. Start server: python server.py")
            print("   2. Check if port 5000 is available")
            print("   3. Check server logs for errors")
        elif not self.results['fastapi'].get('powerbi_test_endpoint'):
            print("\n⚠️  FastAPI Enhancement:")
            print("   1. Add /api/test-powerbi-push endpoint to server.py")
            print("   2. This helps debug Power BI connection issues")
        
        # Power BI recommendations
        if self.results['powerbi']['status'] != 'PASS':
            print("\n❌ Power BI Issues:")
            if not self.results['powerbi'].get('configured'):
                print("   1. Add POWERBI_PUSH_URL to ClearVueConfig.py")
                print("   2. Get URL from Power BI streaming dataset settings")
            elif not self.results['powerbi'].get('url_valid'):
                print("   1. Check Power BI URL format")
                print("   2. Should be: https://api.powerbi.com/.../datasets/.../rows?key=...")
            elif not self.results['powerbi'].get('push_successful'):
                print("   1. Verify streaming dataset exists in Power BI")
                print("   2. Check field names match exactly (case-sensitive)")
                print("   3. Verify API key hasn't expired")
                print("   4. Check Power BI dataset schema")
        else:
            print("\n✅ Power BI Connected Successfully!")
            print(f"   Look for DOC_NUMBER: {self.test_doc_number} in your dashboard")
            print("   Amount should be: 777,777")
            print("\n   If you don't see it:")
            print("   1. Refresh Power BI dashboard (Ctrl+F5)")
            print("   2. Check date filters on visuals")
            print("   3. Enable auto-refresh (5 second interval)")
            print("   4. Add a card visual with COUNT(DOC_NUMBER) to verify data")
        
        # Overall status
        print("\n" + "="*70)
        all_pass = all(
            self.results[comp]['status'] == 'PASS' 
            for comp in ['mongodb', 'kafka', 'fastapi', 'powerbi']
        )
        
        if all_pass:
            print("🎉 ALL SYSTEMS OPERATIONAL!")
            print("\nNext steps:")
            print("1. Generate more sales: http://localhost:5000/simulator")
            print("2. Check Power BI dashboard for new data")
            print("3. Verify streaming pipeline is running")
        else:
            print("⚠️  SOME ISSUES DETECTED - Review recommendations above")
        
        print("="*70)
    
    def run_all_tests(self):
        """Run all diagnostic tests"""
        print("\n" + "="*70)
        print("   CLEARVUE END-TO-END DIAGNOSTIC TEST SUITE")
        print("   Testing: MongoDB → Kafka → FastAPI → Power BI")
        print("="*70)
        print(f"\n⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        self.test_mongodb()
        self.test_kafka()
        self.test_fastapi()
        self.test_powerbi()
        self.test_end_to_end()
        
        self.print_summary()
        
        print(f"\n⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Save results to file
        with open('diagnostic_results.json', 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        print("\n💾 Full results saved to: diagnostic_results.json")
        print("\n")


def main():
    """Run diagnostics"""
    diagnostics = ClearVueDiagnostics()
    diagnostics.run_all_tests()


if __name__ == "__main__":
    main()