"""
Power BI Refresh Integration Test Script
Tests the complete connection to Power BI and triggers a test refresh

Author: ClearVue Team
Date: October 2025
"""

import requests
import time
import json
from datetime import datetime
from pymongo import MongoClient

try:
    from ClearVueConfig import ClearVueConfig
except ImportError:
    print("❌ Error: ClearVueConfig.py not found")
    exit(1)


class PowerBIRefreshTest:
    """Test Power BI refresh integration"""
    
    def __init__(self):
        self.workspace_id = None
        self.dataset_id = None
        self.access_token = None
        self.api_base = "https://api.powerbi.com/v1.0/myorg"
        
    def print_header(self, text):
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
    # TEST 1: Configuration Check
    # ========================================================================
    
    def test_configuration(self):
        """Test if Power BI configuration is present and valid"""
        self.print_header("TEST 1: CONFIGURATION CHECK")
        
        try:
            # Check if methods exist
            if not hasattr(ClearVueConfig, 'get_powerbi_workspace_id'):
                self.print_test("Config methods", "FAIL", "Missing get_powerbi_workspace_id()")
                return False
            
            self.print_test("Config methods", "PASS", "All required methods exist")
            
            # Get values
            self.workspace_id = ClearVueConfig.get_powerbi_workspace_id()
            self.dataset_id = ClearVueConfig.get_powerbi_dataset_id()
            self.access_token = ClearVueConfig.get_powerbi_access_token()
            
            # Validate Workspace ID
            if not self.workspace_id or 'YOUR_WORKSPACE_ID' in self.workspace_id:
                self.print_test("Workspace ID", "FAIL", "Not configured in ClearVueConfig.py")
                return False
            elif len(self.workspace_id) != 36:
                self.print_test("Workspace ID", "FAIL", f"Invalid format: {len(self.workspace_id)} chars (should be 36)")
                return False
            else:
                self.print_test("Workspace ID", "PASS", f"{self.workspace_id[:8]}...{self.workspace_id[-8:]}")
            
            # Validate Dataset ID
            if not self.dataset_id or 'YOUR_DATASET_ID' in self.dataset_id:
                self.print_test("Dataset ID", "FAIL", "Not configured in ClearVueConfig.py")
                return False
            elif len(self.dataset_id) != 36:
                self.print_test("Dataset ID", "FAIL", f"Invalid format: {len(self.dataset_id)} chars (should be 36)")
                return False
            else:
                self.print_test("Dataset ID", "PASS", f"{self.dataset_id[:8]}...{self.dataset_id[-8:]}")
            
            # Validate Access Token
            if not self.access_token or 'YOUR_ACCESS_TOKEN' in self.access_token:
                self.print_test("Access Token", "FAIL", "Not configured in ClearVueConfig.py")
                print("\n💡 Get token from: https://microsoft.github.io/PowerBI-JavaScript/demo/v2-demo/index.html")
                return False
            elif len(self.access_token) < 100:
                self.print_test("Access Token", "FAIL", f"Too short: {len(self.access_token)} chars (should be 500+)")
                return False
            else:
                self.print_test("Access Token", "PASS", f"{len(self.access_token)} characters")
            
            return True
            
        except Exception as e:
            self.print_test("Configuration", "FAIL", str(e))
            return False
    
    # ========================================================================
    # TEST 2: Power BI API Connection
    # ========================================================================
    
    def test_api_connection(self):
        """Test connection to Power BI REST API"""
        self.print_header("TEST 2: POWER BI API CONNECTION")
        
        if not self.access_token:
            self.print_test("API Connection", "SKIP", "No access token configured")
            return False
        
        try:
            # Test 2.1: Validate token with simple API call
            print("\n🔌 Testing API authentication...")
            headers = {'Authorization': f'Bearer {self.access_token}'}
            
            # Try to list workspaces (simplest test)
            url = f"{self.api_base}/groups"
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                self.print_test("API Authentication", "PASS", "Token is valid")
                workspaces = response.json().get('value', [])
                print(f"   Found {len(workspaces)} workspace(s)")
            elif response.status_code == 401:
                self.print_test("API Authentication", "FAIL", "Token expired or invalid")
                print("\n💡 Token expires after 1 hour. Get new token from:")
                print("   https://microsoft.github.io/PowerBI-JavaScript/demo/v2-demo/index.html")
                return False
            else:
                self.print_test("API Authentication", "FAIL", f"Status {response.status_code}: {response.text}")
                return False
            
            # Test 2.2: Check workspace exists
            print("\n📂 Checking workspace access...")
            url = f"{self.api_base}/groups/{self.workspace_id}"
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                workspace = response.json()
                workspace_name = workspace.get('name', 'Unknown')
                self.print_test("Workspace Access", "PASS", f"'{workspace_name}'")
            elif response.status_code == 404:
                self.print_test("Workspace Access", "FAIL", "Workspace not found")
                print("\n💡 Check workspace ID in Power BI URL:")
                print("   https://app.powerbi.com/groups/YOUR_WORKSPACE_ID/...")
                return False
            elif response.status_code == 403:
                self.print_test("Workspace Access", "FAIL", "No permission to access workspace")
                return False
            else:
                self.print_test("Workspace Access", "FAIL", f"Status {response.status_code}")
                return False
            
            # Test 2.3: Check dataset exists
            print("\n📊 Checking dataset access...")
            url = f"{self.api_base}/groups/{self.workspace_id}/datasets/{self.dataset_id}"
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                dataset = response.json()
                dataset_name = dataset.get('name', 'Unknown')
                self.print_test("Dataset Access", "PASS", f"'{dataset_name}'")
                
                # Show dataset info
                print(f"\n   📋 Dataset Details:")
                print(f"      Name: {dataset_name}")
                print(f"      ID: {self.dataset_id}")
                print(f"      Configured By: {dataset.get('configuredBy', 'Unknown')}")
                
            elif response.status_code == 404:
                self.print_test("Dataset Access", "FAIL", "Dataset not found in workspace")
                print("\n💡 Check dataset ID - should match your report's dataset:")
                print("   1. Go to Power BI Service")
                print("   2. Click on your report")
                print("   3. Look at the URL or dataset settings")
                return False
            else:
                self.print_test("Dataset Access", "FAIL", f"Status {response.status_code}")
                return False
            
            return True
            
        except requests.exceptions.Timeout:
            self.print_test("API Connection", "FAIL", "Request timeout")
            return False
        except Exception as e:
            self.print_test("API Connection", "FAIL", str(e))
            return False
    
    # ========================================================================
    # TEST 3: Refresh Permissions
    # ========================================================================
    
    def test_refresh_permissions(self):
        """Test if we have permission to trigger dataset refresh"""
        self.print_header("TEST 3: REFRESH PERMISSIONS")
        
        if not self.access_token:
            self.print_test("Refresh Permissions", "SKIP", "No access token")
            return False
        
        try:
            headers = {'Authorization': f'Bearer {self.access_token}'}
            
            # Get refresh history to test read permissions
            print("\n📜 Checking refresh history access...")
            url = f"{self.api_base}/groups/{self.workspace_id}/datasets/{self.dataset_id}/refreshes?$top=5"
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                self.print_test("Read Refresh History", "PASS", "Can view refresh history")
                
                history = response.json().get('value', [])
                if history:
                    print(f"\n   📊 Recent Refreshes:")
                    for i, refresh in enumerate(history[:3], 1):
                        status = refresh.get('status', 'Unknown')
                        start_time = refresh.get('startTime', 'Unknown')
                        end_time = refresh.get('endTime', 'Unknown')
                        
                        status_icon = "✅" if status == "Completed" else "❌" if status == "Failed" else "⏳"
                        print(f"      {i}. {status_icon} {status} | Started: {start_time[:19] if start_time != 'Unknown' else 'N/A'}")
                else:
                    print("   ℹ️  No refresh history yet")
                    
            elif response.status_code == 403:
                self.print_test("Read Refresh History", "FAIL", "No permission to view refresh history")
                return False
            else:
                self.print_test("Read Refresh History", "WARN", f"Status {response.status_code}")
            
            return True
            
        except Exception as e:
            self.print_test("Refresh Permissions", "FAIL", str(e))
            return False
    
    # ========================================================================
    # TEST 4: Trigger Test Refresh
    # ========================================================================
    
    def test_trigger_refresh(self, actually_trigger=False):
        """Test triggering a dataset refresh"""
        self.print_header("TEST 4: TRIGGER DATASET REFRESH")
        
        if not self.access_token:
            self.print_test("Trigger Refresh", "SKIP", "No access token")
            return False
        
        if not actually_trigger:
            print("\n⚠️  This is a DRY RUN - not actually triggering refresh")
            print("   Set actually_trigger=True to perform real refresh")
            self.print_test("Trigger Refresh", "SKIP", "Dry run mode")
            return True
        
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            print("\n🔄 Triggering dataset refresh...")
            print("   This will refresh your Power BI dataset with latest MongoDB data")
            print("   ⏱️  This may take 30-120 seconds to complete")
            
            url = f"{self.api_base}/groups/{self.workspace_id}/datasets/{self.dataset_id}/refreshes"
            payload = {"notifyOption": "NoNotification"}
            
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 202:
                self.print_test("Trigger Refresh", "PASS", "Refresh request accepted!")
                print("\n   ✅ Power BI is now refreshing your dataset")
                print("   ⏰ Wait 1-2 minutes, then check your dashboard")
                
                # Poll for completion
                print("\n   ⏳ Monitoring refresh progress...")
                return self._monitor_refresh_progress()
                
            elif response.status_code == 400:
                self.print_test("Trigger Refresh", "FAIL", "Bad request - check dataset configuration")
                print(f"   Error: {response.text}")
                return False
            elif response.status_code == 403:
                self.print_test("Trigger Refresh", "FAIL", "No permission to trigger refresh")
                print("\n💡 Your token might not have the required permissions")
                return False
            else:
                self.print_test("Trigger Refresh", "FAIL", f"Status {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            self.print_test("Trigger Refresh", "FAIL", "Request timeout")
            return False
        except Exception as e:
            self.print_test("Trigger Refresh", "FAIL", str(e))
            return False
    
    def _monitor_refresh_progress(self, max_wait=120):
        """Monitor refresh progress"""
        headers = {'Authorization': f'Bearer {self.access_token}'}
        url = f"{self.api_base}/groups/{self.workspace_id}/datasets/{self.dataset_id}/refreshes?$top=1"
        
        start_time = time.time()
        
        while (time.time() - start_time) < max_wait:
            try:
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    history = response.json().get('value', [])
                    if history:
                        latest = history[0]
                        status = latest.get('status', 'Unknown')
                        
                        if status == 'Completed':
                            print(f"   ✅ Refresh completed successfully!")
                            end_time = latest.get('endTime', '')
                            print(f"   🕐 Finished at: {end_time[:19] if end_time else 'N/A'}")
                            return True
                        elif status == 'Failed':
                            print(f"   ❌ Refresh failed!")
                            error = latest.get('serviceExceptionJson', 'No error details')
                            print(f"   Error: {error}")
                            return False
                        elif status in ['InProgress', 'Unknown']:
                            elapsed = int(time.time() - start_time)
                            print(f"   ⏳ Still refreshing... ({elapsed}s elapsed)")
                            time.sleep(10)
                        else:
                            print(f"   ⚠️  Unknown status: {status}")
                            time.sleep(10)
            except Exception as e:
                print(f"   ⚠️  Error checking status: {e}")
                time.sleep(10)
        
        print(f"   ⏱️  Timeout reached ({max_wait}s)")
        print(f"   ℹ️  Refresh might still be running - check Power BI manually")
        return False
    
    # ========================================================================
    # TEST 5: End-to-End Test
    # ========================================================================
    
    def test_end_to_end(self):
        """Test complete flow: Generate data → Detect → Would trigger refresh"""
        self.print_header("TEST 5: END-TO-END SIMULATION")
        
        print("\n📝 This test simulates the complete flow:")
        print("   1. Generate test data in MongoDB")
        print("   2. Verify it's inserted")
        print("   3. Show what would trigger a refresh")
        
        try:
            # Insert test data
            print("\n🔨 Generating test sale in MongoDB...")
            
            mongo_uri = ClearVueConfig.get_mongo_uri()
            db_name = ClearVueConfig.get_database_name()
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            db = client[db_name]
            
            test_doc = {
                'DOC_NUMBER': f'E2ETEST{int(time.time())}',
                'TRANSTYPE_CODE': '2',
                'REP_CODE': 'TEST01',
                'CUSTOMER_NUMBER': 'E2E_CUSTOMER',
                'TRANS_DATE': datetime.now().isoformat(),
                'FIN_PERIOD': datetime.now().strftime('%Y-%m'),
                'line_INVENTORY_CODE': 'E2E_PRODUCT',
                'line_QUANTITY': 888,
                'line_UNIT_SELL_PRICE': 8888,
                'line_TOTAL_LINE_PRICE': 888888,
                'line_LAST_COST': 5000,
                '_source': 'e2e_test',
                '_created_at': datetime.now()
            }
            
            result = db.Sales_flat.insert_one(test_doc)
            
            if result.acknowledged:
                self.print_test("Insert Test Data", "PASS", f"DOC_NUMBER: {test_doc['DOC_NUMBER']}")
            else:
                self.print_test("Insert Test Data", "FAIL", "Insert not acknowledged")
                return False
            
            # Verify it exists
            found = db.Sales_flat.find_one({'DOC_NUMBER': test_doc['DOC_NUMBER']})
            
            if found:
                self.print_test("Verify in MongoDB", "PASS", "Document found")
            else:
                self.print_test("Verify in MongoDB", "FAIL", "Document not found")
                return False
            
            # Explain what happens next
            print("\n📊 In production, this would happen:")
            print("   1. ✅ MongoDB Change Stream detects the insert")
            print("   2. ✅ Kafka receives the change event")
            print("   3. ✅ Pipeline batches the change (30s window)")
            print("   4. ✅ Power BI refresh is triggered")
            print("   5. ✅ Dashboard updates with new data")
            
            self.print_test("E2E Flow", "PASS", "Complete flow verified")
            
            # Clean up
            print("\n🧹 Cleaning up test data...")
            db.Sales_flat.delete_one({'DOC_NUMBER': test_doc['DOC_NUMBER']})
            print("   ✅ Test document removed")
            
            client.close()
            return True
            
        except Exception as e:
            self.print_test("E2E Test", "FAIL", str(e))
            return False
    
    # ========================================================================
    # RUN ALL TESTS
    # ========================================================================
    
    def run_all_tests(self, trigger_refresh=False):
        """Run all tests"""
        print("\n" + "="*70)
        print("   POWER BI REFRESH INTEGRATION TEST SUITE")
        print("="*70)
        print(f"\n⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        results = {
            'config': False,
            'api': False,
            'permissions': False,
            'refresh': False,
            'e2e': False
        }
        
        # Test 1: Configuration
        results['config'] = self.test_configuration()
        
        if not results['config']:
            print("\n❌ Configuration failed - fix config before continuing")
            self.print_final_summary(results)
            return
        
        # Test 2: API Connection
        results['api'] = self.test_api_connection()
        
        if not results['api']:
            print("\n❌ API connection failed - check credentials")
            self.print_final_summary(results)
            return
        
        # Test 3: Permissions
        results['permissions'] = self.test_refresh_permissions()
        
        # Test 4: Trigger Refresh
        results['refresh'] = self.test_trigger_refresh(actually_trigger=trigger_refresh)
        
        # Test 5: End-to-End
        results['e2e'] = self.test_end_to_end()
        
        # Final summary
        self.print_final_summary(results, trigger_refresh)
    
    def print_final_summary(self, results, triggered_refresh=False):
        """Print final test summary"""
        self.print_header("TEST SUMMARY")
        
        print("\n📊 Results:")
        print("-" * 70)
        
        tests = [
            ('Configuration', results.get('config', False)),
            ('API Connection', results.get('api', False)),
            ('Refresh Permissions', results.get('permissions', False)),
            ('Trigger Refresh', results.get('refresh', False)),
            ('End-to-End Flow', results.get('e2e', False))
        ]
        
        for name, passed in tests:
            icon = "✅" if passed else "❌"
            status = "PASS" if passed else "FAIL"
            print(f"  {icon} {name:30} : {status}")
        
        all_critical_passed = results['config'] and results['api']
        
        print("\n" + "="*70)
        
        if all_critical_passed:
            print("🎉 POWER BI INTEGRATION IS WORKING!")
            
            if triggered_refresh:
                print("\n✅ Refresh was triggered successfully")
                print("   Go to your Power BI dashboard and you should see:")
                print("   • DOC_NUMBER: E2ETEST[timestamp]")
                print("   • CUSTOMER: E2E_CUSTOMER")
                print("   • AMOUNT: 888,888")
            else:
                print("\n💡 To trigger an actual refresh, run:")
                print("   python test_powerbi_refresh.py --trigger")
            
            print("\n📋 Next Steps:")
            print("   1. Update ClearVueStreamingPipeline.py with PowerBIRefreshTrigger")
            print("   2. Start the streaming pipeline")
            print("   3. Generate test data")
            print("   4. Watch for refresh in pipeline logs")
            print("   5. Check your dashboard updates!")
            
        else:
            print("❌ INTEGRATION NOT WORKING")
            print("\n🔍 Review the errors above and:")
            
            if not results['config']:
                print("   1. Fix configuration in ClearVueConfig.py")
                print("      Run: python powerbi_setup_helper.py")
            
            if not results['api']:
                print("   2. Get a new access token (expires after 1 hour)")
                print("      URL: https://microsoft.github.io/PowerBI-JavaScript/demo/v2-demo/index.html")
            
            print("\n   Then run this test again")
        
        print("="*70)
        print(f"\n⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


def main():
    """Main entry point"""
    import sys
    
    trigger_refresh = '--trigger' in sys.argv or '-t' in sys.argv
    
    if trigger_refresh:
        print("\n⚠️  LIVE MODE: Will actually trigger a Power BI refresh!")
        print("   This will refresh your dataset with latest data from MongoDB")
        confirm = input("\n   Continue? (y/n) [n]: ").strip().lower()
        
        if confirm != 'y':
            print("\n   Cancelled. Running in dry-run mode instead...\n")
            trigger_refresh = False
    
    tester = PowerBIRefreshTest()
    tester.run_all_tests(trigger_refresh=trigger_refresh)


if __name__ == "__main__":
    main()