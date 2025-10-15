"""
Power BI Push Diagnostics Script
Tests and debugs why streaming pipeline isn't pushing to Power BI

Run this to identify issues with your Power BI integration
"""

import json
import requests
from datetime import datetime
from pymongo import MongoClient

try:
    from ClearVueConfig import ClearVueConfig
except ImportError:
    print("ERROR: ClearVueConfig.py not found")
    exit(1)

print("\n" + "="*70)
print("POWER BI PUSH DIAGNOSTICS")
print("="*70 + "\n")

# ============================================================================
# TEST 1: Configuration Validation
# ============================================================================
print("TEST 1: Configuration Validation")
print("-" * 70)

powerbi_url = ClearVueConfig.get_powerbi_push_url()

if not powerbi_url:
    print("❌ CRITICAL: Power BI URL is not configured!")
    print("   Fix: Set POWERBI_PUSH_URL in ClearVueConfig.py")
    exit(1)
else:
    print(f"✓ Power BI URL configured")
    print(f"  URL: {powerbi_url[:80]}...")

# Validate URL format
if not powerbi_url.startswith('https://api.powerbi.com/'):
    print("❌ Invalid Power BI URL format")
    exit(1)

if '/datasets/' not in powerbi_url or '/rows?' not in powerbi_url:
    print("❌ URL missing required components (/datasets/ or /rows?)")
    exit(1)

if 'key=' not in powerbi_url:
    print("❌ URL missing API key parameter")
    exit(1)

print("✓ URL format is valid\n")

# ============================================================================
# TEST 2: Power BI Schema Definition
# ============================================================================
print("TEST 2: Expected Power BI Schema")
print("-" * 70)

expected_schema = {
    "_id": "TEXT",
    "DOC_NUMBER": "TEXT",
    "TRANSTYPE_CODE": "TEXT",
    "REP_CODE": "TEXT",
    "CUSTOMER_NUMBER": "TEXT",
    "TRANS_DATE": "DATETIME",
    "FIN_PERIOD": "DATETIME",
    "line_INVENTORY_CODE": "TEXT",
    "line_QUANTITY": "INT",
    "line_UNIT_SELL_PRICE": "INT",
    "line_TOTAL_LINE_PRICE": "INT",
    "line_LAST_COST": "INT"
}

print("Expected fields in Power BI dataset:")
for field, dtype in expected_schema.items():
    print(f"  • {field:25} : {dtype}")
print()

# ============================================================================
# TEST 3: MongoDB Connection & Sample Data
# ============================================================================
print("TEST 3: MongoDB Connection & Sample Sales Data")
print("-" * 70)

try:
    mongo_client = MongoClient(
        ClearVueConfig.get_mongo_uri(),
        serverSelectionTimeoutMS=5000
    )
    db = mongo_client[ClearVueConfig.get_database_name()]
    
    # Test connection
    mongo_client.admin.command('ping')
    print("✓ MongoDB connected")
    
    # Get sample sales document
    sample_sale = db.Sales_flat.find_one()
    
    if not sample_sale:
        print("❌ No sales documents found in Sales_flat collection")
        print("   Action: Generate some sales using the simulator first")
        exit(1)
    
    print(f"✓ Found sales documents")
    print(f"  Sample doc ID: {sample_sale.get('_id')}")
    print(f"  Has 'lines' field: {('lines' in sample_sale)}")
    
    if 'lines' in sample_sale and isinstance(sample_sale['lines'], list):
        print(f"  Number of line items: {len(sample_sale['lines'])}")
        
        if len(sample_sale['lines']) > 0:
            line = sample_sale['lines'][0]
            print(f"\n  Sample line item keys: {list(line.keys())[:5]}")
    else:
        print("  ⚠️ Document structure may be incorrect (no 'lines' array)")
    
    print()

except Exception as e:
    print(f"❌ MongoDB Error: {e}")
    exit(1)

# ============================================================================
# TEST 4: Build Test Payload
# ============================================================================
print("TEST 4: Building Test Payload")
print("-" * 70)

try:
    # Use the sample document
    doc = sample_sale
    trans_date = doc.get('trans_date', datetime.now())
    
    if isinstance(trans_date, str):
        try:
            trans_date = datetime.fromisoformat(trans_date.replace('Z', '+00:00'))
        except:
            trans_date = datetime.now()
    
    # Format dates
    trans_date_str = trans_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    fin_period_date = datetime(trans_date.year, trans_date.month, 1)
    fin_period_str = fin_period_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    # Get first line item
    lines = doc.get('lines', [])
    if not lines:
        print("❌ No line items in sample document")
        exit(1)
    
    line = lines[0]
    
    # Build payload matching Power BI schema
    test_payload = {
        "_id": str(doc.get('_id', '')),
        "DOC_NUMBER": str(doc.get('doc_number', doc.get('_id', ''))),
        "TRANSTYPE_CODE": str(doc.get('trans_type', '')),
        "REP_CODE": str(doc.get('rep_code', doc.get('rep', {}).get('id', ''))),
        "CUSTOMER_NUMBER": str(doc.get('customer_id', '')),
        "TRANS_DATE": trans_date_str,
        "FIN_PERIOD": fin_period_str,
        "line_INVENTORY_CODE": str(line.get('inventory_code', '')),
        "line_QUANTITY": int(line.get('quantity', 0)),
        "line_UNIT_SELL_PRICE": int(round(line.get('unit_price', line.get('unit_sell_price', 0)))),
        "line_TOTAL_LINE_PRICE": int(round(line.get('total_line_cost', 0))),
        "line_LAST_COST": int(round(line.get('last_cost', 0)))
    }
    
    print("✓ Test payload built successfully")
    print("\nPayload structure:")
    for key, value in test_payload.items():
        value_type = type(value).__name__
        value_preview = str(value)[:40] if len(str(value)) > 40 else str(value)
        print(f"  {key:25} : {value_type:8} = {value_preview}")
    
    print()

except Exception as e:
    print(f"❌ Error building payload: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# ============================================================================
# TEST 5: Test Power BI Push
# ============================================================================
print("TEST 5: Testing Power BI Push")
print("-" * 70)

print("Attempting to push test payload to Power BI...")

try:
    response = requests.post(
        powerbi_url,
        json=[test_payload],  # Power BI expects an array
        headers={'Content-Type': 'application/json'},
        timeout=10
    )
    
    print(f"\nResponse Status: {response.status_code}")
    
    if response.status_code in (200, 201):
        print("✅ SUCCESS! Data pushed to Power BI")
        print("\nResponse body:")
        try:
            print(json.dumps(response.json(), indent=2))
        except:
            print(response.text)
    else:
        print(f"❌ FAILED! Power BI returned error {response.status_code}")
        print("\nError response:")
        print(response.text)
        
        # Common error analysis
        if response.status_code == 400:
            print("\n⚠️ Status 400: Bad Request - Common causes:")
            print("  1. Schema mismatch between payload and Power BI dataset")
            print("  2. Invalid data types (e.g., sending float instead of int)")
            print("  3. Missing required fields")
            print("  4. Invalid date format")
        elif response.status_code == 401:
            print("\n⚠️ Status 401: Unauthorized - API key may be invalid")
        elif response.status_code == 404:
            print("\n⚠️ Status 404: Dataset not found - check dataset ID in URL")
        elif response.status_code == 429:
            print("\n⚠️ Status 429: Rate limit exceeded")

except requests.exceptions.Timeout:
    print("❌ Request timed out - Power BI endpoint not responding")
except requests.exceptions.ConnectionError:
    print("❌ Connection error - check network/firewall")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# TEST 6: Check Pipeline Code Issues
# ============================================================================
print("\n" + "="*70)
print("TEST 6: Code Analysis")
print("-" * 70)

print("\nChecking your pipeline code for common issues:\n")

issues_found = []

# Check 1: Collection name
if "Sales_flat" not in ClearVueConfig.COLLECTIONS.values():
    issues_found.append("❌ Sales_flat not in COLLECTIONS config")
else:
    print("✓ Sales_flat collection is configured")

# Check 2: Priority
sales_priority = ClearVueConfig.get_collection_priority('Sales_flat')
print(f"✓ Sales priority: {sales_priority}")

# Check 3: Topic
sales_topic = ClearVueConfig.get_topic_for_collection('Sales_flat')
print(f"✓ Sales Kafka topic: {sales_topic}")

# Check 4: URL validation
if ClearVueConfig.validate_powerbi_config():
    print("✓ Power BI config validation passes")
else:
    issues_found.append("❌ Power BI config validation failed")

print("\n" + "="*70)
print("DIAGNOSTIC SUMMARY")
print("="*70)

if issues_found:
    print("\n❌ Issues Found:")
    for issue in issues_found:
        print(f"  {issue}")
else:
    print("\n✅ All diagnostics passed!")
    print("\nIf data still isn't flowing to Power BI:")
    print("  1. Verify the streaming pipeline is running")
    print("  2. Check that Sales documents are being inserted into MongoDB")
    print("  3. Look for errors in clearvue_streaming.log")
    print("  4. Verify Power BI dataset is configured for streaming (not import)")
    print("  5. Check if Power BI dataset schema exactly matches our payload")

print("\n" + "="*70 + "\n")

# ============================================================================
# BONUS: Field Mapping Guide
# ============================================================================
print("BONUS: MongoDB → Power BI Field Mapping")
print("-" * 70)
print("\nYour pipeline should map fields like this:")
print("""
MongoDB Sales_flat document structure:
{
  '_id': ObjectId,
  'doc_number': str,
  'trans_type': str,
  'rep_code': str (or rep.id),
  'customer_id': str,
  'trans_date': datetime,
  'lines': [
    {
      'inventory_code': str,
      'quantity': int,
      'unit_price': float,
      'total_line_cost': float,
      'last_cost': float
    }
  ]
}

Power BI streaming dataset should expect:
  _id                   (TEXT)
  DOC_NUMBER            (TEXT)
  TRANSTYPE_CODE        (TEXT)
  REP_CODE              (TEXT)
  CUSTOMER_NUMBER       (TEXT)
  TRANS_DATE            (DATETIME)
  FIN_PERIOD            (DATETIME)
  line_INVENTORY_CODE   (TEXT)
  line_QUANTITY         (INT)
  line_UNIT_SELL_PRICE  (INT)
  line_TOTAL_LINE_PRICE (INT)
  line_LAST_COST        (INT)

Note: All numeric values should be INTs, not FLOATS!
""")

print("="*70 + "\n")