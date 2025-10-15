"""
MongoDB Document Structure Inspector
Shows EXACTLY what's in your Sales_flat collection
"""

import json
from pymongo import MongoClient
from bson import ObjectId

try:
    from ClearVueConfig import ClearVueConfig
except ImportError:
    print("ERROR: ClearVueConfig.py not found")
    exit(1)

print("\n" + "="*70)
print("MONGODB DOCUMENT STRUCTURE INSPECTOR")
print("="*70 + "\n")

# Connect
mongo_client = MongoClient(
    ClearVueConfig.get_mongo_uri(),
    serverSelectionTimeoutMS=5000
)
db = mongo_client[ClearVueConfig.get_database_name()]

print(f"Database: {ClearVueConfig.get_database_name()}")
print(f"Collection: Sales_flat\n")

# Get total count
total_docs = db.Sales_flat.count_documents({})
print(f"Total documents: {total_docs}\n")

if total_docs == 0:
    print("❌ No documents in Sales_flat collection!")
    print("\nGenerate some sales first:")
    print("  1. Go to http://localhost:5000/simulator")
    print("  2. Click 'Generate Sales'")
    print("  3. Run this script again")
    exit(1)

# Get one document
print("="*70)
print("SAMPLE DOCUMENT (RAW)")
print("="*70 + "\n")

sample = db.Sales_flat.find_one()

def print_value(value, indent=0):
    """Pretty print values"""
    prefix = "  " * indent
    if isinstance(value, (str, int, float, bool, type(None))):
        return f"{prefix}{repr(value)}"
    elif isinstance(value, ObjectId):
        return f"{prefix}ObjectId('{value}')"
    elif isinstance(value, dict):
        lines = [f"{prefix}{{"]
        for k, v in value.items():
            lines.append(f"{prefix}  '{k}': {print_value(v, indent+1).strip()},")
        lines.append(f"{prefix}}}")
        return "\n".join(lines)
    elif isinstance(value, list):
        if not value:
            return f"{prefix}[]"
        lines = [f"{prefix}["]
        for item in value[:3]:  # Show first 3 items
            lines.append(f"{print_value(item, indent+1)},")
        if len(value) > 3:
            lines.append(f"{prefix}  ... ({len(value)} total items)")
        lines.append(f"{prefix}]")
        return "\n".join(lines)
    else:
        return f"{prefix}{type(value).__name__}({value})"

print("Document ID:", sample['_id'])
print("\nAll fields in document:\n")

for key in sorted(sample.keys()):
    value = sample[key]
    value_type = type(value).__name__
    
    if value_type == 'ObjectId':
        value_str = f"ObjectId('{value}')"
    elif isinstance(value, dict):
        value_str = f"(dict with {len(value)} keys)"
    elif isinstance(value, list):
        value_str = f"(list with {len(value)} items)"
    elif isinstance(value, str) and len(str(value)) > 50:
        value_str = f"'{str(value)[:50]}...'"
    else:
        value_str = repr(value)
    
    print(f"  {key:30} : {value_type:12} = {value_str}")

print("\n" + "="*70)
print("DOCUMENT STRUCTURE ANALYSIS")
print("="*70 + "\n")

# Check for different structures
has_lines_array = 'lines' in sample and isinstance(sample['lines'], list)
has_line_prefix = any(k.startswith('line_') for k in sample.keys())
has_uppercase = any(k.isupper() or k[0].isupper() for k in sample.keys() if k != '_id')

# Check for common field names (different variations)
field_checks = {
    'Document Number': ['DOC_NUMBER', 'doc_number', 'doc_no', '_id'],
    'Customer ID': ['CUSTOMER_NUMBER', 'customer_id', 'customer_number'],
    'Transaction Type': ['TRANSTYPE_CODE', 'trans_type', 'transaction_type'],
    'Rep Code': ['REP_CODE', 'rep_code', 'rep'],
    'Transaction Date': ['TRANS_DATE', 'trans_date', 'date'],
    'Inventory Code': ['line_INVENTORY_CODE', 'inventory_code', 'product_id'],
    'Quantity': ['line_QUANTITY', 'quantity', 'qty'],
    'Unit Price': ['line_UNIT_SELL_PRICE', 'unit_price', 'unit_sell_price'],
    'Line Total': ['line_TOTAL_LINE_PRICE', 'total_line_cost', 'line_total'],
    'Cost': ['line_LAST_COST', 'last_cost', 'cost'],
}

print("Structure Type:")
if has_lines_array:
    print("  ❌ NESTED structure (has 'lines' array)")
    print("     This is OLD format - needs conversion")
elif has_line_prefix:
    print("  ✅ FLAT structure (has 'line_' prefixed fields)")
    print("     This is CORRECT for streaming pipeline")
elif has_uppercase:
    print("  ✅ FLAT structure (has UPPERCASE fields)")
    print("     This is CORRECT for streaming pipeline")
else:
    print("  ❓ UNKNOWN structure")

print("\nField Mapping (what exists in your document):\n")
for field_name, possible_names in field_checks.items():
    found = None
    for name in possible_names:
        if name in sample:
            found = name
            break
    
    if found:
        value = sample[found]
        if isinstance(value, (dict, list)):
            value_preview = f"({type(value).__name__})"
        else:
            value_preview = str(value)[:40]
        print(f"  ✓ {field_name:20} → {found:25} = {value_preview}")
    else:
        print(f"  ✗ {field_name:20} → NOT FOUND (tried: {', '.join(possible_names)})")

print("\n" + "="*70)
print("POWER BI PAYLOAD BUILDER")
print("="*70 + "\n")

# Try to build a payload
try:
    from datetime import datetime
    
    # Helper function to get first matching field
    def get_field(doc, *field_names):
        for name in field_names:
            if name in doc:
                return doc[name]
        return None
    
    # Get transaction date
    trans_date_raw = get_field(sample, 'TRANS_DATE', 'trans_date', 'date')
    if not trans_date_raw:
        trans_date_raw = datetime.now()
    
    if isinstance(trans_date_raw, str):
        try:
            trans_date = datetime.fromisoformat(trans_date_raw.replace('Z', '+00:00'))
        except:
            trans_date = datetime.now()
    else:
        trans_date = trans_date_raw if isinstance(trans_date_raw, datetime) else datetime.now()
    
    trans_date_str = trans_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    fin_period_date = datetime(trans_date.year, trans_date.month, 1)
    fin_period_str = fin_period_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    # Build payload
    payload = {
        "_id": str(get_field(sample, '_id', 'DOC_NUMBER', 'doc_number') or ''),
        "DOC_NUMBER": str(get_field(sample, 'DOC_NUMBER', 'doc_number', '_id') or ''),
        "TRANSTYPE_CODE": str(get_field(sample, 'TRANSTYPE_CODE', 'trans_type') or ''),
        "REP_CODE": str(get_field(sample, 'REP_CODE', 'rep_code') or ''),
        "CUSTOMER_NUMBER": str(get_field(sample, 'CUSTOMER_NUMBER', 'customer_id') or ''),
        "TRANS_DATE": trans_date_str,
        "FIN_PERIOD": fin_period_str,
        "line_INVENTORY_CODE": str(get_field(sample, 'line_INVENTORY_CODE', 'inventory_code') or ''),
        "line_QUANTITY": int(get_field(sample, 'line_QUANTITY', 'quantity') or 0),
        "line_UNIT_SELL_PRICE": int(get_field(sample, 'line_UNIT_SELL_PRICE', 'unit_price', 'unit_sell_price') or 0),
        "line_TOTAL_LINE_PRICE": int(get_field(sample, 'line_TOTAL_LINE_PRICE', 'total_line_cost', 'line_total') or 0),
        "line_LAST_COST": int(get_field(sample, 'line_LAST_COST', 'last_cost', 'cost') or 0)
    }
    
    print("✓ Successfully built Power BI payload\n")
    print("Payload:")
    for key, value in payload.items():
        status = "✓" if value and value != '0' and value != 0 else "✗"
        print(f"  {status} {key:25} = {value}")
    
    # Check for issues
    missing = [k for k, v in payload.items() if not v or v == '0' or v == 0]
    if missing:
        print(f"\n⚠️  WARNING: These fields are empty/zero:")
        for field in missing:
            print(f"     • {field}")
        print("\n   This might cause issues. Check your MongoDB document structure.")
    else:
        print("\n✅ All required fields have values!")
    
except Exception as e:
    print(f"❌ Error building payload: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*70)
print("RECOMMENDATIONS")
print("="*70 + "\n")

if has_lines_array:
    print("❌ Your documents have NESTED structure")
    print("\nYou need to:")
    print("  1. Update your FastAPI simulator to create FLAT documents")
    print("  2. OR convert existing documents to flat structure")
    print("  3. Each line item should be a separate document")
elif has_line_prefix or has_uppercase:
    print("✅ Your documents have FLAT structure (correct!)")
    print("\nNext steps:")
    print("  1. Use the FIXED send_to_powerbi method I provided")
    print("  2. Make sure field names match between MongoDB and the method")
    print("  3. Test with verify_flat_structure.py script")
else:
    print("❓ Your document structure is unclear")
    print("\nReview the 'All fields in document' section above")
    print("and ensure your FastAPI simulator creates the correct structure")

print("\n" + "="*70 + "\n")