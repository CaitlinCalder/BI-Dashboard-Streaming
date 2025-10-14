"""
ClearVue FastAPI Server - Complete Solution
ONE server with Dashboard + Transaction Generator

Port: 5000
Author: ClearVue Analytics Team
Date: October 2025
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient
from datetime import datetime, timedelta
import random
import time
from faker import Faker
import uvicorn

try:
    from ClearVueConfig import ClearVueConfig
except ImportError:
    print("ERROR: ClearVueConfig.py not found")
    exit(1)

app = FastAPI(title="ClearVue Dashboard & Simulator")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# CONFIGURATION - Paste Your Power BI Embed Code Here!
# ============================================================================

POWERBI_EMBED_CODE = '''
<iframe title="CMPG321_PowerBI_Import" 
        width="1140" 
        height="541.25" 
        src="https://app.powerbi.com/reportEmbed?reportId=1d010b2c-bb11-442c-8a32-4d613bdd24ea&autoAuth=true&ctid=b14d86f1-83ba-4b13-a702-b5c0231b9337" 
        frameborder="0" 
        allowFullScreen="true">
</iframe>
'''

# ============================================================================
# MongoDB & Transaction Generator Setup
# ============================================================================

mongo_client = None
db = None
fake = Faker()
fake.seed_instance(42)

# Sample data cache
sample_customers = []
sample_products = []
sample_suppliers = []

# Statistics
stats = {
    'total_transactions': 0,
    'sales_count': 0,
    'payments_count': 0,
    'purchases_count': 0
}


def connect_mongodb():
    """Connect to MongoDB Atlas"""
    global mongo_client, db, sample_customers, sample_products, sample_suppliers
    
    mongo_uri = ClearVueConfig.get_mongo_uri()
    db_name = ClearVueConfig.get_database_name()
    
    mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = mongo_client[db_name]
    
    # Load sample data
    sample_customers = list(db.Customer_flat_step2.find().limit(50))
    sample_products = list(db.Products_flat.find().limit(50))
    sample_suppliers = list(db.Suppliers.find().limit(20))
    
    print(f"Connected to MongoDB: {db_name}")
    print(f"Loaded: {len(sample_customers)} customers, {len(sample_products)} products, {len(sample_suppliers)} suppliers")


@app.on_event("startup")
async def startup():
    """Initialize on startup"""
    print("\nInitializing ClearVue Server...")
    connect_mongodb()
    print("Server ready!")

def generate_sale():
    if not sample_customers or not sample_products:
        return None
    
    customer = random.choice(sample_customers)
    trans_date = datetime.now()
    doc_number = f"SALE{int(time.time() * 1000) % 1000000}"
    rep_id = f"REP{random.randint(1, 10):02d}"
    
    # Generate line items
    num_lines = random.randint(1, 3)
    flat_documents = []
    
    for line_num in range(num_lines):
        product = random.choice(sample_products)
        quantity = random.randint(1, 10)
        base_cost = product.get('last_cost', 500)
        unit_price = base_cost * random.uniform(1.3, 2.0)
        line_total = quantity * unit_price
        
        # Create ONE FLAT DOCUMENT per line item
        flat_doc = {
            # Header fields (repeated for each line)
            'DOC_NUMBER': doc_number,
            'TRANSTYPE_CODE': '2',  # Tax Invoice code
            'REP_CODE': rep_id,
            'CUSTOMER_NUMBER': str(customer['_id']),
            'TRANS_DATE': trans_date.strftime('%Y-%m-%dT%H:%M:%S'),
            'FIN_PERIOD': trans_date.strftime('%Y-%m'),
            
            # Line item fields (prefixed with "line_")
            'line_INVENTORY_CODE': str(product.get('_id', '')),
            'line_QUANTITY': quantity,
            'line_UNIT_SELL_PRICE': int(round(unit_price)),
            'line_TOTAL_LINE_PRICE': int(round(line_total)),
            'line_LAST_COST': int(base_cost),
            
            # Metadata
            '_source': 'web_simulator',
            '_created_at': datetime.now()
        }
        
        flat_documents.append(flat_doc)
    
    # Insert ALL flattened documents
    if flat_documents:
        db.Sales_flat.insert_many(flat_documents)
        stats['total_transactions'] += len(flat_documents)
        stats['sales_count'] += len(flat_documents)
    
    # Return summary
    total_amount = sum(doc['line_TOTAL_LINE_PRICE'] for doc in flat_documents)
    
    return {
        'type': 'sale',
        'doc_id': doc_number,
        'customer_id': str(customer['_id']),
        'amount': round(total_amount, 2),
        'items': len(flat_documents)
    }


def generate_payment():
    """Generate a payment transaction - FLATTENED"""
    if not sample_customers:
        return None
    
    customer = random.choice(sample_customers)
    deposit_date = datetime.now()
    deposit_ref = f"DEP{int(time.time() * 1000) % 1000000}"
    
    bank_amt = round(random.uniform(5000, 50000), 2)
    discount = round(bank_amt * customer.get('discount', 0) / 100, 2)
    tot_payment = bank_amt - discount
    
    # Create ONE FLAT DOCUMENT (Payments typically have one line)
    flat_doc = {
        'CUSTOMER_NUMBER': str(customer['_id']),
        'DEPOSIT_REF': deposit_ref,
        'DEPOSIT_DATE': deposit_date.strftime('%Y-%m-%dT%H:%M:%S'),
        'FIN_PERIOD': deposit_date.strftime('%Y-%m'),
        'BANK_AMT': int(round(bank_amt)),
        'DISCOUNT': int(round(discount)),
        'TOT_PAYMENT': int(round(tot_payment)),
        '_source': 'web_simulator',
        '_created_at': datetime.now()
    }
    
    db.Payments_flat.insert_one(flat_doc)
    stats['total_transactions'] += 1
    stats['payments_count'] += 1
    
    return {
        'type': 'payment',
        'doc_id': deposit_ref,
        'customer_id': str(customer['_id']),
        'amount': round(tot_payment, 2),
        'discount': round(discount, 2)
    }


def generate_purchase():
    """Generate a purchase transaction - FLATTENED"""
    if not sample_suppliers or not sample_products:
        return None
    
    supplier = random.choice(sample_suppliers)
    purch_date = datetime.now()
    purch_doc_no = f"PO{int(time.time() * 1000) % 1000000}"
    
    # Generate line items
    num_lines = random.randint(1, 3)
    flat_documents = []
    
    for line_num in range(num_lines):
        product = random.choice(sample_products)
        quantity = random.randint(10, 100)
        unit_cost = product.get('last_cost', 500) * random.uniform(0.95, 1.05)
        line_cost = quantity * unit_cost
        
        # Create ONE FLAT DOCUMENT per line item
        flat_doc = {
            'PURCH_DOC_NO': purch_doc_no,
            'SUPPLIER_ID': str(supplier['_id']),
            'PURCH_DATE': purch_date.strftime('%Y-%m-%dT%H:%M:%S'),
            'FIN_PERIOD': purch_date.strftime('%Y-%m'),
            'line_INVENTORY_CODE': str(product.get('_id', '')),
            'line_QUANTITY': quantity,
            'line_UNIT_COST_PRICE': int(round(unit_cost)),
            'line_TOTAL_LINE_COST': int(round(line_cost)),
            '_source': 'web_simulator',
            '_created_at': datetime.now()
        }
        
        flat_documents.append(flat_doc)
    
    # Insert ALL flattened documents
    if flat_documents:
        db.Purchases_flat.insert_many(flat_documents)
        stats['total_transactions'] += len(flat_documents)
        stats['purchases_count'] += len(flat_documents)
    
    # Return summary
    total_cost = sum(doc['line_TOTAL_LINE_COST'] for doc in flat_documents)
    
    return {
        'type': 'purchase',
        'doc_id': purch_doc_no,
        'supplier_id': str(supplier['_id']),
        'amount': round(total_cost, 2),
        'items': len(flat_documents)
    }

# ============================================================================
# API ENDPOINTS
# ============================================================================

class GenerateRequest(BaseModel):
    transaction_type: str
    quantity: int


@app.post("/api/generate")
async def generate_transactions(request: GenerateRequest):
    """Generate transactions"""
    try:
        transaction_type = request.transaction_type
        quantity = request.quantity
        
        results = []
        
        for i in range(quantity):
            if transaction_type == 'sales':
                result = generate_sale()
            elif transaction_type == 'payments':
                result = generate_payment()
            elif transaction_type == 'purchases':
                result = generate_purchase()
            elif transaction_type == 'mixed':
                # Random mix
                choice = random.choice(['sales', 'sales', 'payments', 'purchases'])
                if choice == 'sales':
                    result = generate_sale()
                elif choice == 'payments':
                    result = generate_payment()
                else:
                    result = generate_purchase()
            else:
                continue
            
            if result:
                results.append(result)
        
        return {
            'success': True,
            'generated': len(results),
            'transactions': results[-10:],
            'stats': stats
        }
    
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={'success': False, 'error': str(e)}
        )


@app.get("/api/stats")
async def get_stats():
    """Get current statistics"""
    try:
        return {
            'total_transactions': stats['total_transactions'],
            'sales_count': db.Sales_flat.count_documents({}),
            'payments_count': db.Payments_flat.count_documents({}),
            'purchases_count': db.Purchases_flat.count_documents({})
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={'error': str(e)}
        )


# ============================================================================
# WEB PAGES
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def home():
    """Home page"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ClearVue Platform - Real-time Business Intelligence</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            * { 
                margin: 0; 
                padding: 0; 
                box-sizing: border-box; 
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                background: linear-gradient(135deg, #1a2947 0%, #2d4a7c 50%, #3b82f6 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
                position: relative;
                overflow: hidden;
            }
            
            /* Animated background elements */
            .bg-glow {
                position: absolute;
                border-radius: 50%;
                filter: blur(100px);
                opacity: 0.3;
                animation: glow 4s ease-in-out infinite;
            }
            
            .bg-glow-1 {
                top: -20%;
                left: -20%;
                width: 60%;
                height: 60%;
                background: #3b82f6;
            }
            
            .bg-glow-2 {
                bottom: -20%;
                right: -20%;
                width: 60%;
                height: 60%;
                background: #1e3a8a;
                animation-delay: 2s;
            }
            
            @keyframes glow {
                0%, 100% { opacity: 0.3; transform: scale(1); }
                50% { opacity: 0.15; transform: scale(1.1); }
            }
            
            .container {
                position: relative;
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(20px);
                padding: 60px 80px;
                border-radius: 24px;
                box-shadow: 0 25px 80px rgba(0, 0, 0, 0.4), 0 0 60px rgba(59, 130, 246, 0.3);
                text-align: center;
                max-width: 900px;
                width: 100%;
                animation: fadeUp 0.8s ease-out;
            }
            
            @keyframes fadeUp {
                from {
                    opacity: 0;
                    transform: translateY(30px);
                }
                to {
                    opacity: 1;
                    transform: translateY(0);
                }
            }
            
            h1 {
                font-size: 56px;
                font-weight: 700;
                margin-bottom: 20px;
                background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 50%, #60a5fa 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                background-size: 200% auto;
                animation: shimmer 3s linear infinite;
                letter-spacing: -0.02em;
            }
            
            @keyframes shimmer {
                0% { background-position: 0% center; }
                100% { background-position: 200% center; }
            }
            
            p {
                font-size: 18px;
                color: #64748b;
                margin-bottom: 50px;
                line-height: 1.6;
                max-width: 600px;
                margin-left: auto;
                margin-right: auto;
                animation: fadeIn 0.6s ease-out 0.2s backwards;
            }
            
            @keyframes fadeIn {
                from { opacity: 0; }
                to { opacity: 1; }
            }
            
            .buttons {
                display: flex;
                gap: 20px;
                justify-content: center;
                flex-wrap: wrap;
                animation: fadeIn 0.6s ease-out 0.4s backwards;
            }
            
            .btn {
                padding: 18px 40px;
                font-size: 16px;
                font-weight: 600;
                border: none;
                border-radius: 12px;
                cursor: pointer;
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                text-decoration: none;
                display: inline-flex;
                align-items: center;
                gap: 10px;
                color: white;
                letter-spacing: 0.3px;
                position: relative;
                overflow: hidden;
            }
            
            .btn::before {
                content: '';
                position: absolute;
                top: 0;
                left: -100%;
                width: 100%;
                height: 100%;
                background: rgba(255, 255, 255, 0.2);
                transition: left 0.5s;
            }
            
            .btn:hover::before {
                left: 100%;
            }
            
            .btn:hover {
                transform: translateY(-3px);
                box-shadow: 0 12px 30px rgba(0, 0, 0, 0.25), 0 0 40px rgba(59, 130, 246, 0.4);
            }
            
            .btn:active {
                transform: translateY(-1px);
            }
            
            .btn-dashboard {
                background: linear-gradient(135deg, #1e3a8a 0%, #2563eb 100%);
            }
            
            .btn-simulator {
                background: linear-gradient(135deg, #3b82f6 0%, #60a5fa 100%);
            }
            
            .icon {
                width: 20px;
                height: 20px;
            }
            
            .arrow {
                transition: transform 0.3s;
            }
            
            .btn:hover .arrow {
                transform: translateX(4px);
            }
            
            .features {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                gap: 20px;
                margin-top: 60px;
                animation: fadeIn 0.6s ease-out 0.6s backwards;
            }
            
            .feature-card {
                padding: 24px;
                background: rgba(241, 245, 249, 0.5);
                border-radius: 16px;
                transition: all 0.3s;
                text-align: left;
            }
            
            .feature-card:hover {
                background: rgba(241, 245, 249, 0.8);
                transform: translateY(-2px);
                box-shadow: 0 8px 20px rgba(0, 0, 0, 0.08);
            }
            
            .feature-card h3 {
                font-size: 16px;
                color: #1e3a8a;
                margin-bottom: 8px;
                font-weight: 600;
                transition: color 0.3s;
            }
            
            .feature-card:hover h3 {
                color: #3b82f6;
            }
            
            .feature-card p {
                font-size: 14px;
                color: #64748b;
                margin: 0;
                line-height: 1.5;
            }
            
            @media (max-width: 768px) {
                .container {
                    padding: 40px 30px;
                }
                
                h1 {
                    font-size: 36px;
                }
                
                p {
                    font-size: 16px;
                }
                
                .buttons {
                    flex-direction: column;
                }
                
                .btn {
                    width: 100%;
                }
            }
        </style>
    </head>
    <body>
        <div class="bg-glow bg-glow-1"></div>
        <div class="bg-glow bg-glow-2"></div>
        
        <div class="container">
            <h1>ClearVue Platform</h1>
            <p>Real-time Business Intelligence & Data Generation</p>
            
            <div class="buttons">
                <a href="/dashboard" class="btn btn-dashboard">
                    <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                    </svg>
                    View Dashboards
                    <svg class="icon arrow" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                    </svg>
                </a>
                
                <a href="/simulator" class="btn btn-simulator">
                    <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
                    </svg>
                    Generate Data
                    <svg class="icon arrow" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                    </svg>
                </a>
            </div>
            
            <div class="features">
                <div class="feature-card">
                    <h3>Real-time Analytics</h3>
                    <p>Monitor your business metrics as they happen</p>
                </div>
                <div class="feature-card">
                    <h3>Smart Generation</h3>
                    <p>Create realistic data sets for testing our capabilities</p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return html



@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Power BI Dashboard page"""
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>ClearVue Dashboards - Business Intelligence</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            * {{ 
                margin: 0; 
                padding: 0; 
                box-sizing: border-box; 
            }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                background: linear-gradient(180deg, #f8fafc 0%, #e2e8f0 100%);
                min-height: 100vh;
            }}
            
            /* Header */
            .header {{
                background: linear-gradient(135deg, #1e3a8a 0%, #2563eb 100%);
                padding: 24px 40px;
                box-shadow: 0 4px 20px rgba(30, 58, 138, 0.3);
                display: flex;
                justify-content: space-between;
                align-items: center;
                position: sticky;
                top: 0;
                z-index: 100;
                animation: slideDown 0.5s ease-out;
            }}
            
            @keyframes slideDown {{
                from {{
                    opacity: 0;
                    transform: translateY(-20px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}
            
            .header-content {{
                display: flex;
                align-items: center;
                gap: 16px;
            }}
            
            .header h1 {{
                color: white;
                font-size: 28px;
                font-weight: 700;
                letter-spacing: -0.02em;
                display: flex;
                align-items: center;
                gap: 12px;
            }}
            
            .header-icon {{
                width: 32px;
                height: 32px;
                stroke: white;
            }}
            
            .back-btn {{
                padding: 12px 28px;
                background: rgba(255, 255, 255, 0.15);
                backdrop-filter: blur(10px);
                color: white;
                text-decoration: none;
                border-radius: 10px;
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                font-weight: 600;
                border: 1px solid rgba(255, 255, 255, 0.2);
                display: inline-flex;
                align-items: center;
                gap: 8px;
                font-size: 15px;
            }}
            
            .back-btn:hover {{
                background: rgba(255, 255, 255, 0.25);
                transform: translateY(-2px);
                box-shadow: 0 8px 20px rgba(0, 0, 0, 0.2);
            }}
            
            .back-btn:active {{
                transform: translateY(0);
            }}
            
            .back-icon {{
                width: 18px;
                height: 18px;
                transition: transform 0.3s;
            }}
            
            .back-btn:hover .back-icon {{
                transform: translateX(-4px);
            }}
            
            /* Main Container */
            .dashboard-container {{
                padding: 40px 20px;
                max-width: 1600px;
                margin: 0 auto;
                animation: fadeUp 0.6s ease-out 0.2s backwards;
            }}
            
            @keyframes fadeUp {{
                from {{
                    opacity: 0;
                    transform: translateY(30px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}
            
            /* Dashboard Card */
            .dashboard-embed {{
                background: white;
                border-radius: 16px;
                padding: 32px;
                box-shadow: 0 4px 24px rgba(0, 0, 0, 0.06), 
                           0 0 1px rgba(0, 0, 0, 0.04);
                transition: all 0.3s;
                border: 1px solid rgba(226, 232, 240, 0.8);
            }}
            
            .dashboard-embed:hover {{
                box-shadow: 0 8px 32px rgba(30, 58, 138, 0.12), 
                           0 0 1px rgba(0, 0, 0, 0.04);
            }}
            
            .dashboard-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 24px;
                padding-bottom: 20px;
                border-bottom: 2px solid #f1f5f9;
            }}
            
            .dashboard-title {{
                font-size: 22px;
                font-weight: 700;
                background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
            }}
            
            .dashboard-status {{
                display: flex;
                align-items: center;
                gap: 8px;
                padding: 8px 16px;
                background: rgba(34, 197, 94, 0.1);
                border-radius: 20px;
                font-size: 14px;
                font-weight: 600;
                color: #16a34a;
            }}
            
            .status-dot {{
                width: 8px;
                height: 8px;
                background: #22c55e;
                border-radius: 50%;
                animation: pulse 2s ease-in-out infinite;
            }}
            
            @keyframes pulse {{
                0%, 100% {{ 
                    opacity: 1;
                    transform: scale(1);
                }}
                50% {{ 
                    opacity: 0.5;
                    transform: scale(1.2);
                }}
            }}
            
            .dashboard-embed iframe {{
                width: 100%;
                height: 850px;
                border: none;
                border-radius: 12px;
                background: #f8fafc;
            }}
            
            /* Loading State */
            .loading-placeholder {{
                width: 100%;
                height: 850px;
                background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%);
                background-size: 200% 100%;
                animation: shimmer 2s infinite;
                border-radius: 12px;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #64748b;
                font-size: 16px;
            }}
            
            @keyframes shimmer {{
                0% {{ background-position: 200% 0; }}
                100% {{ background-position: -200% 0; }}
            }}
            
            /* Responsive */
            @media (max-width: 768px) {{
                .header {{
                    padding: 20px 24px;
                    flex-direction: column;
                    gap: 16px;
                    text-align: center;
                }}
                
                .header h1 {{
                    font-size: 22px;
                }}
                
                .dashboard-container {{
                    padding: 24px 16px;
                }}
                
                .dashboard-embed {{
                    padding: 20px;
                }}
                
                .dashboard-header {{
                    flex-direction: column;
                    align-items: flex-start;
                    gap: 12px;
                }}
                
                .dashboard-embed iframe {{
                    height: 600px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <div class="header-content">
                <svg class="header-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
                <h1>ClearVue Dashboards</h1>
            </div>
            <a href="/" class="back-btn">
                <svg class="back-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
                </svg>
                Back to Home
            </a>
        </div>
        
        <div class="dashboard-container">
            <div class="dashboard-embed">
                <div class="dashboard-header">
                    <h2 class="dashboard-title">Business Intelligence Dashboard</h2>
                    <div class="dashboard-status">
                        <span class="status-dot"></span>
                        Live Data
                    </div>
                </div>
                {POWERBI_EMBED_CODE}
            </div>
        </div>
    </body>
    </html>
    """
    return html

@app.get("/api/verify")
async def verify_data():
    """Verify last inserted documents"""
    try:
        last_sales = list(db.Sales_flat.find({'_source': 'web_simulator'})
                         .sort('_created_at', -1).limit(5))
        
        return {
            'total_sales': db.Sales_flat.count_documents({'_source': 'web_simulator'}),
            'last_5': [str(doc['_id']) for doc in last_sales],
            'timestamps': [doc.get('_created_at') for doc in last_sales]
        }
    except Exception as e:
        return {'error': str(e)}
    
@app.get("/simulator", response_class=HTMLResponse)
async def simulator():
    """Transaction Generator page"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ClearVue Transaction Generator</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: #f5f7fa;
            }
            
            .header {
                background: white;
                padding: 20px 40px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                display: flex;
                justify-content: space-between;
                align-items: center;
                border-bottom: 3px solid #1e3c72;
            }
            
            .header h1 {
                color: #1e3c72;
                font-size: 26px;
                font-weight: 600;
            }
            
            .header a {
                padding: 10px 24px;
                background: #1e3c72;
                color: white;
                text-decoration: none;
                border-radius: 6px;
                transition: all 0.3s;
                font-weight: 500;
            }
            
            .header a:hover {
                background: #2a5298;
            }
            
            .container {
                max-width: 1200px;
                margin: 20px auto;
                padding: 0 20px;
            }
            
            .controls {
                background: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 12px rgba(0,0,0,0.08);
                margin-bottom: 20px;
            }
            
            .quantity {
                margin-bottom: 25px;
            }
            
            .quantity label {
                display: block;
                font-weight: 600;
                margin-bottom: 10px;
                color: #333;
                font-size: 14px;
            }
            
            .quantity input {
                width: 100%;
                padding: 12px 16px;
                font-size: 16px;
                border: 2px solid #e1e8ed;
                border-radius: 6px;
                transition: border-color 0.3s;
            }
            
            .quantity input:focus {
                outline: none;
                border-color: #1e3c72;
            }
            
            .button-group {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 15px;
                margin-bottom: 25px;
            }
            
            .btn {
                padding: 16px 24px;
                border: none;
                border-radius: 6px;
                font-size: 15px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s;
                color: white;
                letter-spacing: 0.3px;
            }
            
            .btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            }
            
            .btn:disabled {
                opacity: 0.6;
                cursor: not-allowed;
                transform: none;
            }
            
            .btn-sales { background: #2563eb; }
            .btn-sales:hover { background: #1d4ed8; }
            
            .btn-payments { background: #0891b2; }
            .btn-payments:hover { background: #0e7490; }
            
            .btn-purchases { background: #7c3aed; }
            .btn-purchases:hover { background: #6d28d9; }
            
            .btn-mixed { background: #1e3c72; }
            .btn-mixed:hover { background: #2a5298; }
            
            .stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-top: 25px;
            }
            
            .stat-card {
                background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
                color: white;
                padding: 20px;
                border-radius: 6px;
                text-align: center;
            }
            
            .stat-card h3 { 
                font-size: 13px;
                opacity: 0.9;
                margin-bottom: 8px;
                font-weight: 500;
                letter-spacing: 0.5px;
                text-transform: uppercase;
            }
            
            .stat-card .value { 
                font-size: 32px;
                font-weight: 700;
            }
            
            .feed {
                background: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 12px rgba(0,0,0,0.08);
                max-height: 400px;
                overflow-y: auto;
            }
            
            .feed h2 {
                color: #1e3c72;
                margin-bottom: 15px;
                padding-bottom: 10px;
                border-bottom: 2px solid #e1e8ed;
                font-size: 18px;
                font-weight: 600;
            }
            
            .feed-item {
                padding: 14px;
                margin-bottom: 10px;
                border-radius: 6px;
                background: #f8fafc;
                border-left: 4px solid #2563eb;
                animation: slideIn 0.3s;
            }
            
            .feed-item.payment { border-left-color: #0891b2; }
            .feed-item.purchase { border-left-color: #7c3aed; }
            
            .feed-item .details { 
                color: #475569;
                font-weight: 500;
                margin-bottom: 6px;
                font-size: 14px;
            }
            
            .feed-item .amount { 
                color: #1e3c72;
                font-weight: 700;
                font-size: 16px;
            }
            
            @keyframes slideIn {
                from { opacity: 0; transform: translateX(-20px); }
                to { opacity: 1; transform: translateX(0); }
            }
            
            .loading {
                display: none;
                text-align: center;
                padding: 14px;
                background: #dbeafe;
                border-radius: 6px;
                color: #1e40af;
                font-weight: 600;
                margin-bottom: 20px;
                border: 1px solid #93c5fd;
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Transaction Generator</h1>
            <a href="/">Back to Home</a>
        </div>
        
        <div class="container">
            <div class="controls">
                <div class="loading" id="loading">Generating transactions...</div>
                
                <div class="quantity">
                    <label>Number of Transactions:</label>
                    <input type="number" id="quantity" value="10" min="1" max="100">
                </div>
                
                <div class="button-group">
                    <button class="btn btn-sales" onclick="generate('sales')">
                        Generate Sales
                    </button>
                    <button class="btn btn-payments" onclick="generate('payments')">
                        Generate Payments
                    </button>
                    <button class="btn btn-purchases" onclick="generate('purchases')">
                        Generate Purchases
                    </button>
                    <button class="btn btn-mixed" onclick="generate('mixed')">
                        Generate Mixed
                    </button>
                </div>
                
                <div class="stats">
                    <div class="stat-card">
                        <h3>Total Transactions</h3>
                        <div class="value" id="stat-total">0</div>
                    </div>
                    <div class="stat-card">
                        <h3>Sales</h3>
                        <div class="value" id="stat-sales">0</div>
                    </div>
                    <div class="stat-card">
                        <h3>Payments</h3>
                        <div class="value" id="stat-payments">0</div>
                    </div>
                    <div class="stat-card">
                        <h3>Purchases</h3>
                        <div class="value" id="stat-purchases">0</div>
                    </div>
                </div>
            </div>
            
            <div class="feed">
                <h2>Recent Transactions</h2>
                <div id="feed-items">
                    <p style="color: #94a3b8; text-align: center; padding: 40px; font-size: 14px;">
                        Click a button above to generate transactions
                    </p>
                </div>
            </div>
        </div>
        
        <script>
            const feedEl = document.getElementById('feed-items');
            const loadingEl = document.getElementById('loading');
            
            // Load stats on page load
            loadStats();
            
            async function generate(type) {
                const quantity = document.getElementById('quantity').value;
                
                // Disable buttons
                document.querySelectorAll('.btn').forEach(btn => btn.disabled = true);
                loadingEl.style.display = 'block';
                
                try {
                    const response = await fetch('/api/generate', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            transaction_type: type,
                            quantity: parseInt(quantity)
                        })
                    });
                    
                    const data = await response.json();
                    
                    if (data.success) {
                        // Clear placeholder
                        if (feedEl.querySelector('p')) {
                            feedEl.innerHTML = '';
                        }
                        
                        // Add new transactions
                        data.transactions.reverse().forEach(item => {
                            addFeedItem(item);
                        });
                        
                        // Update stats
                        updateStats(data.stats);
                    } else {
                        alert('Error: ' + data.error);
                    }
                } catch (error) {
                    alert('Error: ' + error.message);
                } finally {
                    // Re-enable buttons
                    document.querySelectorAll('.btn').forEach(btn => btn.disabled = false);
                    loadingEl.style.display = 'none';
                }
            }
            
            function addFeedItem(item) {
                const itemEl = document.createElement('div');
                itemEl.className = `feed-item ${item.type}`;
                
                let details = '';
                if (item.type === 'sale') {
                    details = `${item.doc_id} | Customer: ${item.customer_id} | ${item.items} items`;
                } else if (item.type === 'payment') {
                    details = `${item.doc_id} | Customer: ${item.customer_id}`;
                } else if (item.type === 'purchase') {
                    details = `${item.doc_id} | Supplier: ${item.supplier_id} | ${item.items} items`;
                }
                
                itemEl.innerHTML = `
                    <div class="details">${details}</div>
                    <div class="amount">R${item.amount.toLocaleString('en-ZA', {minimumFractionDigits: 2})}</div>
                `;
                
                feedEl.insertBefore(itemEl, feedEl.firstChild);
                
                // Keep only last 10
                while (feedEl.children.length > 10) {
                    feedEl.removeChild(feedEl.lastChild);
                }
            }
            
            function updateStats(stats) {
                document.getElementById('stat-total').textContent = stats.total_transactions;
                document.getElementById('stat-sales').textContent = stats.sales_count;
                document.getElementById('stat-payments').textContent = stats.payments_count;
                document.getElementById('stat-purchases').textContent = stats.purchases_count;
            }
            
            async function loadStats() {
                try {
                    const response = await fetch('/api/stats');
                    const stats = await response.json();
                    updateStats(stats);
                } catch (error) {
                    console.error('Error loading stats:', error);
                }
            }
            
            // Refresh stats every 10 seconds
            setInterval(loadStats, 10000);
        </script>
    </body>
    </html>
    """
    return html


def main():
    """Start the FastAPI server"""
    print("\n" + "="*70)
    print("CLEARVUE FASTAPI SERVER")
    print("="*70)
    print("\nStarting server with:")
    print("   - Dashboard (Power BI)")
    print("   - Transaction Generator")
    print("\nServer running at:")
    print(f"   Home:       http://localhost:5000")
    print(f"   Dashboard:  http://localhost:5000/dashboard")
    print(f"   Generator:  http://localhost:5000/simulator")
    print("="*70 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()