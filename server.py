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
    """Generate a sale transaction"""
    if not sample_customers or not sample_products:
        return None
    
    customer = random.choice(sample_customers)
    trans_date = datetime.now()
    doc_number = f"SALE{int(time.time() * 1000) % 1000000}"
    
    lines = []
    total_amount = 0
    
    for _ in range(random.randint(1, 3)):
        product = random.choice(sample_products)
        quantity = random.randint(1, 10)
        base_cost = product.get('last_cost', 500)
        unit_price = base_cost * random.uniform(1.3, 2.0)
        line_total = quantity * unit_price
        total_amount += line_total
        
        lines.append({
            'product_id': product['_id'],
            'quantity': quantity,
            'unit_sell_price': round(unit_price, 2),
            'total_line_cost': round(line_total, 2),
            'last_cost': base_cost
        })
    
    sale_doc = {
        '_id': doc_number,
        'customer_id': customer['_id'],
        'rep': {
            'id': f"REP{random.randint(1, 10):02d}",
            'desc': fake.name(),
            'commission': random.choice([5, 10, 15])
        },
        'trans_type': 'Tax Invoice',
        'trans_date': trans_date,
        'fin_period': int(f"{trans_date.year}{trans_date.month:02d}"),
        'lines': lines,
        '_source': 'web_simulator',
        '_created_at': datetime.now()
    }
    
    db.Sales_flat.insert_one(sale_doc)
    stats['total_transactions'] += 1
    stats['sales_count'] += 1
    
    return {
        'type': 'sale',
        'doc_id': doc_number,
        'customer_id': customer['_id'],
        'amount': round(total_amount, 2),
        'items': len(lines)
    }


def generate_payment():
    """Generate a payment transaction"""
    if not sample_customers:
        return None
    
    customer = random.choice(sample_customers)
    deposit_date = datetime.now()
    deposit_ref = f"DEP{int(time.time() * 1000) % 1000000}"
    payment_id = f"PAYM{int(time.time() * 1000) % 1000000}"
    
    bank_amt = round(random.uniform(5000, 50000), 2)
    discount = round(bank_amt * customer.get('discount', 0) / 100, 2)
    tot_payment = bank_amt - discount
    
    payment_doc = {
        '_id': payment_id,
        'customer_id': customer['_id'],
        'deposit_ref': deposit_ref,
        'lines': [{
            'fin_period': int(f"{deposit_date.year}{deposit_date.month:02d}"),
            'deposit_date': deposit_date,
            'bank_amt': bank_amt,
            'discount': discount,
            'tot_payment': tot_payment
        }],
        '_source': 'web_simulator',
        '_created_at': datetime.now()
    }
    
    db.Payments_flat.insert_one(payment_doc)
    stats['total_transactions'] += 1
    stats['payments_count'] += 1
    
    return {
        'type': 'payment',
        'doc_id': deposit_ref,
        'customer_id': customer['_id'],
        'amount': round(tot_payment, 2),
        'discount': round(discount, 2)
    }


def generate_purchase():
    """Generate a purchase transaction"""
    if not sample_suppliers or not sample_products:
        return None
    
    supplier = random.choice(sample_suppliers)
    purch_date = datetime.now()
    purch_doc_no = f"PO{int(time.time() * 1000) % 1000000}"
    
    lines = []
    total_cost = 0
    
    for _ in range(random.randint(1, 3)):
        product = random.choice(sample_products)
        quantity = random.randint(10, 100)
        unit_cost = product.get('last_cost', 500) * random.uniform(0.95, 1.05)
        line_cost = quantity * unit_cost
        total_cost += line_cost
        
        lines.append({
            'product_id': product['_id'],
            'quantity': quantity,
            'unit_cost_price': round(unit_cost, 2),
            'total_line_cost': round(line_cost, 2)
        })
    
    purchase_doc = {
        '_id': purch_doc_no,
        'supplier_id': supplier['_id'],
        'purch_date': purch_date,
        'lines': lines,
        '_source': 'web_simulator',
        '_created_at': datetime.now()
    }
    
    db.Purchases_flat.insert_one(purchase_doc)
    stats['total_transactions'] += 1
    stats['purchases_count'] += 1
    
    return {
        'type': 'purchase',
        'doc_id': purch_doc_no,
        'supplier_id': supplier['_id'],
        'amount': round(total_cost, 2),
        'items': len(lines)
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
        <title>ClearVue Platform</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
            }
            
            .container {
                background: white;
                padding: 60px;
                border-radius: 12px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                text-align: center;
                max-width: 600px;
            }
            
            h1 {
                font-size: 42px;
                color: #1e3c72;
                margin-bottom: 15px;
                font-weight: 600;
            }
            
            p {
                font-size: 16px;
                color: #666;
                margin-bottom: 40px;
                line-height: 1.6;
            }
            
            .buttons {
                display: flex;
                gap: 20px;
                justify-content: center;
                flex-wrap: wrap;
            }
            
            .btn {
                padding: 18px 36px;
                font-size: 16px;
                font-weight: 600;
                border: none;
                border-radius: 8px;
                cursor: pointer;
                transition: all 0.3s;
                text-decoration: none;
                display: inline-block;
                color: white;
                letter-spacing: 0.5px;
            }
            
            .btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 20px rgba(0,0,0,0.2);
            }
            
            .btn-dashboard {
                background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            }
            
            .btn-simulator {
                background: linear-gradient(135deg, #0575E6 0%, #021B79 100%);
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>ClearVue Platform</h1>
            <p>Real-time Business Intelligence & Data Generation</p>
            
            <div class="buttons">
                <a href="/dashboard" class="btn btn-dashboard">
                    View Dashboards
                </a>
                <a href="/simulator" class="btn btn-simulator">
                    Generate Data
                </a>
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
        <title>ClearVue Dashboards</title>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: #f5f7fa;
            }}
            
            .header {{
                background: white;
                padding: 20px 40px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                display: flex;
                justify-content: space-between;
                align-items: center;
                border-bottom: 3px solid #1e3c72;
            }}
            
            .header h1 {{
                color: #1e3c72;
                font-size: 26px;
                font-weight: 600;
            }}
            
            .header a {{
                padding: 10px 24px;
                background: #1e3c72;
                color: white;
                text-decoration: none;
                border-radius: 6px;
                transition: all 0.3s;
                font-weight: 500;
            }}
            
            .header a:hover {{
                background: #2a5298;
            }}
            
            .dashboard-container {{
                padding: 20px;
                max-width: 1400px;
                margin: 0 auto;
            }}
            
            .dashboard-embed {{
                background: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 12px rgba(0,0,0,0.08);
                min-height: 800px;
            }}
            
            .dashboard-embed iframe {{
                width: 100%;
                height: 800px;
                border: none;
                border-radius: 4px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>ClearVue Dashboards</h1>
            <a href="/">Back to Home</a>
        </div>
        
        <div class="dashboard-container">
            <div class="dashboard-embed">
                {POWERBI_EMBED_CODE}
            </div>
        </div>
    </body>
    </html>
    """
    return html


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