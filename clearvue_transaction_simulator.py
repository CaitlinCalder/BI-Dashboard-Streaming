"""
ClearVue Web-Based Transaction Simulator
Simple web UI for your existing transaction simulator

Port: 5001
Author: ClearVue Analytics Team  
Date: October 2025
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Import your existing simulator class
try:
    from ClearVue_Transaction_Simulator import ClearVueTransactionSimulator
except ImportError:
    print("❌ Error: ClearVue_Transaction_Simulator.py not found")
    exit(1)

app = FastAPI(title="ClearVue Transaction Simulator Web")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize simulator (once at startup)
simulator = None

@app.on_event("startup")
async def startup_event():
    """Initialize simulator on startup"""
    global simulator
    print("\n🚀 Initializing Transaction Simulator...")
    simulator = ClearVueTransactionSimulator()
    print("✅ Simulator ready!")


# Request model
class GenerateRequest(BaseModel):
    transaction_type: str
    quantity: int


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.post("/api/generate")
async def generate_transactions(request: GenerateRequest):
    """Generate transactions via API"""
    try:
        transaction_type = request.transaction_type
        quantity = request.quantity
        
        results = []
        
        for i in range(quantity):
            success = simulator.insert_transaction(transaction_type)
            if success:
                # Get last transaction info based on type
                if transaction_type in ['sales', 'mixed']:
                    doc = simulator.db.Sales_flat.find_one(
                        {'_source': {'$in': ['simulator', 'web_simulator']}},
                        sort=[('_created_at', -1)]
                    )
                    if doc:
                        total = sum(line['total_line_cost'] for line in doc.get('lines', []))
                        results.append({
                            'type': 'sale',
                            'doc_id': doc['_id'],
                            'customer_id': doc['customer_id'],
                            'amount': round(total, 2),
                            'items': len(doc.get('lines', []))
                        })
                
                elif transaction_type == 'payments':
                    doc = simulator.db.Payments_flat.find_one(
                        {'_source': {'$in': ['simulator', 'web_simulator']}},
                        sort=[('_created_at', -1)]
                    )
                    if doc:
                        total = sum(line['tot_payment'] for line in doc.get('lines', []))
                        results.append({
                            'type': 'payment',
                            'doc_id': doc['deposit_ref'],
                            'customer_id': doc['customer_id'],
                            'amount': round(total, 2)
                        })
                
                elif transaction_type == 'purchases':
                    doc = simulator.db.Purchases_flat.find_one(
                        {'_source': {'$in': ['simulator', 'web_simulator']}},
                        sort=[('_created_at', -1)]
                    )
                    if doc:
                        total = sum(line['total_line_cost'] for line in doc.get('lines', []))
                        results.append({
                            'type': 'purchase',
                            'doc_id': doc['_id'],
                            'supplier_id': doc['supplier_id'],
                            'amount': round(total, 2),
                            'items': len(doc.get('lines', []))
                        })
        
        # Get statistics
        stats = {
            'total_transactions': simulator.transaction_count,
            'sales_count': simulator.db.Sales_flat.count_documents({'_source': {'$in': ['simulator', 'web_simulator']}}),
            'payments_count': simulator.db.Payments_flat.count_documents({'_source': {'$in': ['simulator', 'web_simulator']}}),
            'purchases_count': simulator.db.Purchases_flat.count_documents({'_source': {'$in': ['simulator', 'web_simulator']}})
        }
        
        return {
            'success': True,
            'generated': len(results),
            'transactions': results[-10:],  # Return last 10
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
        stats = {
            'total_transactions': simulator.transaction_count,
            'sales_count': simulator.db.Sales_flat.count_documents({}),
            'payments_count': simulator.db.Payments_flat.count_documents({}),
            'purchases_count': simulator.db.Purchases_flat.count_documents({}),
            'customers_count': simulator.db.Customer_flat_step2.count_documents({}),
            'products_count': simulator.db.Products_flat.count_documents({}),
            'suppliers_count': simulator.db.Suppliers.count_documents({})
        }
        return stats
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={'error': str(e)}
        )


@app.get("/", response_class=HTMLResponse)
async def home():
    """Simple web UI"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ClearVue Transaction Generator</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }
            
            .container { max-width: 1000px; margin: 0 auto; }
            
            .header {
                background: white;
                padding: 30px;
                border-radius: 12px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }
            
            .header h1 { color: #333; font-size: 32px; margin-bottom: 10px; }
            .header p { color: #666; }
            
            .controls {
                background: white;
                padding: 30px;
                border-radius: 12px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }
            
            .button-group {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 20px;
            }
            
            .btn {
                padding: 20px;
                border: none;
                border-radius: 8px;
                font-size: 18px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s;
                color: white;
            }
            
            .btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            }
            
            .btn:disabled {
                opacity: 0.6;
                cursor: not-allowed;
                transform: none;
            }
            
            .btn-sales { background: #4CAF50; }
            .btn-payments { background: #2196F3; }
            .btn-purchases { background: #FF9800; }
            .btn-mixed { background: #9C27B0; }
            
            .quantity {
                margin-bottom: 20px;
            }
            
            .quantity label {
                display: block;
                font-weight: 600;
                margin-bottom: 10px;
                color: #333;
            }
            
            .quantity input {
                width: 100%;
                padding: 12px;
                font-size: 18px;
                border: 2px solid #ddd;
                border-radius: 8px;
            }
            
            .stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 15px;
                margin-top: 20px;
            }
            
            .stat-card {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px;
                border-radius: 8px;
                text-align: center;
            }
            
            .stat-card h3 { font-size: 14px; opacity: 0.9; margin-bottom: 10px; }
            .stat-card .value { font-size: 28px; font-weight: bold; }
            
            .feed {
                background: white;
                padding: 20px;
                border-radius: 12px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                max-height: 400px;
                overflow-y: auto;
            }
            
            .feed h2 {
                color: #333;
                margin-bottom: 15px;
                padding-bottom: 10px;
                border-bottom: 2px solid #667eea;
            }
            
            .feed-item {
                padding: 12px;
                margin-bottom: 10px;
                border-radius: 6px;
                background: #f8f9fa;
                border-left: 4px solid #4CAF50;
                animation: slideIn 0.3s;
            }
            
            .feed-item.payment { border-left-color: #2196F3; }
            .feed-item.purchase { border-left-color: #FF9800; }
            
            .feed-item .details { color: #333; font-weight: 500; }
            .feed-item .amount { color: #4CAF50; font-weight: bold; font-size: 16px; }
            
            @keyframes slideIn {
                from { opacity: 0; transform: translateX(-20px); }
                to { opacity: 1; transform: translateX(0); }
            }
            
            .loading {
                display: none;
                text-align: center;
                padding: 20px;
                color: #667eea;
                font-weight: 600;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎲 ClearVue Transaction Generator</h1>
                <p>Generate realistic test transactions for your streaming pipeline</p>
            </div>
            
            <div class="controls">
                <div class="quantity">
                    <label>📊 Number of Transactions:</label>
                    <input type="number" id="quantity" value="10" min="1" max="100">
                </div>
                
                <div class="button-group">
                    <button class="btn btn-sales" onclick="generate('sales')">
                        📊 Sales
                    </button>
                    <button class="btn btn-payments" onclick="generate('payments')">
                        💳 Payments
                    </button>
                    <button class="btn btn-purchases" onclick="generate('purchases')">
                        📦 Purchases
                    </button>
                    <button class="btn btn-mixed" onclick="generate('mixed')">
                        🔀 Mixed
                    </button>
                </div>
                
                <div class="loading" id="loading">⏳ Generating transactions...</div>
                
                <div class="stats">
                    <div class="stat-card">
                        <h3>Total</h3>
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
                <h2>📝 Recent Transactions</h2>
                <div id="feed-items">
                    <p style="color: #999; text-align: center;">Click a button above to generate transactions</p>
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
                        // Clear existing feed
                        feedEl.innerHTML = '';
                        
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
                const icons = {'sale': '📊', 'payment': '💳', 'purchase': '📦'};
                
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
                    <div class="details">${icons[item.type]} ${details}</div>
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
    """Start the web simulator"""
    print("\n" + "="*70)
    print("🌐 CLEARVUE WEB SIMULATOR")
    print("="*70)
    print("\n🚀 Starting web server...")
    print("\n✅ Server running at:")
    print(f"   🌐 http://localhost:5001")
    print("\n💡 Open your browser and go to http://localhost:5001")
    print("="*70 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=5001)


if __name__ == "__main__":
    main()