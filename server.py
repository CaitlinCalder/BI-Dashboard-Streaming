"""
ClearVue FastAPI Streaming Server
Real-time API and Web Dashboard for Power BI Integration

Features:
- REST API endpoints for Power BI
- WebSocket streaming for web dashboard
- Real-time metrics aggregation
- Historical data caching
- CORS enabled for Power BI custom visuals

Author: ClearVue Streaming Team
Date: October 2025
"""

import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from collections import defaultdict, deque

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import threading

# Import config
try:
    from ClearVueConfig import ClearVueConfig
except ImportError:
    print("Error: config.py not found")
    exit(1)

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ClearVueAPI")

# FastAPI app
app = FastAPI(
    title="ClearVue Streaming API",
    description="Real-time streaming data API for Power BI dashboards",
    version="2.0"
)

# CORS - Allow Power BI and web dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify Power BI domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class StreamingDataManager:
    """Manages real-time data streaming and caching"""
    
    def __init__(self):
        # Real-time data cache (last 1000 messages)
        self.recent_messages = deque(maxlen=1000)
        
        # Aggregated metrics
        self.metrics = {
            'total_sales': 0,
            'total_payments': 0,
            'total_profit': 0,
            'transaction_count': 0,
            'last_update': None
        }
        
        # Metrics by collection
        self.collection_metrics = defaultdict(lambda: {
            'count': 0,
            'total_amount': 0,
            'last_update': None
        })
        
        # WebSocket connections
        self.active_connections: List[WebSocket] = []
        
        # Kafka consumer
        self.consumer = None
        self.is_consuming = False
        
        # Statistics
        self.stats = {
            'messages_processed': 0,
            'api_requests': 0,
            'websocket_broadcasts': 0,
            'start_time': datetime.now()
        }
    
    def start_kafka_consumer(self):
        """Start Kafka consumer in background thread"""
        
        def consume_kafka():
            logger.info("Starting Kafka consumer thread...")
            
            try:
                # Subscribe to all ClearVue topics
                topics = [config['topic'] for config in ClearVueConfig.KAFKA_TOPICS.values()]
                
                self.consumer = KafkaConsumer(
                    *topics,
                    bootstrap_servers=ClearVueConfig.get_kafka_servers(),
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='clearvue_api_consumer'
                )
                
                logger.info(f"Kafka consumer connected to {len(topics)} topics")
                self.is_consuming = True
                
                for message in self.consumer:
                    if not self.is_consuming:
                        break
                    
                    self.process_message(message.value)
            
            except Exception as e:
                logger.error(f"Kafka consumer error: {e}")
                self.is_consuming = False
        
        # Start in background thread
        thread = threading.Thread(target=consume_kafka, daemon=True)
        thread.start()
        logger.info("Kafka consumer thread started")
    
    def process_message(self, data: Dict):
        """Process incoming Kafka message"""
        
        # Add to recent messages
        self.recent_messages.append({
            'timestamp': datetime.now().isoformat(),
            'data': data
        })
        
        # Update metrics
        collection = data.get('collection')
        ctx = data.get('business_context', {})
        
        self.collection_metrics[collection]['count'] += 1
        self.collection_metrics[collection]['last_update'] = datetime.now().isoformat()
        
        # Update collection-specific metrics
        if collection == 'sales':
            amount = ctx.get('total_amount', 0)
            profit = ctx.get('total_profit', 0)
            
            self.metrics['total_sales'] += amount
            self.metrics['total_profit'] += profit
            self.collection_metrics[collection]['total_amount'] += amount
        
        elif collection == 'payments':
            payment = ctx.get('total_payment', 0)
            self.metrics['total_payments'] += payment
            self.collection_metrics[collection]['total_amount'] += payment
        
        elif collection == 'purchases':
            cost = ctx.get('total_cost', 0)
            self.collection_metrics[collection]['total_amount'] += cost
        
        self.metrics['transaction_count'] += 1
        self.metrics['last_update'] = datetime.now().isoformat()
        self.stats['messages_processed'] += 1
        
        # Broadcast to WebSocket clients
        asyncio.run(self.broadcast_to_websockets(data))
    
    async def broadcast_to_websockets(self, data: Dict):
        """Broadcast message to all connected WebSocket clients"""
        if not self.active_connections:
            return
        
        message = json.dumps({
            'type': 'realtime_update',
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
        
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
                self.stats['websocket_broadcasts'] += 1
            except:
                disconnected.append(connection)
        
        # Remove disconnected clients
        for conn in disconnected:
            self.active_connections.remove(conn)
    
    def get_metrics_summary(self) -> Dict:
        """Get aggregated metrics summary"""
        return {
            'metrics': self.metrics,
            'collection_metrics': dict(self.collection_metrics),
            'stats': {
                **self.stats,
                'uptime_seconds': (datetime.now() - self.stats['start_time']).seconds,
                'active_websockets': len(self.active_connections)
            }
        }


# Global data manager
data_manager = StreamingDataManager()


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info("ClearVue Streaming API starting...")
    data_manager.start_kafka_consumer()
    logger.info("API ready!")


@app.get("/")
async def root():
    """Root endpoint - API info"""
    return {
        "service": "ClearVue Streaming API",
        "version": "2.0",
        "status": "running",
        "kafka_consuming": data_manager.is_consuming,
        "endpoints": {
            "metrics": "/api/metrics",
            "recent": "/api/recent",
            "sales": "/api/sales",
            "payments": "/api/payments",
            "dashboard": "/dashboard",
            "websocket": "/ws"
        }
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy" if data_manager.is_consuming else "degraded",
        "kafka_connected": data_manager.is_consuming,
        "messages_processed": data_manager.stats['messages_processed'],
        "active_websockets": len(data_manager.active_connections),
        "uptime_seconds": (datetime.now() - data_manager.stats['start_time']).seconds
    }


@app.get("/api/metrics")
async def get_metrics():
    """
    Get aggregated metrics summary
    Perfect for Power BI card visuals
    """
    data_manager.stats['api_requests'] += 1
    return data_manager.get_metrics_summary()


@app.get("/api/recent")
async def get_recent_messages(
    limit: int = Query(100, ge=1, le=1000),
    collection: Optional[str] = None
):
    """
    Get recent messages
    
    Args:
        limit: Number of messages to return (max 1000)
        collection: Filter by collection (sales, payments, etc.)
    """
    data_manager.stats['api_requests'] += 1
    
    messages = list(data_manager.recent_messages)
    
    # Filter by collection if specified
    if collection:
        messages = [m for m in messages if m['data'].get('collection') == collection]
    
    # Limit results
    messages = messages[-limit:]
    
    return {
        "total": len(messages),
        "limit": limit,
        "collection": collection,
        "messages": messages
    }


@app.get("/api/sales")
async def get_sales_data(
    limit: int = Query(50, ge=1, le=500),
    financial_year: Optional[int] = None,
    financial_quarter: Optional[str] = None
):
    """
    Get sales transactions for Power BI
    
    Args:
        limit: Number of records
        financial_year: Filter by year (e.g., 2025)
        financial_quarter: Filter by quarter (e.g., Q4)
    """
    data_manager.stats['api_requests'] += 1
    
    # Filter sales messages
    sales_messages = [
        m for m in data_manager.recent_messages
        if m['data'].get('collection') == 'sales'
    ]
    
    # Apply filters
    if financial_year:
        sales_messages = [
            m for m in sales_messages
            if m['data'].get('business_context', {}).get('financial_year') == financial_year
        ]
    
    if financial_quarter:
        sales_messages = [
            m for m in sales_messages
            if m['data'].get('business_context', {}).get('financial_quarter') == financial_quarter
        ]
    
    # Limit and format for Power BI
    sales_messages = sales_messages[-limit:]
    
    # Transform to flat structure for Power BI
    sales_data = []
    for msg in sales_messages:
        data = msg['data']
        ctx = data.get('business_context', {})
        
        sales_data.append({
            'event_id': data.get('event_id'),
            'timestamp': data.get('timestamp'),
            'doc_number': ctx.get('doc_number'),
            'customer_id': ctx.get('customer_id'),
            'rep_id': ctx.get('rep_id'),
            'rep_name': ctx.get('rep_name'),
            'trans_type': ctx.get('trans_type'),
            'total_amount': ctx.get('total_amount', 0),
            'total_cost': ctx.get('total_cost', 0),
            'total_profit': ctx.get('total_profit', 0),
            'profit_margin_pct': ctx.get('profit_margin_pct', 0),
            'line_count': ctx.get('line_count', 0),
            'fin_period': ctx.get('fin_period'),
            'financial_year': ctx.get('financial_year'),
            'financial_quarter': ctx.get('financial_quarter'),
            'financial_month': ctx.get('financial_month')
        })
    
    return {
        "total": len(sales_data),
        "data": sales_data
    }


@app.get("/api/payments")
async def get_payments_data(
    limit: int = Query(50, ge=1, le=500),
    financial_year: Optional[int] = None
):
    """Get payment transactions for Power BI"""
    data_manager.stats['api_requests'] += 1
    
    # Filter payment messages
    payment_messages = [
        m for m in data_manager.recent_messages
        if m['data'].get('collection') == 'payments'
    ]
    
    # Apply filters
    if financial_year:
        payment_messages = [
            m for m in payment_messages
            if m['data'].get('business_context', {}).get('financial_year') == financial_year
        ]
    
    payment_messages = payment_messages[-limit:]
    
    # Transform to flat structure
    payment_data = []
    for msg in payment_messages:
        data = msg['data']
        ctx = data.get('business_context', {})
        
        payment_data.append({
            'event_id': data.get('event_id'),
            'timestamp': data.get('timestamp'),
            'customer_id': ctx.get('customer_id'),
            'deposit_ref': ctx.get('deposit_ref'),
            'total_bank_amount': ctx.get('total_bank_amount', 0),
            'total_discount': ctx.get('total_discount', 0),
            'total_payment': ctx.get('total_payment', 0),
            'discount_pct': ctx.get('discount_pct', 0),
            'fin_period': ctx.get('fin_period'),
            'financial_year': ctx.get('financial_year'),
            'financial_quarter': ctx.get('financial_quarter')
        })
    
    return {
        "total": len(payment_data),
        "data": payment_data
    }


@app.get("/api/collection/{collection_name}")
async def get_collection_data(
    collection_name: str,
    limit: int = Query(100, ge=1, le=500)
):
    """
    Get data for specific collection
    Generic endpoint for any collection
    """
    data_manager.stats['api_requests'] += 1
    
    messages = [
        m for m in data_manager.recent_messages
        if m['data'].get('collection') == collection_name
    ]
    
    return {
        "collection": collection_name,
        "total": len(messages),
        "messages": messages[-limit:]
    }


# ============================================================================
# WEBSOCKET ENDPOINT
# ============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time streaming
    Web dashboard connects here for live updates
    """
    await websocket.accept()
    data_manager.active_connections.append(websocket)
    
    logger.info(f"WebSocket client connected. Total: {len(data_manager.active_connections)}")
    
    try:
        # Send initial metrics
        await websocket.send_json({
            'type': 'connection_established',
            'metrics': data_manager.get_metrics_summary()
        })
        
        # Keep connection alive
        while True:
            # Wait for messages from client (ping/pong)
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                
                # Echo back for ping/pong
                if data == "ping":
                    await websocket.send_text("pong")
            
            except asyncio.TimeoutError:
                # Send heartbeat
                await websocket.send_json({
                    'type': 'heartbeat',
                    'timestamp': datetime.now().isoformat()
                })
    
    except WebSocketDisconnect:
        data_manager.active_connections.remove(websocket)
        logger.info(f"WebSocket client disconnected. Remaining: {len(data_manager.active_connections)}")


# ============================================================================
# WEB DASHBOARD
# ============================================================================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Real-time web dashboard"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ClearVue Real-Time Dashboard</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: #fff;
                padding: 20px;
            }
            .container { max-width: 1400px; margin: 0 auto; }
            h1 {
                text-align: center;
                margin-bottom: 30px;
                font-size: 2.5em;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            }
            .status {
                text-align: center;
                margin-bottom: 20px;
                padding: 10px;
                background: rgba(255,255,255,0.1);
                border-radius: 10px;
            }
            .status.connected { background: rgba(16,185,129,0.3); }
            .status.disconnected { background: rgba(239,68,68,0.3); }
            
            .metrics {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            .metric-card {
                background: rgba(255,255,255,0.15);
                backdrop-filter: blur(10px);
                border-radius: 15px;
                padding: 25px;
                box-shadow: 0 8px 32px rgba(0,0,0,0.1);
                transition: transform 0.3s;
            }
            .metric-card:hover { transform: translateY(-5px); }
            .metric-label {
                font-size: 0.9em;
                opacity: 0.9;
                margin-bottom: 10px;
            }
            .metric-value {
                font-size: 2.5em;
                font-weight: bold;
                margin-bottom: 5px;
            }
            .metric-change {
                font-size: 0.85em;
                opacity: 0.8;
            }
            
            .transactions {
                background: rgba(255,255,255,0.15);
                backdrop-filter: blur(10px);
                border-radius: 15px;
                padding: 25px;
                max-height: 600px;
                overflow-y: auto;
            }
            .transaction-item {
                background: rgba(255,255,255,0.1);
                padding: 15px;
                border-radius: 10px;
                margin-bottom: 10px;
                border-left: 4px solid #10b981;
                animation: slideIn 0.3s ease-out;
            }
            @keyframes slideIn {
                from {
                    opacity: 0;
                    transform: translateX(-20px);
                }
                to {
                    opacity: 1;
                    transform: translateX(0);
                }
            }
            .transaction-header {
                display: flex;
                justify-content: space-between;
                margin-bottom: 8px;
                font-weight: bold;
            }
            .transaction-details {
                font-size: 0.9em;
                opacity: 0.9;
            }
            .priority-HIGH { border-left-color: #ef4444; }
            .priority-MEDIUM { border-left-color: #f59e0b; }
            .priority-LOW { border-left-color: #10b981; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>ClearVue Real-Time Dashboard</h1>
            
            <div id="status" class="status disconnected">
                <strong>Status:</strong> <span id="statusText">Connecting...</span>
            </div>
            
            <div class="metrics">
                <div class="metric-card">
                    <div class="metric-label"> Total Sales</div>
                    <div class="metric-value" id="totalSales">R0</div>
                    <div class="metric-change">This session</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label"> Total Payments</div>
                    <div class="metric-value" id="totalPayments">R0</div>
                    <div class="metric-change">This session</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label"> Total Profit</div>
                    <div class="metric-value" id="totalProfit">R0</div>
                    <div class="metric-change">This session</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label"> Transactions</div>
                    <div class="metric-value" id="transactionCount">0</div>
                    <div class="metric-change">Live count</div>
                </div>
            </div>
            
            <div class="transactions">
                <h2 style="margin-bottom: 20px;"> Live Transactions</h2>
                <div id="transactionsList"></div>
            </div>
        </div>
        
        <script>
            let ws;
            const maxTransactions = 50;
            
            function connect() {
                ws = new WebSocket(`ws://${window.location.host}/ws`);
                
                ws.onopen = () => {
                    console.log('WebSocket connected');
                    document.getElementById('status').className = 'status connected';
                    document.getElementById('statusText').textContent = ' Connected';
                };
                
                ws.onmessage = (event) => {
                    const message = JSON.parse(event.data);
                    
                    if (message.type === 'connection_established') {
                        updateMetrics(message.metrics.metrics);
                    }
                    else if (message.type === 'realtime_update') {
                        handleRealtimeUpdate(message.data);
                    }
                };
                
                ws.onerror = (error) => {
                    console.error('WebSocket error:', error);
                };
                
                ws.onclose = () => {
                    console.log('WebSocket disconnected');
                    document.getElementById('status').className = 'status disconnected';
                    document.getElementById('statusText').textContent = ' Disconnected - Reconnecting...';
                    setTimeout(connect, 3000);
                };
                
                // Send ping every 25 seconds
                setInterval(() => {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send('ping');
                    }
                }, 25000);
            }
            
            function updateMetrics(metrics) {
                document.getElementById('totalSales').textContent = 
                    'R' + metrics.total_sales.toLocaleString('en-ZA', {minimumFractionDigits: 2});
                document.getElementById('totalPayments').textContent = 
                    'R' + metrics.total_payments.toLocaleString('en-ZA', {minimumFractionDigits: 2});
                document.getElementById('totalProfit').textContent = 
                    'R' + metrics.total_profit.toLocaleString('en-ZA', {minimumFractionDigits: 2});
                document.getElementById('transactionCount').textContent = 
                    metrics.transaction_count.toLocaleString('en-ZA');
            }
            
            function handleRealtimeUpdate(data) {
                const ctx = data.business_context || {};
                const collection = data.collection;
                const operation = data.operation;
                const priority = data.priority;
                
                // Update metrics
                fetch('/api/metrics')
                    .then(r => r.json())
                    .then(d => updateMetrics(d.metrics));
                
                // Add to transaction list
                const list = document.getElementById('transactionsList');
                const item = document.createElement('div');
                item.className = `transaction-item priority-${priority}`;
                
                let icon = '';
                let title = collection.toUpperCase();
                let details = '';
                
                if (collection === 'sales') {
                    icon = '';
                    details = `Customer: ${ctx.customer_id} | Amount: R${(ctx.total_amount || 0).toLocaleString('en-ZA', {minimumFractionDigits: 2})}`;
                } else if (collection === 'payments') {
                    icon = '';
                    details = `Customer: ${ctx.customer_id} | Payment: R${(ctx.total_payment || 0).toLocaleString('en-ZA', {minimumFractionDigits: 2})}`;
                } else if (collection === 'purchases') {
                    icon = '';
                    details = `Supplier: ${ctx.supplier_id} | Cost: R${(ctx.total_cost || 0).toLocaleString('en-ZA', {minimumFractionDigits: 2})}`;
                } else {
                    details = `${operation.toUpperCase()}`;
                }
                
                item.innerHTML = `
                    <div class="transaction-header">
                        <span>${icon} ${title}</span>
                        <span style="font-size: 0.85em; opacity: 0.8;">${new Date().toLocaleTimeString()}</span>
                    </div>
                    <div class="transaction-details">${details}</div>
                `;
                
                list.insertBefore(item, list.firstChild);
                
                // Keep only last N transactions
                while (list.children.length > maxTransactions) {
                    list.removeChild(list.lastChild);
                }
            }
            
            // Connect on load
            connect();
        </script>
    </body>
    </html>
    """


# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print(" CLEARVUE FASTAPI STREAMING SERVER")
    print("="*70)
    print("\n Starting server...")
    print("   API: http://localhost:8000")
    print("   Docs: http://localhost:8000/docs")
    print("   Dashboard: http://localhost:8000/dashboard")
    print("\n" + "="*70 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )