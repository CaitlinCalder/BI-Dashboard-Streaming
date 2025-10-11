"""
ClearVue Real-Time API - TRUE Real-Time Streaming for Power BI
Uses WebSockets + Server-Sent Events (SSE) for instant data push

This API pushes data to dashboards INSTANTLY when Kafka receives messages.
No polling, no delays - true real-time updates!

Two modes:
1. WebSockets - For custom dashboards (bidirectional)
2. Server-Sent Events (SSE) - For Power BI (unidirectional push)

Author: ClearVue Analytics Team
Date: October 2025
"""

from flask import Flask, jsonify, request, Response, render_template_string
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
import json
import threading
import time
from datetime import datetime
from collections import deque
import logging

try:
    from ClearVueConfig import ClearVueConfig
except ImportError:
    print("❌ Error: ClearVueConfig.py not found")
    exit(1)

# Initialize Flask app with SocketIO
app = Flask(__name__)
app.config['SECRET_KEY'] = 'clearvue-secret-key'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('ClearVueRealTimeAPI')

# Message cache (for REST endpoints)
MESSAGE_CACHE_SIZE = 1000
message_cache = {
    'sales': deque(maxlen=MESSAGE_CACHE_SIZE),
    'payments': deque(maxlen=MESSAGE_CACHE_SIZE),
    'purchases': deque(maxlen=MESSAGE_CACHE_SIZE),
    'customers': deque(maxlen=MESSAGE_CACHE_SIZE),
    'products': deque(maxlen=MESSAGE_CACHE_SIZE),
    'suppliers': deque(maxlen=MESSAGE_CACHE_SIZE)
}

# SSE clients (for Server-Sent Events)
sse_clients = {
    'sales': [],
    'payments': [],
    'purchases': [],
    'customers': [],
    'products': [],
    'suppliers': [],
    'all': []
}

# Statistics
api_stats = {
    'start_time': datetime.now().isoformat(),
    'messages_pushed': 0,
    'websocket_connections': 0,
    'sse_connections': 0,
    'messages_consumed': 0
}

# Consumer threads
consumer_threads = []
consumers_running = False


class RealTimeKafkaConsumer:
    """
    Real-time Kafka consumer that INSTANTLY broadcasts to connected clients
    """
    
    def __init__(self, topic, channel):
        self.topic = topic
        self.channel = channel  # e.g., 'sales', 'payments'
        self.consumer = None
        self.running = False
    
    def broadcast_message(self, message_data):
        """
        Broadcast message to ALL connected clients via:
        1. WebSocket (for custom dashboards)
        2. SSE (for Power BI/web apps)
        3. Cache (for REST API fallback)
        """
        
        formatted_message = {
            'timestamp': datetime.now().isoformat(),
            'channel': self.channel,
            'data': message_data
        }
        
        # 1. Broadcast via WebSocket to connected clients
        try:
            socketio.emit(
                'new_message',
                formatted_message,
                namespace=f'/{self.channel}',
                broadcast=True
            )
            api_stats['messages_pushed'] += 1
            logger.debug(f"📡 WebSocket broadcast: {self.channel}")
        except Exception as e:
            logger.error(f"WebSocket broadcast error: {e}")
        
        # 2. Send to SSE clients (for Power BI)
        try:
            self._send_to_sse_clients(formatted_message)
        except Exception as e:
            logger.error(f"SSE broadcast error: {e}")
        
        # 3. Add to cache (for REST API)
        message_cache[self.channel].append(formatted_message)
    
    def _send_to_sse_clients(self, message):
        """Send message to all SSE clients subscribed to this channel"""
        # Send to channel-specific clients
        for client_queue in sse_clients.get(self.channel, []):
            try:
                client_queue.put(message)
            except:
                pass
        
        # Send to 'all' subscribers
        for client_queue in sse_clients.get('all', []):
            try:
                client_queue.put(message)
            except:
                pass
    
    def start(self):
        """Start consuming and broadcasting messages"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=ClearVueConfig.get_kafka_servers(),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=f'clearvue_realtime_{self.channel}',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            
            self.running = True
            logger.info(f"✅ Started real-time consumer: {self.topic}")
            
            while self.running:
                try:
                    messages = self.consumer.poll(timeout_ms=100)  # Poll very frequently!
                    
                    for topic_partition, records in messages.items():
                        for record in records:
                            # INSTANTLY broadcast to all connected clients!
                            self.broadcast_message(record.value)
                            
                            api_stats['messages_consumed'] += 1
                            
                            logger.info(
                                f"⚡ Real-time broadcast: {self.channel} | "
                                f"Pushed: {api_stats['messages_pushed']}"
                            )
                
                except Exception as e:
                    logger.error(f"Error consuming from {self.topic}: {e}")
                    time.sleep(1)
        
        except Exception as e:
            logger.error(f"Failed to start consumer for {self.topic}: {e}")
        
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info(f"Stopped consumer: {self.topic}")
    
    def stop(self):
        """Stop consuming"""
        self.running = False


def start_realtime_consumers():
    """Start real-time Kafka consumers"""
    global consumers_running, consumer_threads
    
    if consumers_running:
        return
    
    logger.info("🚀 Starting real-time Kafka consumers...")
    
    topics_config = [
        ('clearvue.sales', 'sales'),
        ('clearvue.payments', 'payments'),
        ('clearvue.purchases', 'purchases'),
        ('clearvue.customers', 'customers'),
        ('clearvue.products', 'products'),
        ('clearvue.suppliers', 'suppliers')
    ]
    
    for topic, channel in topics_config:
        consumer = RealTimeKafkaConsumer(topic, channel)
        thread = threading.Thread(target=consumer.start, daemon=True)
        thread.start()
        consumer_threads.append({'thread': thread, 'consumer': consumer})
    
    consumers_running = True
    logger.info(f"✅ Started {len(consumer_threads)} real-time consumers")


# ============================================================================
# WebSocket Endpoints (for Custom Dashboards)
# ============================================================================

@socketio.on('connect', namespace='/sales')
def handle_sales_connect():
    """Client connected to sales WebSocket"""
    api_stats['websocket_connections'] += 1
    logger.info(f"🔌 WebSocket connected: sales | Total: {api_stats['websocket_connections']}")
    emit('connection_response', {'status': 'connected', 'channel': 'sales'})


@socketio.on('connect', namespace='/payments')
def handle_payments_connect():
    """Client connected to payments WebSocket"""
    api_stats['websocket_connections'] += 1
    logger.info(f"🔌 WebSocket connected: payments | Total: {api_stats['websocket_connections']}")
    emit('connection_response', {'status': 'connected', 'channel': 'payments'})


@socketio.on('disconnect', namespace='/sales')
def handle_sales_disconnect():
    """Client disconnected from sales WebSocket"""
    api_stats['websocket_connections'] -= 1
    logger.info(f"❌ WebSocket disconnected: sales")


# ============================================================================
# Server-Sent Events (SSE) - For Power BI Real-Time
# ============================================================================

@app.route('/stream/sales')
def stream_sales():
    """SSE endpoint for real-time sales - INSTANT PUSH to Power BI!"""
    from queue import Queue
    
    client_queue = Queue(maxsize=100)
    sse_clients['sales'].append(client_queue)
    api_stats['sse_connections'] += 1
    
    logger.info(f"📡 SSE client connected: sales | Total: {api_stats['sse_connections']}")
    
    def generate():
        """Generate Server-Sent Events stream"""
        try:
            # Send initial connection confirmation
            yield f"data: {json.dumps({'status': 'connected', 'channel': 'sales'})}\n\n"
            
            # Stream messages as they arrive
            while True:
                try:
                    # Wait for new message (blocking)
                    message = client_queue.get(timeout=30)
                    
                    # Send message to client
                    yield f"data: {json.dumps(message)}\n\n"
                    
                except:
                    # Send keepalive every 30 seconds
                    yield f": keepalive\n\n"
        
        finally:
            # Clean up when client disconnects
            if client_queue in sse_clients['sales']:
                sse_clients['sales'].remove(client_queue)
            api_stats['sse_connections'] -= 1
            logger.info(f"❌ SSE client disconnected: sales")
    
    return Response(generate(), mimetype='text/event-stream')


@app.route('/stream/payments')
def stream_payments():
    """SSE endpoint for real-time payments"""
    from queue import Queue
    
    client_queue = Queue(maxsize=100)
    sse_clients['payments'].append(client_queue)
    api_stats['sse_connections'] += 1
    
    def generate():
        try:
            yield f"data: {json.dumps({'status': 'connected', 'channel': 'payments'})}\n\n"
            
            while True:
                try:
                    message = client_queue.get(timeout=30)
                    yield f"data: {json.dumps(message)}\n\n"
                except:
                    yield f": keepalive\n\n"
        finally:
            if client_queue in sse_clients['payments']:
                sse_clients['payments'].remove(client_queue)
            api_stats['sse_connections'] -= 1
    
    return Response(generate(), mimetype='text/event-stream')


@app.route('/stream/all')
def stream_all():
    """SSE endpoint for ALL real-time data - ALL channels combined!"""
    from queue import Queue
    
    client_queue = Queue(maxsize=500)
    sse_clients['all'].append(client_queue)
    api_stats['sse_connections'] += 1
    
    logger.info(f"📡 SSE client connected: ALL channels | Total: {api_stats['sse_connections']}")
    
    def generate():
        try:
            yield f"data: {json.dumps({'status': 'connected', 'channel': 'all'})}\n\n"
            
            while True:
                try:
                    message = client_queue.get(timeout=30)
                    yield f"data: {json.dumps(message)}\n\n"
                except:
                    yield f": keepalive\n\n"
        finally:
            if client_queue in sse_clients['all']:
                sse_clients['all'].remove(client_queue)
            api_stats['sse_connections'] -= 1
    
    return Response(generate(), mimetype='text/event-stream')


# ============================================================================
# REST API Endpoints (Fallback / Historical Data)
# ============================================================================

@app.route('/')
def home():
    """API home page"""
    return jsonify({
        'name': 'ClearVue Real-Time API',
        'version': '2.0',
        'description': 'INSTANT real-time data streaming for Power BI',
        'real_time_endpoints': {
            'sse_sales': '/stream/sales (Server-Sent Events)',
            'sse_payments': '/stream/payments',
            'sse_all': '/stream/all (All channels combined)',
            'websocket_sales': 'ws://localhost:5000/sales',
            'websocket_payments': 'ws://localhost:5000/payments'
        },
        'rest_endpoints': {
            '/api/sales': 'Get recent sales (REST)',
            '/api/payments': 'Get recent payments (REST)',
            '/api/stats': 'Get API statistics',
            '/api/health': 'Health check'
        },
        'demo': '/demo (Live dashboard demo)'
    })


@app.route('/api/health')
def health():
    """Health check"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'websocket_connections': api_stats['websocket_connections'],
        'sse_connections': api_stats['sse_connections'],
        'messages_pushed': api_stats['messages_pushed']
    })


@app.route('/api/stats')
def stats():
    """API statistics"""
    uptime = str(datetime.now() - datetime.fromisoformat(api_stats['start_time'])).split('.')[0]
    
    return jsonify({
        **api_stats,
        'uptime': uptime,
        'cache_sizes': {k: len(v) for k, v in message_cache.items()},
        'active_sse_clients': {k: len(v) for k, v in sse_clients.items()}
    })


@app.route('/api/sales')
def get_sales():
    """Get recent sales (REST fallback)"""
    limit = request.args.get('limit', 100, type=int)
    messages = list(message_cache['sales'])[-limit:]
    
    return jsonify({
        'count': len(messages),
        'data': messages,
        'note': 'For real-time updates, use /stream/sales (SSE)'
    })


@app.route('/api/payments')
def get_payments():
    """Get recent payments (REST fallback)"""
    limit = request.args.get('limit', 100, type=int)
    messages = list(message_cache['payments'])[-limit:]
    
    return jsonify({
        'count': len(messages),
        'data': messages,
        'note': 'For real-time updates, use /stream/payments (SSE)'
    })


# ============================================================================
# Demo Dashboard (HTML with live updates)
# ============================================================================

@app.route('/demo')
def demo():
    """Live demo dashboard showing real-time updates"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ClearVue Real-Time Dashboard Demo</title>
        <style>
            body { font-family: Arial; margin: 20px; background: #1e1e1e; color: #fff; }
            h1 { color: #4CAF50; }
            .container { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
            .panel { background: #2d2d2d; padding: 20px; border-radius: 8px; }
            .message { padding: 10px; margin: 5px 0; background: #3d3d3d; border-left: 3px solid #4CAF50; }
            .timestamp { color: #888; font-size: 12px; }
            .amount { color: #4CAF50; font-weight: bold; }
            #status { padding: 10px; background: #4CAF50; color: white; border-radius: 5px; }
            #status.connected { background: #4CAF50; }
            #status.disconnected { background: #f44336; }
        </style>
    </head>
    <body>
        <h1>🚀 ClearVue Real-Time Dashboard Demo</h1>
        <div id="status">⏳ Connecting...</div>
        <p>This dashboard updates INSTANTLY when new data arrives from Kafka!</p>
        
        <div class="container">
            <div class="panel">
                <h2>💰 Real-Time Sales</h2>
                <div id="sales"></div>
            </div>
            <div class="panel">
                <h2>💳 Real-Time Payments</h2>
                <div id="payments"></div>
            </div>
        </div>
        
        <script>
            // Connect to Server-Sent Events
            const salesSource = new EventSource('/stream/sales');
            const paymentsSource = new EventSource('/stream/payments');
            
            const statusDiv = document.getElementById('status');
            const salesDiv = document.getElementById('sales');
            const paymentsDiv = document.getElementById('payments');
            
            salesSource.onopen = () => {
                statusDiv.textContent = '✅ Connected - Streaming live data!';
                statusDiv.className = 'connected';
            };
            
            salesSource.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.status === 'connected') return;
                
                const msg = data.data.business_context || {};
                const html = `
                    <div class="message">
                        <div class="timestamp">${data.timestamp}</div>
                        <div>Customer: ${msg.customer_id || 'N/A'}</div>
                        <div class="amount">Amount: R${(msg.total_amount || 0).toFixed(2)}</div>
                        <div>Profit: R${(msg.total_profit || 0).toFixed(2)}</div>
                    </div>
                `;
                salesDiv.innerHTML = html + salesDiv.innerHTML;
                
                // Keep only last 10
                const messages = salesDiv.getElementsByClassName('message');
                while (messages.length > 10) {
                    salesDiv.removeChild(messages[messages.length - 1]);
                }
            };
            
            paymentsSource.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.status === 'connected') return;
                
                const msg = data.data.business_context || {};
                const html = `
                    <div class="message">
                        <div class="timestamp">${data.timestamp}</div>
                        <div>Customer: ${msg.customer_id || 'N/A'}</div>
                        <div class="amount">Payment: R${(msg.total_payment || 0).toFixed(2)}</div>
                        <div>Discount: R${(msg.total_discount || 0).toFixed(2)}</div>
                    </div>
                `;
                paymentsDiv.innerHTML = html + paymentsDiv.innerHTML;
                
                const messages = paymentsDiv.getElementsByClassName('message');
                while (messages.length > 10) {
                    paymentsDiv.removeChild(messages[messages.length - 1]);
                }
            };
            
            salesSource.onerror = () => {
                statusDiv.textContent = '❌ Disconnected - Retrying...';
                statusDiv.className = 'disconnected';
            };
        </script>
    </body>
    </html>
    """
    return render_template_string(html)


def main():
    """Start the real-time API server"""
    print("\n" + "="*70)
    print("🚀 CLEARVUE REAL-TIME API SERVER")
    print("="*70)
    print("\n⚡ TRUE REAL-TIME MODE")
    print("   Messages are pushed INSTANTLY to connected clients!")
    print("   No polling delays - sub-second latency!")
    print("\n📡 Starting Kafka real-time consumers...")
    
    # Start Kafka consumers
    start_realtime_consumers()
    time.sleep(2)
    
    print("\n✅ Real-time consumers active")
    print("\n🌐 Starting Flask + SocketIO server...")
    print("="*70)
    print("\n📋 Endpoints:")
    print(f"   Home:              http://localhost:5000/")
    print(f"   Live Demo:         http://localhost:5000/demo")
    print(f"   Health:            http://localhost:5000/api/health")
    print(f"   Stats:             http://localhost:5000/api/stats")
    print("\n📡 Real-Time Streaming (SSE - for Power BI):")
    print(f"   Sales Stream:      http://localhost:5000/stream/sales")
    print(f"   Payments Stream:   http://localhost:5000/stream/payments")
    print(f"   All Data Stream:   http://localhost:5000/stream/all")
    print("\n🔌 WebSocket (for custom dashboards):")
    print(f"   Sales WS:          ws://localhost:5000/sales")
    print(f"   Payments WS:       ws://localhost:5000/payments")
    print("="*70 + "\n")
    print("💡 TIP: Open http://localhost:5000/demo to see live updates!")
    print("="*70 + "\n")
    print("⚠️  IMPORTANT: Do NOT use uvicorn to run this!")
    print("   This is a WSGI app (Flask), not ASGI")
    print("   Run with: python ClearVue_RealTime_API.py")
    print("="*70 + "\n")
    
    try:
        # Start SocketIO server (uses Flask's built-in server)
        socketio.run(
            app,
            host='0.0.0.0',
            port=5000,
            debug=False,
            use_reloader=False,
            log_output=True
        )
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down...")
    finally:
        print("✅ API server stopped\n")


if __name__ == '__main__':
    main()