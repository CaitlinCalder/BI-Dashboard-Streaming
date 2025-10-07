ClearVue Streaming Pipeline - Quick Start Guide
🎯 Choose Your MongoDB Setup
You have TWO options for where to store your data:

Option 1: MongoDB Atlas (Cloud) ☁️ RECOMMENDED
✅ Already configured
✅ No local setup needed
✅ Works anywhere with internet
✅ Free tier available
Option 2: Local Docker MongoDB 🐳
✅ Fully offline
✅ Complete control
✅ No cloud dependencies
⚠️ Requires Docker setup
🚀 Complete Setup (5 Minutes)
Step 1: Automated Setup
bash
# Make script executable
chmod +x setup_complete.sh

# Run setup wizard
./setup_complete.sh

# It will ask:
# "Where do you want to store your MongoDB data?"
# 1) MongoDB Atlas (Cloud) - Recommended
# 2) Local Docker MongoDB
Choose option 1 for Atlas (easiest) or option 2 for local Docker

The script will:

✅ Install Python dependencies
✅ Create Docker volumes & network
✅ Start Kafka & Zookeeper
✅ Start MongoDB (if local mode)
✅ Create .env configuration file
Step 2: Load Your Data
bash
# Place your 17 CSV files in ./cleaned_data/
mkdir -p cleaned_data
# Copy: Trans_Types.csv, Suppliers.csv, etc.

# Transform and load (ONE-TIME operation)
python transform_and_load.py ./cleaned_data

# Expected output:
# ✅ Files loaded: 17/17
# ✅ Collections created: 6/6
# ✅ Documents inserted: X,XXX
Step 3: Start Streaming
bash
# Terminal 1: Start the pipeline
python clearvue_streaming_pipeline_final.py
# Choose: 1 (ALL collections)

# You should see:
# 🟢 PIPELINE IS RUNNING!
# 👁️ Watching: sales, payments, purchases...
Step 4: Test It
bash
# Terminal 2: Generate test transactions
python transaction_simulator_enhanced.py
# Choose: 1 (Mixed burst)

# Watch Terminal 1 for:
# ⚡ SALES: insert detected
# ⚡ PAYMENT: insert detected
Step 5: Verify
Open in browser:

Kafka UI: http://localhost:8080 (see messages)
Mongo Express: http://localhost:8081 (see data)
📊 What Gets Created
6 MongoDB Collections:
customers - Customer master with embedded age_analysis
suppliers - Supplier information
products - Products with embedded brand/category/style
sales - Sales transactions with embedded lines
payments - Payment transactions with embedded lines
purchases - Purchase orders with embedded lines
6 Kafka Topics:
clearvue.sales.realtime - Real-time sales
clearvue.payments.realtime - Real-time payments
clearvue.purchases.realtime - Real-time purchases
clearvue.customers.changes - Customer updates
clearvue.products.changes - Product updates
clearvue.suppliers.changes - Supplier updates
🎮 Usage Examples
Generate Different Transaction Types
bash
python transaction_simulator_enhanced.py

# Options:
1. Mixed burst (20 transactions)      # ← Best for testing
2. Continuous (10 tx/min)             # ← For demo
3. Heavy load (30 tx/min)             # ← Stress test
4. Sales only
5. Payments only
6. Master data updates only
Test Kafka Messages
bash
python kafka_consumer_test.py

# Choose: 7 (ALL topics)
# Then run simulator in another terminal
# Watch formatted messages appear
Switch MongoDB Mode
bash
# To use Atlas:
export MONGODB_MODE=atlas
python transform_and_load.py ./cleaned_data

# To use Local Docker:
export MONGODB_MODE=local
python transform_and_load.py ./cleaned_data
🔍 Verify Everything Works
Check 1: Docker Services
bash
docker-compose ps

# Should show (healthy):
# clearvue_kafka
# clearvue_zookeeper
# clearvue_mongodb (if local mode)
Check 2: MongoDB Data
bash
# If using Atlas:
mongosh "mongodb+srv://Tyra:1234@novacluster.1re1a4e.mongodb.net/Nova_Analytics"

# If using Local:
mongosh "mongodb://admin:clearvue123@localhost:27017/clearvue_bi"

# Then:
show collections
db.sales.countDocuments()
db.payments.countDocuments()
Check 3: Kafka Topics
Visit: http://localhost:8080

Click "Topics"
You should see 6 clearvue topics
Click one to see messages (after running simulator)
Check 4: Pipeline Logs
bash
tail -f streaming_pipeline.log

# Should show:
# INFO - Started change stream: sales
# INFO - Started change stream: payments
# ...
🐛 Quick Troubleshooting
Problem: "Connection failed"
bash
# Check Docker is running:
docker ps

# Restart services:
docker-compose restart

# Check logs:
docker-compose logs kafka
docker-compose logs mongodb
Problem: "No collections found"
bash
# Did you run the transformation?
python transform_and_load.py ./cleaned_data

# Check if files exist:
ls -la cleaned_data/
Problem: "Pipeline not detecting changes"
bash
# Make sure pipeline is running:
ps aux | grep streaming_pipeline

# Generate a test transaction:
python transaction_simulator_enhanced.py
# Choose: 4 (Sales only)

# Check Kafka UI immediately:
# http://localhost:8080
Problem: "Kafka topics empty"
bash
# Make sure pipeline is running FIRST
python clearvue_streaming_pipeline_final.py

# THEN generate transactions
python transaction_simulator_enhanced.py
🎬 For Your Demo Video
bash
# Terminal 1: Start pipeline
python clearvue_streaming_pipeline_final.py

# Terminal 2: Run simulator
python transaction_simulator_enhanced.py
# Choose: 2 (Continuous)

# Show in video:
1. Terminal 1 - Pipeline detecting changes
2. Terminal 2 - Simulator generating transactions
3. Browser - Kafka UI showing messages (http://localhost:8080)
4. Browser - MongoDB data (http://localhost:8081)
5. Power BI - Dashboard updating (if connected)
📁 Project File Structure
clearvue-streaming/
├── config.py                       # Configuration manager
├── setup_complete.sh               # Setup wizard
├── docker-compose.yml              # Infrastructure
├── transform_and_load.py           # Data transformation
├── clearvue_streaming_pipeline_final.py  # Main pipeline
├── transaction_simulator_enhanced.py     # Test data generator
├── kafka_consumer_test.py          # Kafka message tester
├── .env                            # Your config (auto-generated)
├── streaming_pipeline.log          # Pipeline logs
├── cleaned_data/                   # Your 17 CSV files
│   ├── Trans_Types.csv
│   ├── Suppliers.csv
│   └── ... (15 more files)
├── QUICK_START.md                  # This file
└── README.md                       # Full documentation
💡 Pro Tips
Always start pipeline BEFORE simulator
Pipeline must be watching for changes
Check Kafka UI frequently
http://localhost:8080
Verify messages are flowing
Use Mixed burst for testing
Tests all 6 collections
Shows variety of transactions
Export Kafka schema for Power BI team
bash
   python kafka_consumer_test.py
   # Choose: Export schema
   # Share kafka_message_schema.json
Monitor pipeline stats
Stats print every 30 seconds
Shows what's being processed
🆘 Need Help?
Check Logs
bash
# Pipeline logs
tail -f streaming_pipeline.log

# Docker logs
docker-compose logs -f kafka
docker-compose logs -f mongodb

# All logs
docker-compose logs --tail=100
Health Checks
bash
# Kafka
docker exec clearvue_kafka kafka-topics --bootstrap-server localhost:9092 --list

# MongoDB (local)
docker exec clearvue_mongodb mongosh --eval "db.adminCommand('ping')"

# Check everything
./setup_complete.sh  # Re-run setup
✅ Success Checklist
Before your demo/presentation:

 Docker services running (docker-compose ps)
 17 CSV files in ./cleaned_data/
 Data transformed and loaded (6 collections exist)
 Pipeline running and watching collections
 Simulator generates transactions successfully
 Kafka UI shows messages (http://localhost:8080)
 Mongo Express shows data (http://localhost:8081)
 kafka_consumer_test.py displays messages
 Power BI connected (optional)
🎓 Understanding the Flow
Your 17 CSV Files
        ↓
[transform_and_load.py] ONE-TIME
        ↓
6 MongoDB Collections
        ↓
[clearvue_streaming_pipeline_final.py] ALWAYS RUNNING
   (Watches for changes via Change Streams)
        ↓
Enriches with business context
        ↓
Apache Kafka Topics
        ↓
Power BI Dashboard (Real-time updates)
Key Point: The pipeline doesn't move data, it detects and broadcasts changes in real-time!

🚀 You're Ready!
If you completed all steps above, you have:

✅ Complete streaming infrastructure
✅ 6 NoSQL collections with real data
✅ Real-time change detection
✅ Kafka streaming working
✅ Test data generator
✅ Message verification tool
Next: Connect Power BI to Kafka topics and build your dashboard!

