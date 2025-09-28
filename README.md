# ClearVue NoSQL BI Dashboard - Streaming Pipeline

This project implements a real-time streaming pipeline for ClearVue's business intelligence system using MongoDB, Apache Kafka, and Python.

## Architecture Overview

```
MongoDB (Change Streams) → Kafka Producer → Kafka Topics → Kafka Consumer → BI Dashboard
```

## Quick Start (TL;DR)

**Essential Files Only:**
1. Install Docker Desktop
2. `docker-compose up -d`
3. Wait 30 seconds, then: `docker exec -it clearvue_mongodb mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"`
4. `python clearvue_dummy_data.py`
5. `python mongodb_kafka_streaming.py`
6. In new terminal: `python clearvue_transaction_simulator.py`

**Optional (for testing/demo):**
7. In third terminal: `python clearvue_kafka_consumer.py`

### Required Software
1. **Docker Desktop** - Download from [docker.com](https://www.docker.com/products/docker-desktop/)
2. **Python 3.8+** with pip
3. **Git** (for cloning the repository)

### Python Dependencies
Install required packages:
```bash
pip install pymongo kafka-python faker bson
```

## Project Structure

```
NoSQL BI-dashboard/
├── docker-compose.yml          # Docker services configuration
├── clearvue_dummy_data.py      # Generate test data
├── mongodb_kafka_streaming.py  # Main streaming pipeline
├── clearvue_kafka_consumer.py  # Test message consumer (OPTIONAL)
├── clearvue_transaction_simulator.py  # Generate real-time transactions
└── README.md                   # This file
```

## Setup Instructions

### Step 1: Start Docker Services

1. **Download and start Docker Desktop**
2. **Clone the repository and navigate to project folder**
3. **Start all services:**
   ```bash
   docker-compose down -v  # Clean start
   docker-compose up -d    # Start all services
   ```

4. **Check that all containers are running:**
   ```bash
   docker ps
   ```
   You should see: `clearvue_mongodb`, `clearvue_kafka`, `clearvue_zookeeper`, etc.

### Step 2: Initialize MongoDB Replica Set

MongoDB needs to be configured as a replica set for change streams to work:

```bash
# Wait 30 seconds for MongoDB to start, then run:
docker exec -it clearvue_mongodb mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"
```

**Verify replica set is working:**
```bash
docker exec -it clearvue_mongodb mongosh --eval "rs.status()"
```

### Step 3: Generate Initial Data

Run the dummy data generator to populate MongoDB:
```bash
python clearvue_dummy_data.py
```

Expected output:
```
Populating ClearVue database with dummy data...
✅ Inserted 1000 customers
✅ Inserted 100 products
✅ Inserted 2000 payment headers
✅ Inserted [X] payment lines
Database populated successfully!
```

## Running the Streaming Pipeline

### Core Pipeline (Required)

**Terminal 1: Start the Streaming Pipeline**
```bash
python mongodb_kafka_streaming.py
```

Expected output:
```
Starting ClearVue Streaming Pipeline...
Started 4 change stream watchers
Pipeline is running! Press Ctrl+C to stop
```

**Terminal 2: Generate Real-time Transactions**
```bash
python clearvue_transaction_simulator.py
```

Choose option 3 for continuous simulation. This will generate transactions every few seconds.

### Optional: Monitor Messages (For Testing/Demo)

**Terminal 3: Start the Kafka Consumer (Optional)**
```bash
python clearvue_kafka_consumer.py
```

Choose option 1 to monitor all topics. You should see:
```
ClearVue Kafka Consumer
Waiting for messages... (Press Ctrl+C to stop)
```

**Note:** The consumer is just for viewing messages - the pipeline works without it.

## Verifying Everything Works

When everything is working correctly, you should see:

1. **Streaming Pipeline Terminal (`mongodb_kafka_streaming.py`):**
   ```
   Processed insert on payment_headers (Total processed: 15)
   Sent to Kafka - Topic: clearvue.payments.realtime
   ```

2. **Transaction Simulator Terminal:**
   ```
   Transaction 789012: R25,500.00 (Completed) [Total: 8]
   ```

3. **Consumer Terminal (if running - optional):**
   ```
   + Payment 123456: R15,000.00 (Gauteng) [Completed] | 2024-10-21 14:30
   ```

## Kafka Topics Created

The pipeline creates these topics automatically:
- `clearvue.customers.changes` - Customer data changes
- `clearvue.payments.realtime` - Payment transactions
- `clearvue.payments.lines` - Payment line items
- `clearvue.products.updates` - Product updates

## Web Interfaces

Access these in your browser:

- **Kafka UI:** http://localhost:8080 - Monitor Kafka topics and messages
- **MongoDB Express:** http://localhost:8081 - Browse MongoDB data
  - Username: `clearvue`
  - Password: `admin123`

## Integration with Team Data

### To Replace Dummy Data with Cleaned Team Data:

1. **Stop the streaming pipeline** (Ctrl+C)

2. **Update `clearvue_dummy_data.py` or create new script:**
   ```python
   # Replace the generate_* methods with your team's data loading logic
   def load_cleaned_customers(self, file_path):
       # Load your cleaned customer data
       # Make sure to include 'created_at' field for streaming
       pass
   ```

3. **Ensure all documents have `created_at` field:**
   ```python
   # Add this to all your documents:
   'created_at': datetime.now()
   ```

4. **Restart pipeline:**
   ```bash
   python clearvue_streaming_pipeline.py
   ```

## Troubleshooting

### Common Issues:

**1. "Replica set not supported" error:**
```bash
# Re-initialize replica set:
docker exec -it clearvue_mongodb mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"
```

**2. Kafka connection errors:**
```bash
# Restart Kafka:
docker-compose restart kafka
docker-compose logs kafka
```

**3. No messages flowing through Kafka:**
- Check that transaction simulator is running
- Verify streaming pipeline shows "Processed insert" messages
- Check Kafka UI at http://localhost:8080 to see topic activity
- Make sure both terminals are running simultaneously

**4. Python import errors:**
```bash
pip install pymongo kafka-python faker bson
```

### Checking Container Status:
```bash
# View all containers:
docker ps -a

# Check logs for specific service:
docker-compose logs mongodb
docker-compose logs kafka

# Restart specific service:
docker-compose restart mongodb
```

## Development Notes

### Code Structure:
- **`mongodb_kafka_streaming.py`** - Main pipeline with change streams
- **`clearvue_transaction_simulator.py`** - Generates test transactions
- **`clearvue_dummy_data.py`** - Data population (replace with your cleaned data)

### Key Configuration:
- MongoDB runs on port 27017 (no authentication in dev)
- Kafka runs on port 29092
- All data stored in Docker volumes (persists between restarts)

## Team Integration Checklist

- [ ] Docker Desktop installed and running
- [ ] All containers started successfully
- [ ] MongoDB replica set initialized
- [ ] Dummy data generated successfully
- [ ] Streaming pipeline running without errors
- [ ] Kafka consumer receiving messages
- [ ] Transaction simulator generating data
- [ ] Web interfaces accessible

## Next Steps for BI Dashboard

1. **Connect your BI tool** (Power BI, Tableau, etc.) to Kafka topics
2. **Create real-time visualizations** using the streamed data
3. **Implement data transformations** as needed for your dashboard
4. **Set up monitoring** for production deployment

## Contact

If you have issues setting this up, check:
1. Docker containers are all running
2. MongoDB replica set is initialized
3. Python dependencies are installed
4. No port conflicts (27017, 29092, 8080, 8081)

---

**Note:** This is a development setup. For production, you'd need proper authentication, SSL, monitoring, and scaling considerations.
