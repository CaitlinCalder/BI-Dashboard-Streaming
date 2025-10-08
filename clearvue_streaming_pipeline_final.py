import os
import json
import time
import threading
from pymongo import MongoClient
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load MongoDB Atlas URI from .env
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Connect to MongoDB Atlas
client = MongoClient(mongo_uri)
db = client["Nova_Analytics"]  # Database name your team uses

# Kafka Producer (Docker running on localhost)
producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Collections to watch
collections = ["sales", "payments", "purchases", "customers", "suppliers", "products"]

# Function to enrich document before sending
def enrich_document(doc, collection_name):
    enriched = doc.copy()
    enriched["processed_at"] = time.strftime('%Y-%m-%d %H:%M:%S')
    enriched["source_collection"] = collection_name
    return enriched

# Function to watch one collection and stream changes to Kafka
def watch_collection(collection_name):
    collection = db[collection_name]
    with collection.watch(full_document='updateLookup') as stream:
        print(f"👀 Watching: {collection_name}")
        for change in stream:
            operation = change["operationType"]
            full_doc = change.get("fullDocument", {})
            if full_doc:
                enriched = enrich_document(full_doc, collection_name)
                topic = f"{collection_name}_topic"
                print(f"📤 Sending to Kafka → {topic}")
                producer.send(topic, enriched)

# Start a thread for each collection
if __name__ == "__main__":
    threads = []
    for col in collections:
        t = threading.Thread(target=watch_collection, args=(col,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
