"""
Transaction Simulator for Nova_Analytics
Generates and inserts random transactions into MongoDB Atlas collections for testing the streaming pipeline.
"""
import os
import random
import time
from pymongo import MongoClient
from dotenv import load_dotenv

# Load MongoDB Atlas URI from .env
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["Nova_Analytics"]

# Collections to simulate
collections = ["sales", "payments", "purchases", "customers", "suppliers", "products"]

def random_doc(collection_name):
	if collection_name == "sales":
		return {
			"sale_id": f"SALE{random.randint(1000,9999)}",
			"customer_id": f"CUST{random.randint(100,999)}",
			"amount": round(random.uniform(100, 5000), 2),
			"date": time.strftime('%Y-%m-%d'),
			"lines": [
				{"product_id": f"PROD{random.randint(100,999)}", "qty": random.randint(1,5), "price": round(random.uniform(10,500),2)}
				for _ in range(random.randint(1,3))
			]
		}
	elif collection_name == "payments":
		return {
			"payment_id": f"PAY{random.randint(1000,9999)}",
			"customer_id": f"CUST{random.randint(100,999)}",
			"amount": round(random.uniform(100, 5000), 2),
			"date": time.strftime('%Y-%m-%d'),
			"lines": [
				{"fin_period": 202510, "deposit_date": time.strftime('%Y-%m-%d'), "bank_amt": round(random.uniform(100,5000),2), "tot_payment": round(random.uniform(100,5000),2)}
			]
		}
	elif collection_name == "purchases":
		return {
			"purchase_id": f"PUR{random.randint(1000,9999)}",
			"supplier_id": f"SUP{random.randint(100,999)}",
			"amount": round(random.uniform(100, 5000), 2),
			"date": time.strftime('%Y-%m-%d'),
			"lines": [
				{"product_id": f"PROD{random.randint(100,999)}", "qty": random.randint(1,5), "cost": round(random.uniform(10,500),2)}
				for _ in range(random.randint(1,3))
			]
		}
	elif collection_name == "customers":
		return {
			"customer_id": f"CUST{random.randint(100,999)}",
			"name": f"Customer {random.randint(1,100)}",
			"region": random.choice(["Gauteng", "Western Cape", "KwaZulu-Natal"]),
			"status": random.choice(["Active", "Inactive"])
		}
	elif collection_name == "suppliers":
		return {
			"supplier_id": f"SUP{random.randint(100,999)}",
			"name": f"Supplier {random.randint(1,100)}",
			"category": random.choice(["Tech", "Office", "Logistics"])
		}
	elif collection_name == "products":
		return {
			"product_id": f"PROD{random.randint(100,999)}",
			"name": f"Product {random.randint(1,100)}",
			"brand": random.choice(["BrandA", "BrandB", "BrandC"]),
			"unit_price": round(random.uniform(10,500),2)
		}
	else:
		return {}

def simulate_transactions(n=10, delay=1):
	for i in range(n):
		collection_name = random.choice(collections)
		doc = random_doc(collection_name)
		result = db[collection_name].insert_one(doc)
		print(f"Inserted into {collection_name}: {doc}")
		time.sleep(delay)

if __name__ == "__main__":
	print("🚀 Starting Transaction Simulator...")
	simulate_transactions(n=20, delay=2)
	print("✅ Simulation complete.")
