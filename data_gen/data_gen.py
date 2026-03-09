"""
Retail Data Generator (Optimized for t3.micro / 1GB RAM + Swap)
Generates 100,000 rows in small batches with aggressive gc.collect()
to stay within the ~1GB RAM + swap budget of the EC2 instance.
"""

import os
import csv
import random
import uuid
import gc  # Garbage Collector para sa RAM cleaning
from datetime import datetime, timedelta
from faker import Faker
import numpy as np

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# --- Configuration ---
# ✅ Lowered to 100k records — safe for 1GB RAM + swap (t3.micro)
NUM_ROWS = int(os.environ.get("NUM_ROWS", "100000"))  # Override via env var if needed
BATCH_SIZE = 10_000  # Flush gc every 10k rows — more frequent to protect RAM
OUTPUT_DIR = os.environ.get("DATA_OUTPUT_DIR", "/opt/airflow/data_gen/output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "retail_transactions.csv")

# --- Reference Data ---
PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Groceries", "Home & Kitchen",
    "Sports & Outdoors", "Beauty & Personal Care", "Books", "Toys & Games"
]

PRODUCTS = {
    "Electronics":           [("Smartphone", 8000, 60000), ("Laptop", 25000, 120000),
                               ("Earbuds", 500, 8000),    ("Smart Watch", 3000, 25000),
                               ("Tablet", 10000, 50000)],
    "Clothing":              [("T-Shirt", 200, 1500),     ("Jeans", 500, 4000),
                               ("Dress", 400, 5000),      ("Jacket", 800, 8000),
                               ("Sneakers", 1000, 12000)],
    "Groceries":             [("Rice (5kg)", 250, 400),   ("Cooking Oil (1L)", 80, 150),
                               ("Instant Noodles", 10, 30), ("Canned Goods", 30, 120),
                               ("Coffee (200g)", 150, 600)],
    "Home & Kitchen":        [("Blender", 800, 5000),     ("Rice Cooker", 600, 4000),
                               ("Frying Pan", 300, 2500), ("Storage Box", 150, 800),
                               ("Dish Rack", 200, 1000)],
    "Sports & Outdoors":     [("Yoga Mat", 400, 2000),    ("Dumbbells (pair)", 800, 5000),
                               ("Running Shoes", 1500, 8000), ("Bicycle Helmet", 500, 3000),
                               ("Jump Rope", 100, 600)],
    "Beauty & Personal Care":[("Facial Wash", 100, 600),  ("Shampoo", 80, 500),
                               ("Sunscreen SPF50", 200, 1200), ("Lip Balm", 50, 300),
                               ("Moisturizer", 150, 1500)],
    "Books":                 [("Fiction Novel", 150, 600), ("Self-Help Book", 200, 800),
                               ("Textbook", 500, 3000),   ("Children's Book", 100, 400),
                               ("Comic Book", 80, 350)],
    "Toys & Games":          [("Board Game", 400, 3000),  ("Action Figure", 150, 1500),
                               ("Building Blocks", 300, 2000), ("Stuffed Toy", 100, 800),
                               ("Puzzle (500pcs)", 200, 1200)],
}

PAYMENT_METHODS = ["Cash", "Credit Card", "Debit Card", "GCash", "Maya", "ShopeePay", "Bank Transfer"]

STORE_LOCATIONS = [
    "Manila", "Quezon City", "Cebu City", "Davao City", "Makati",
    "Pasig", "Taguig", "Iloilo City", "Bacolod", "Cagayan de Oro"
]

def generate_customers(n=5000):  # ✅ Reduced pool: 5k customers is enough for 100k transactions
    print(f"Generating {n} customer profiles...")
    customers = []
    for _ in range(n):
        customers.append({
            "customer_id": str(uuid.uuid4()),
            "customer_name": fake.name(),
            "customer_email": fake.email(),
        })
    return customers

def generate_transactions(num_rows, customers, output_file=None):
    if output_file is None:
        output_file = OUTPUT_FILE

    print(f"Generating {num_rows:,} transaction rows in batches...")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    fieldnames = [
        "transaction_id", "transaction_date", "customer_id",
        "customer_name", "customer_email", "product_category",
        "product_name", "quantity", "unit_price", "total_amount",
        "payment_method", "store_location"
    ]

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range_days = (end_date - start_date).days

    # Buksan ang file sa simula pa lang
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(num_rows):
            category = random.choice(PRODUCT_CATEGORIES)
            product_name, min_price, max_price = random.choice(PRODUCTS[category])

            unit_price = round(random.uniform(min_price, max_price), 2)
            quantity = np.random.choice(
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                p=[0.40, 0.25, 0.15, 0.08, 0.05, 0.03, 0.015, 0.01, 0.005, 0.01]  # sums to 1.0
            )
            total_amount = round(unit_price * quantity, 2)
            customer = random.choice(customers)

            random_days = random.randint(0, date_range_days)
            txn_date = start_date + timedelta(
                days=random_days,
                hours=random.randint(6, 22),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )

            writer.writerow({
                "transaction_id": str(uuid.uuid4()),
                "transaction_date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
                "customer_id": customer["customer_id"],
                "customer_name": customer["customer_name"],
                "customer_email": customer["customer_email"],
                "product_category": category,
                "product_name": product_name,
                "quantity": int(quantity),
                "unit_price": unit_price,
                "total_amount": total_amount,
                "payment_method": random.choice(PAYMENT_METHODS),
                "store_location": random.choice(STORE_LOCATIONS),
            })

            # Force memory cleanup every batch
            if (i + 1) % BATCH_SIZE == 0:
                print(f"  {i + 1:>10,} / {num_rows:,} rows generated...")
                gc.collect() # <--- Eto ang sikreto para hindi mag-crash

    print(f"\n✅ Done! {num_rows:,} rows saved to {output_file}")
    return output_file

if __name__ == "__main__":
    customers = generate_customers()
    generate_transactions(NUM_ROWS, customers)