"""
Run locally to generate sample JSON data for the RetailFlow project.

Usage:
    python setup/01_generate_data.py

Outputs to data/ directory:
    data/customers/batch_001.json  ... batch_003.json  (incremental drops)
    data/products/batch_001.json   ... batch_002.json
    data/orders/batch_001.json     ... batch_005.json  (higher volume)

Upload to your UC Volume after generating:
    databricks fs cp -r data/customers dbfs:/Volumes/retailflow/raw/landing/customers --profile DEV
    databricks fs cp -r data/products  dbfs:/Volumes/retailflow/raw/landing/products  --profile DEV
    databricks fs cp -r data/orders    dbfs:/Volumes/retailflow/raw/landing/orders    --profile DEV
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

REGIONS = ["EMEA", "AMER", "APJ"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books", "Food"]
ORDER_STATUSES = ["CREATED", "SHIPPED", "DELIVERED", "CANCELLED"]

FIRST_NAMES = ["Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry",
               "Iris", "James", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
               "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander"]
LAST_NAMES  = ["Smith", "Jones", "Williams", "Brown", "Taylor", "Davies", "Evans",
               "Wilson", "Thomas", "Roberts", "Johnson", "Lewis", "Walker", "Hall"]

BASE_DATE = datetime(2024, 1, 1)


def rand_date(start_offset_days=0, end_offset_days=365):
    return (BASE_DATE + timedelta(
        days=random.randint(start_offset_days, end_offset_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )).isoformat()


def make_customer(customer_id: int) -> dict:
    first = random.choice(FIRST_NAMES)
    last  = random.choice(LAST_NAMES)
    created = rand_date(0, 200)
    return {
        "customer_id": f"CUST_{customer_id:05d}",
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{customer_id}@example.com",
        "phone": f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "region": random.choice(REGIONS),
        "city": random.choice(["London", "New York", "Sydney", "Berlin", "Tokyo", "Paris"]),
        "loyalty_tier": random.choice(["BRONZE", "SILVER", "GOLD", "PLATINUM"]),
        "created_at": created,
        "updated_at": created,
        "is_active": True,
    }


def make_product(product_id: int) -> dict:
    category = random.choice(CATEGORIES)
    return {
        "product_id": f"PROD_{product_id:04d}",
        "name": f"{category} Item {product_id}",
        "category": category,
        "price": round(random.uniform(5.0, 999.99), 2),
        "cost": round(random.uniform(2.0, 500.0), 2),
        "stock_quantity": random.randint(0, 500),
        "supplier_id": f"SUP_{random.randint(1, 20):03d}",
        "updated_at": rand_date(0, 100),
    }


def make_order(order_id: int, customer_ids: list, product_ids: list) -> dict:
    cid = random.choice(customer_ids)
    pid = random.choice(product_ids)
    qty = random.randint(1, 5)
    price = round(random.uniform(10.0, 500.0), 2)
    order_time = rand_date(100, 365)
    # Simulate late-arriving events: ~10% arrive with delay
    ingested_at = (datetime.fromisoformat(order_time) + timedelta(
        minutes=random.choice([0, 0, 0, 0, 0, 0, 0, 0, 45, 120])
    )).isoformat()
    return {
        "order_id": f"ORD_{order_id:07d}",
        "customer_id": cid,
        "product_id": pid,
        "quantity": qty,
        "unit_price": price,
        "total_amount": round(qty * price, 2),
        "status": random.choice(ORDER_STATUSES),
        "region": random.choice(REGIONS),
        "order_time": order_time,
        "ingested_at": ingested_at,
    }


def write_jsonl(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    print(f"  Wrote {len(records):>5} records -> {path}")


def main():
    base = Path(__file__).parent.parent / "data"

    # --- Customers: 3 batches simulating incremental profile drops ---
    print("Generating customers...")
    all_customers = [make_customer(i) for i in range(1, 1001)]
    customer_ids  = [c["customer_id"] for c in all_customers]

    write_jsonl(all_customers[:400],  base / "customers/batch_001.json")
    write_jsonl(all_customers[400:700], base / "customers/batch_002.json")
    write_jsonl(all_customers[700:],  base / "customers/batch_003.json")

    # Batch 002 also contains some updates to batch_001 customers (for CDF demo)
    updated = []
    for c in all_customers[:50]:
        u = c.copy()
        u["loyalty_tier"] = "PLATINUM"
        u["updated_at"] = rand_date(200, 300)
        updated.append(u)
    write_jsonl(updated, base / "customers/batch_002_updates.json")

    # --- Products: 2 batches ---
    print("Generating products...")
    all_products = [make_product(i) for i in range(1, 201)]
    product_ids  = [p["product_id"] for p in all_products]

    write_jsonl(all_products[:120], base / "products/batch_001.json")
    write_jsonl(all_products[120:], base / "products/batch_002.json")

    # --- Orders: 5 batches, higher volume ---
    print("Generating orders...")
    all_orders = [make_order(i, customer_ids, product_ids) for i in range(1, 5001)]

    batch_size = 1000
    for b in range(5):
        write_jsonl(
            all_orders[b * batch_size:(b + 1) * batch_size],
            base / f"orders/batch_{b+1:03d}.json",
        )

    print("\nDone. Upload to your UC Volume with:")
    print("  databricks fs cp -r data/customers dbfs:/Volumes/retailflow/raw/landing/customers --profile DEV")
    print("  databricks fs cp -r data/products  dbfs:/Volumes/retailflow/raw/landing/products  --profile DEV")
    print("  databricks fs cp -r data/orders    dbfs:/Volumes/retailflow/raw/landing/orders    --profile DEV")


if __name__ == "__main__":
    main()
