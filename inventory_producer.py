"""
inventory_producer.py
----------------------
Generates sample inventory/stock-level data by sampling REAL rows from
sales_data.csv (Assignment 1 source) and streams them continuously to the
Kafka topic 'inventory-updates'.

For each streamed update:
  - Region (used as Zone), Inventory Level, Units Sold, Units Ordered are
    taken directly from a randomly sampled real row of sales_data.csv.
  - SKU name/category is picked from the shared sku_list.json (same file
    order_producer.py uses), so SKU identities always match across both
    streams -- sales_data's own Product IDs/Categories belong to a
    different retail domain (Electronics/Clothing/etc.) and are NOT used
    for naming, only its real numeric stock patterns are reused.
  - A running in-memory stock table (keyed by SKU + zone, seeded from
    sku_list.json starting_stock) is updated using the REAL Units Sold
    value sampled from the row, so depletion behaviour reflects real
    retail sales patterns.
  - Timestamp is the current real time.
"""

import json
import random
import time
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaProducer

# ---------- CONFIG ----------
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "inventory-updates"
SKU_FILE = "sku_list.json"
SALES_FILE = "sales_data.csv"
MIN_DELAY_SECONDS = 0.6
MAX_DELAY_SECONDS = 1.2
# -----------------------------


def load_sku_reference(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def load_real_sales(path: str) -> pd.DataFrame:
    """Load the real Assignment 1 inventory/sales dataset once at startup."""
    df = pd.read_csv(path)
    return df


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def build_starting_stock(reference: dict) -> dict:
    """
    In-memory stock table keyed by (sku_id, zone), seeded from the
    starting_stock values in sku_list.json (themselves sampled from real
    Inventory Level values in sales_data.csv during reference-building).
    """
    stock = {}
    for sku in reference["skus"]:
        for zone in reference["zones"]:
            stock[(sku["sku_id"], zone)] = sku["starting_stock"]
    return stock


def main():
    reference = load_sku_reference(SKU_FILE)
    stock_table = build_starting_stock(reference)
    sku_lookup = {s["sku_id"]: s for s in reference["skus"]}

    print(f"[inventory_producer] Loading real sales/inventory data from {SALES_FILE} ...")
    real_sales = load_real_sales(SALES_FILE)
    print(f"[inventory_producer] Loaded {len(real_sales)} real rows for sampling.")

    producer = build_producer()

    print(f"[inventory_producer] Connected to Kafka at {KAFKA_BROKER}")
    print(f"[inventory_producer] Streaming stock updates (sampled from real data) to topic '{TOPIC_NAME}'")
    print("[inventory_producer] Press Ctrl+C to stop.\n")

    try:
        while True:
            real_row = real_sales.sample(1).iloc[0]

            # Real Region from sales_data.csv IS our zone -> guaranteed match
            # with order_producer.py's zone list (both sourced from sales_data).
            zone = real_row["Region"]

            sku = random.choice(reference["skus"])
            key = (sku["sku_id"], zone)

            # Deplete stock using the REAL Units Sold value from the sampled row
            real_units_sold = int(real_row["Units Sold"])
            depletion = max(1, real_units_sold % 10)  # scale down to keep demo readable
            stock_table[key] = max(0, stock_table[key] - depletion)

            # Occasionally restock using the REAL Units Ordered value
            if random.random() < 0.05:
                real_units_ordered = int(real_row["Units Ordered"])
                stock_table[key] += real_units_ordered % 50

            update = {
                "sku_id": sku["sku_id"],
                "sku_name": sku_lookup[sku["sku_id"]]["name"],
                "category": sku["category"],
                "zone": zone,                                   # real Region from CSV
                "current_stock": stock_table[key],
                "real_units_sold_sampled": real_units_sold,      # real value from CSV
                "real_store_id_sampled": real_row["Store ID"],   # real value from CSV
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            producer.send(TOPIC_NAME, value=update)
            print(f"Sent -> {update}")

            time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))

    except KeyboardInterrupt:
        print("\n[inventory_producer] Stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
