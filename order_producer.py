"""
order_producer.py
------------------
Generates sample order data by sampling REAL rows from
ecommerce_delivery_analytics.csv (Assignment 1 source) and streams them
continuously to the Kafka topic 'order-events'.

For each streamed order:
  - Product Category, Platform, Order Value, Delivery Time, Service Rating,
    Delivery Delay are taken directly from a randomly sampled real row of
    the CSV.
  - Zone is picked from the shared sku_list.json zone list (real Region
    names sourced from sales_data.csv) so order and inventory data align.
  - SKU name is picked from the shared sku_list.json, matched to the same
    real category as the sampled row, so category values are consistent
    across both producers.
  - Timestamp is the current real time (this is what makes the stream
    'live', regardless of the underlying row being historical).
"""

import json
import random
import time
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaProducer

# ---------- CONFIG ----------
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "order-events"
SKU_FILE = "sku_list.json"
ECOMMERCE_FILE = "ecommerce_delivery_analytics.csv"
MIN_DELAY_SECONDS = 0.3
MAX_DELAY_SECONDS = 0.6
# -----------------------------


def load_sku_reference(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def load_real_orders(path: str) -> pd.DataFrame:
    """Load the real Assignment 1 order dataset once at startup."""
    df = pd.read_csv(path)
    return df


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def skus_by_category(reference: dict) -> dict:
    """Group shared SKU list by category for quick lookup."""
    grouped = {}
    for sku in reference["skus"]:
        grouped.setdefault(sku["category"], []).append(sku)
    return grouped


def generate_order(real_orders: pd.DataFrame, reference: dict,
                    grouped_skus: dict, order_id: int) -> dict:
    """
    Sample one real row from the ecommerce dataset and combine it with
    the shared SKU/zone reference to build one live streaming order event.
    """
    real_row = real_orders.sample(1).iloc[0]
    category = real_row["Product Category"]
    zone = random.choice(reference["zones"])

    # pick a SKU that belongs to the SAME real category as the sampled row
    matching_skus = grouped_skus.get(category, reference["skus"])
    sku = random.choice(matching_skus)

    order = {
        "order_id": order_id,
        "sku_id": sku["sku_id"],
        "sku_name": sku["name"],
        "category": category,                              # real value from CSV
        "platform": real_row["Platform"],                   # real value from CSV
        "zone": zone,                                        # real Region from sales_data
        "order_value_inr": float(real_row["Order Value (INR)"]),  # real value from CSV
        "delivery_time_minutes": float(real_row["Delivery Time (Minutes)"]),  # real value
        "service_rating": float(real_row["Service Rating"]) if not pd.isna(real_row["Service Rating"]) else None,
        "delivery_delay": real_row["Delivery Delay"],        # real value from CSV
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return order


def main():
    reference = load_sku_reference(SKU_FILE)
    grouped_skus = skus_by_category(reference)

    print(f"[order_producer] Loading real order data from {ECOMMERCE_FILE} ...")
    real_orders = load_real_orders(ECOMMERCE_FILE)
    print(f"[order_producer] Loaded {len(real_orders)} real rows for sampling.")

    producer = build_producer()

    print(f"[order_producer] Connected to Kafka at {KAFKA_BROKER}")
    print(f"[order_producer] Streaming orders (sampled from real data) to topic '{TOPIC_NAME}'")
    print("[order_producer] Press Ctrl+C to stop.\n")

    order_id = 1
    try:
        while True:
            order = generate_order(real_orders, reference, grouped_skus, order_id)
            producer.send(TOPIC_NAME, value=order)
            print(f"Sent -> {order}")

            order_id += 1
            time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))

    except KeyboardInterrupt:
        print("\n[order_producer] Stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
