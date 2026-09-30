"""
consumer.py
------------
Reads live events from BOTH Kafka topics your producers write to:
  - order-events        (from order_producer.py)
  - inventory-updates   (from inventory_producer.py)

For every message received, it updates running aggregates in MongoDB.
These aggregates are what the dashboard (dashboard.py) reads and charts --
the raw event itself is not stored permanently, only the derived summary
(this matches the design discussed earlier: Kafka = temporary pass-through,
MongoDB = permanent store of processed results).

MongoDB collections written:
  - demand_by_zone      : running order count + total order value per zone
  - demand_by_category  : running order count + total order value per category
  - stock_snapshot      : latest known stock level per SKU-zone (for stockout view)
"""

import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
from pymongo import MongoClient

# ---------- CONFIG ----------
KAFKA_BROKER = "localhost:9092"
ORDER_TOPIC = "order-events"
INVENTORY_TOPIC = "inventory-updates"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "qcommerce"
# -----------------------------


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        ORDER_TOPIC,
        INVENTORY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",   # only process NEW messages from now on
        enable_auto_commit=True,
        group_id="qcommerce-dashboard-consumer",
    )


def handle_order_event(db, event: dict):
    """Update demand_by_zone and demand_by_category using this order."""
    zone = event.get("zone")
    category = event.get("category")
    order_value = event.get("order_value_inr", 0) or 0

    db.demand_by_zone.update_one(
        {"zone": zone},
        {
            "$inc": {"order_count": 1, "total_value": order_value},
            "$set": {"last_updated": datetime.now(timezone.utc).isoformat()},
        },
        upsert=True,
    )

    db.demand_by_category.update_one(
        {"category": category},
        {
            "$inc": {"order_count": 1, "total_value": order_value},
            "$set": {"last_updated": datetime.now(timezone.utc).isoformat()},
        },
        upsert=True,
    )


def handle_inventory_event(db, event: dict):
    """Update stock_snapshot with the latest stock level for this SKU-zone."""
    db.stock_snapshot.update_one(
        {"sku_id": event.get("sku_id"), "zone": event.get("zone")},
        {
            "$set": {
                "sku_name": event.get("sku_name"),
                "category": event.get("category"),
                "zone": event.get("zone"),
                "current_stock": event.get("current_stock"),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
        },
        upsert=True,
    )


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    consumer = build_consumer()

    print(f"[consumer] Connected to Kafka at {KAFKA_BROKER}")
    print(f"[consumer] Subscribed to topics: {ORDER_TOPIC}, {INVENTORY_TOPIC}")
    print(f"[consumer] Writing aggregates to MongoDB database '{DB_NAME}'")
    print("[consumer] Press Ctrl+C to stop.\n")

    try:
        for message in consumer:
            topic = message.topic
            event = message.value

            if topic == ORDER_TOPIC:
                handle_order_event(db, event)
                print(f"[order-events] processed -> zone={event.get('zone')}, "
                      f"category={event.get('category')}, value={event.get('order_value_inr')}")

            elif topic == INVENTORY_TOPIC:
                handle_inventory_event(db, event)
                print(f"[inventory-updates] processed -> sku={event.get('sku_name')}, "
                      f"zone={event.get('zone')}, stock={event.get('current_stock')}")

    except KeyboardInterrupt:
        print("\n[consumer] Stopped by user.")
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
