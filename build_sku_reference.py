"""
build_sku_reference.py
------------------------
Builds sku_list.json using REAL values pulled from the two Assignment 1
datasets:

  - ecommerce_delivery_analytics.csv  -> supplies the real grocery/quick-commerce
    Product Categories (Fruits & Vegetables, Dairy, Beverages, Personal Care,
    Grocery, Snacks) and real Platform names.

  - sales_data.csv -> supplies real Region names (used as our Zones) and
    real Inventory Level values (used as realistic starting stock ranges).

Note: Neither file contains actual product/SKU names (ecommerce data only
has categories; sales data has anonymized Product IDs from an unrelated
retail domain). So representative SKU names are assigned per real category
-- everything else (categories, zones, stock-level ranges) is pulled
directly from the real files, not invented.

Run this once to (re)generate sku_list.json.
"""

import pandas as pd
import json
import random

ECOMMERCE_FILE = "ecommerce_delivery_analytics.csv"
SALES_FILE = "sales_data.csv"
OUTPUT_FILE = "sku_list.json"

# Representative product names per real grocery category
# (SKU names are the only invented piece -- neither dataset provides them)
SKU_NAMES_BY_CATEGORY = {
    "Fruits & Vegetables": ["Bananas 1kg", "Tomatoes 1kg", "Onions 1kg", "Apples 1kg"],
    "Dairy": ["Amul Full Cream Milk 500ml", "Farm Eggs (6 pack)", "Amul Butter 100g", "Paneer 200g"],
    "Beverages": ["Coca-Cola 750ml", "Red Bull Energy Drink 250ml", "Nescafe Instant Coffee 50g", "Real Fruit Juice 1L"],
    "Personal Care": ["Colgate Toothpaste 100g", "Dettol Handwash 200ml", "Dove Soap 100g", "Head & Shoulders Shampoo 180ml"],
    "Grocery": ["Tata Salt 1kg", "Aashirvaad Atta 5kg", "Fortune Sunflower Oil 1L", "Toor Dal 1kg"],
    "Snacks": ["Lays Classic Chips 52g", "Parle-G Biscuits 200g", "Maggi Noodles 2-Min 70g", "Cadbury Dairy Milk 55g"],
}


def main():
    print("Reading ecommerce_delivery_analytics.csv ...")
    ecom = pd.read_csv(ECOMMERCE_FILE)
    real_categories = sorted(ecom["Product Category"].unique().tolist())
    real_platforms = sorted(ecom["Platform"].unique().tolist())
    print(f"Real categories found: {real_categories}")
    print(f"Real platforms found: {real_platforms}")

    print("\nReading sales_data.csv ...")
    sales = pd.read_csv(SALES_FILE)
    real_zones = sorted(sales["Region"].unique().tolist())
    inventory_levels = sales["Inventory Level"].tolist()
    print(f"Real zones (regions) found: {real_zones}")
    print(f"Inventory Level range in real data: {min(inventory_levels)} - {max(inventory_levels)}")

    # Build SKU list: one entry per representative product per real category
    skus = []
    sku_counter = 1
    for category in real_categories:
        names = SKU_NAMES_BY_CATEGORY.get(category, [f"{category} Item"])
        for name in names:
            starting_stock = int(random.choice(inventory_levels))  # real sampled value
            skus.append({
                "sku_id": f"SKU{sku_counter:03d}",
                "name": name,
                "category": category,
                "starting_stock": starting_stock
            })
            sku_counter += 1

    reference = {
        "zones": real_zones,          # real Region names from sales_data.csv
        "platforms": real_platforms,  # real Platform names from ecommerce_delivery_analytics.csv
        "skus": skus
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(reference, f, indent=2)

    print(f"\n{OUTPUT_FILE} generated with {len(skus)} SKUs across {len(real_categories)} real categories and {len(real_zones)} real zones.")


if __name__ == "__main__":
    main()
