# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d0d79186-79f8-4c5d-9beb-27d1c9f02868",
# META       "default_lakehouse_name": "lh_market_intelligence",
# META       "default_lakehouse_workspace_id": "4c8e2bff-9177-4eed-bc17-9f87e0dd8f2f",
# META       "known_lakehouses": [
# META         {
# META           "id": "d0d79186-79f8-4c5d-9beb-27d1c9f02868"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "c03cf241-b117-9966-4ce6-3cbb2edbd51c",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import requests
import pandas as pd
from datetime import datetime

URL = "https://api.fundpress.io/fund/searchEntity"

HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://www.mandg.com",
    "Referer": "https://www.mandg.com/",
    "X-KSYS-TOKEN": "6440e8fd-e841-4807-848e-476143c1b20d"
}

def fetch_mandg_funds(start=0, limit=15):
    payload = {
        "type": "CLSS",
        "fundList": ["gb_private"],
        "limit": limit,
        "start": start,
        "sort": [{"direction": "ASC", "key": "fund_name"}],
        "search": [{"property": "product_type", "values": ["OEIC"], "matchtype": "MATCH"}],
        "include": {"statistics": {"limitTo": ["latest_prices"]}},
        "culture": "en-GB"
    }
    response = requests.post(URL, json=payload, headers=HEADERS)
    return response.json()

# Test
first_batch = fetch_mandg_funds(start=0, limit=15)
total = first_batch.get("total", 0)
print(f"Total funds: {total}")
print(f"First batch: {len(first_batch.get('values', []))} funds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pobierz wszystkie fundusze przez paginację
all_funds = []
start = 0
limit = 15

print(f"Fetching all {total} funds...")

while start < total:
    batch = fetch_mandg_funds(start=start, limit=limit)
    funds = batch.get("values", [])
    
    for fund in funds:
        props = fund.get("properties_pub", {})
        stats = fund.get("statistics", [])
        
        # Wyciągnij ceny z statistics
        nav_price = None
        price_change_pct = None
        yield_val = None
        
        for stat in stats:
            if stat.get("code") == "latest_prices":
                for v in stat.get("values", []):
                    label = v.get("label", "")
                    value = v.get("value")
                    if label == "Nav Price":
                        nav_price = value
                    elif label == "Price Change %":
                        price_change_pct = value
                    elif label == "Yield":
                        yield_val = value

        all_funds.append({
            "isin":             props.get("isin", {}).get("value"),
            "fund_name":        props.get("fund_name", {}).get("value"),
            "share_class":      props.get("share_class_type", {}).get("value"),
            "currency":         props.get("share_class_currency", {}).get("value"),
            "asset_class":      props.get("asset_class", {}).get("value"),
            "region":           props.get("region", {}).get("value"),
            "nav_price":        nav_price,
            "price_change_pct": price_change_pct,
            "yield_pct":        yield_val,
            "scraped_date":     datetime.today().strftime('%Y-%m-%d'),
            "ingested_at":      datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    start += limit
    print(f"  ✓ Fetched {min(start, total)}/{total}")

df_mandg = pd.DataFrame(all_funds)
print(f"\nTotal rows: {len(df_mandg)}")
display(df_mandg.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Konwersja do Spark i zapis Bronze
spark_df_mandg = spark.createDataFrame(df_mandg)

spark_df_mandg = spark_df_mandg \
    .withColumn("nav_price", F.col("nav_price").cast("double")) \
    .withColumn("price_change_pct", F.col("price_change_pct").cast("double")) \
    .withColumn("yield_pct", F.col("yield_pct").cast("double")) \
    .withColumn("scraped_date", F.to_date("scraped_date")) \
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))

spark_df_mandg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_mandg_funds")

print(f"✓ bronze_mandg_funds saved: {spark_df_mandg.count()} rows")
spark_df_mandg.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
