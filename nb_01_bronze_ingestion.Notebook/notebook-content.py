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
# META     }
# META   }
# META }

# CELL ********************

%pip install yfinance requests pandas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "JPM", "BAC", "GS",
    "SPY"  
]


end_date = datetime.today().strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days=730)).strftime('%Y-%m-%d')

print(f"Downloading data from {start_date} to {end_date}")
print(f"Tickers: {TICKERS}")

all_data = []

for ticker in TICKERS:
    df = yf.download(ticker, start=start_date, end=end_date, 
                     progress=False, auto_adjust=True)
    df = df.reset_index()
    df["ticker"] = ticker
    
    df.columns = [c[0].lower().replace(" ", "_") 
                  if isinstance(c, tuple) else c.lower().replace(" ", "_") 
                  for c in df.columns]
    all_data.append(df)
    print(f"✓ {ticker}: {len(df)} rows")

df_prices = pd.concat(all_data, ignore_index=True)
print(f"\nTotal rows: {len(df_prices)}")
print(df_prices.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_prices)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


spark_df = spark.createDataFrame(df_prices)

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Files/bronze/prices/")

print(f"✓ Bronze prices saved successfully")
print(f"✓ Rows written: {spark_df.count()}")
print(f"✓ Schema:")
spark_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_prices")

print("✓ Table bronze_prices created in Lakehouse")

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
