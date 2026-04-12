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
    .saveAsTable("bronze_prices")

print(f"✓ bronze_prices saved: {spark_df.count()} rows")
spark_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_ticker = spark.createDataFrame([
    ("AAPL", "Apple", "Technology", "NASDAQ", "USA"),
    ("MSFT", "Microsoft", "Technology", "NASDAQ", "USA"),
    ("GOOGL", "Alphabet", "Technology", "NASDAQ", "USA"),
    ("NVDA", "NVIDIA", "Semiconductors", "NASDAQ", "USA"),
    ("TSLA", "Tesla", "Automotive/EV", "NASDAQ", "USA"),
    ("META", "Meta", "Technology", "NASDAQ", "USA"),
    ("AMZN", "Amazon", "E-commerce/Cloud", "NASDAQ", "USA"),
    ("JPM", "JPMorgan Chase", "Banking", "NYSE", "USA"),
    ("BAC", "Bank of America", "Banking", "NYSE", "USA"),
    ("GS", "Goldman Sachs", "Investment Banking", "NYSE", "USA"),
    ("SPY", "S&P 500 ETF", "ETF", "NYSE", "USA"),
], ["ticker", "company_name", "sector", "exchange", "country"])

dim_ticker.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_ticker")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
