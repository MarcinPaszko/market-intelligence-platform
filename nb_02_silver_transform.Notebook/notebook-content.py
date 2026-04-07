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

# MARKDOWN ********************


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_bronze = spark.table("bronze_prices")


window_ticker = Window.partitionBy("ticker").orderBy("date")


df_silver = df_bronze \
    .withColumn("trade_date", F.col("date").cast("date")) \
    .withColumn("prev_close", F.lag("close", 1).over(window_ticker)) \
    .withColumn("daily_return_pct",
        F.round(
            (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100,
        2)) \
    .withColumn("volatility_7d",
        F.round(
            F.stddev("daily_return_pct").over(
                window_ticker.rowsBetween(-6, 0)),
        4)) \
    .withColumn("volume_change_pct",
        F.round(
            (F.col("volume") - F.lag("volume", 1).over(window_ticker)) /
             F.lag("volume", 1).over(window_ticker) * 100,
        2)) \
    .withColumn("price_range_pct",
        F.round((F.col("high") - F.col("low")) / F.col("low") * 100, 2)) \
    .withColumn("loaded_at", F.current_timestamp()) \
    .select(
        "ticker",
        "trade_date",
        F.round("open", 4).alias("open"),
        F.round("high", 4).alias("high"),
        F.round("low", 4).alias("low"),
        F.round("close", 4).alias("close"),
        "volume",
        "daily_return_pct",
        "volatility_7d",
        "volume_change_pct",
        "price_range_pct",
        "loaded_at"
    ) \
    .dropna(subset=["daily_return_pct"])

print(f"✓ Silver rows: {df_silver.count()}")
display(df_silver.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_prices")

print("✓ silver_prices saved to Lakehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Gold 1 – daily prices (czyste dane dla Power BI)
gold_daily = df_silver.select(
    "ticker",
    "trade_date",
    F.col("open").alias("open_price"),
    F.col("high").alias("high_price"),
    F.col("low").alias("low_price"),
    F.col("close").alias("close_price"),
    "volume",
    "daily_return_pct",
    "volatility_7d",
    "volume_change_pct",
    "price_range_pct",
    F.current_timestamp().alias("loaded_at")
)

gold_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_daily_prices")

print(f"✓ gold_daily_prices: {gold_daily.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold 2 – miesięczne agregaty
gold_monthly = df_silver \
    .withColumn("year", F.year("trade_date")) \
    .withColumn("month", F.month("trade_date")) \
    .groupBy("ticker", "year", "month") \
    .agg(
        F.round(F.avg("close"), 4).alias("avg_close"),
        F.round(F.min("close"), 4).alias("min_close"),
        F.round(F.max("close"), 4).alias("max_close"),
        F.sum("volume").alias("total_volume"),
        F.round(F.avg("daily_return_pct"), 4).alias("avg_daily_return"),
        F.round(F.avg("volatility_7d"), 4).alias("avg_volatility"),
        F.sum(F.when(F.col("daily_return_pct") > 0, 1).otherwise(0)).alias("positive_days"),
        F.sum(F.when(F.col("daily_return_pct") < 0, 1).otherwise(0)).alias("negative_days"),
        F.current_timestamp().alias("loaded_at")
    ) \
    .orderBy("ticker", "year", "month")

gold_monthly.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_monthly_summary")

print(f"✓ gold_monthly_summary: {gold_monthly.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold 3 – ranking spółek (Sharpe Ratio + overall stats)
gold_stats = df_silver \
    .groupBy("ticker") \
    .agg(
        F.round(F.sum("daily_return_pct"), 4).alias("total_return_pct"),
        F.round(F.avg("daily_return_pct"), 4).alias("avg_daily_return"),
        F.round(F.max("volatility_7d"), 4).alias("max_volatility"),
        F.round(F.avg("volatility_7d"), 4).alias("avg_volatility"),
        F.round(
            F.avg("daily_return_pct") / F.stddev("daily_return_pct"),
        4).alias("sharpe_ratio"),
        F.round(F.max("daily_return_pct"), 4).alias("best_day_return"),
        F.round(F.min("daily_return_pct"), 4).alias("worst_day_return"),
        F.current_timestamp().alias("loaded_at")
    ) \
    .orderBy(F.col("sharpe_ratio").desc())

gold_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_ticker_stats")

print(f"✓ gold_ticker_stats: {gold_stats.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
