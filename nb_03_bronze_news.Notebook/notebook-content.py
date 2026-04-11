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
from datetime import datetime, timedelta

try:
    API_KEY = notebookutils.runtime.context.get("API_KEY", "")
    if not API_KEY:
        API_KEY = "xxxxx"  
except:
    API_KEY = "xxxxx" 

TICKERS = {
    "AAPL": "Apple",
    "MSFT": "Microsoft", 
    "GOOGL": "Google",
    "NVDA": "NVIDIA",
    "TSLA": "Tesla",
    "META": "Meta",
    "AMZN": "Amazon",
    "JPM": "JPMorgan",
    "BAC": "Bank of America",
    "GS": "Goldman Sachs"
}


end_date = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S')

all_articles = []

for ticker, company in TICKERS.items():
    print(f"Fetching news for {company}...")
    
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": f"{company} stock",
        "from": start_date,
        "to": end_date,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 20,
        "apiKey": API_KEY
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if data.get("status") == "ok":
        for article in data.get("articles", []):
            all_articles.append({
                "ticker": ticker,
                "company": company,
                "published_at": article.get("publishedAt"),
                "title": article.get("title"),
                "description": article.get("description"),
                "source": article.get("source", {}).get("name"),
                "url": article.get("url")
            })
        print(f"  ✓ {len(data.get('articles', []))} articles")
    else:
        print(f"  ✗ Error: {data.get('message')}")

df_news = pd.DataFrame(all_articles)
print(f"\nTotal articles: {len(df_news)}")
display(df_news.head())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F


spark_df_news = spark.createDataFrame(df_news)

spark_df_news = spark_df_news \
    .withColumn("published_at", F.to_timestamp("published_at")) \
    .withColumn("ingested_at", F.current_timestamp())

spark_df_news.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_news")

print(f"✓ bronze_news saved: {spark_df_news.count()} rows")
spark_df_news.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from textblob import TextBlob
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import pandas as pd


df_news_bronze = spark.table("bronze_news").toPandas()


def get_polarity(text):
    try:
        return float(TextBlob(str(text)).sentiment.polarity)
    except:
        return 0.0

def get_subjectivity(text):
    try:
        return float(TextBlob(str(text)).sentiment.subjectivity)
    except:
        return 0.0


df_news_bronze["text_combined"] = (
    df_news_bronze["title"].fillna("") + " " + 
    df_news_bronze["description"].fillna("")
)

df_news_bronze["sentiment_polarity"]    = df_news_bronze["text_combined"].apply(get_polarity)
df_news_bronze["sentiment_subjectivity"] = df_news_bronze["text_combined"].apply(get_subjectivity)


def categorize(score):
    if score > 0.1:
        return "positive"
    elif score < -0.1:
        return "negative"
    else:
        return "neutral"

df_news_bronze["sentiment_label"] = df_news_bronze["sentiment_polarity"].apply(categorize)

print(f"✓ Sentiment scored: {len(df_news_bronze)} articles")
print(df_news_bronze[["ticker", "title", "sentiment_polarity", "sentiment_label"]].head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_silver_news = spark.createDataFrame(df_news_bronze[[
    "ticker", "company", "published_at", "title",
    "sentiment_polarity", "sentiment_subjectivity", 
    "sentiment_label", "source", "ingested_at"
]])

df_silver_news.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_news")

print(f"✓ silver_news saved: {df_silver_news.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
