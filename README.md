# Market Intelligence Platform

End-to-end financial analytics platform built on Microsoft Fabric, combining real-time stock data, news sentiment analysis, and M&G fund pricing into a unified Power BI dashboard delivered as a shareable app.

![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Trial-0078D4?logo=microsoft)
![Power BI](https://img.shields.io/badge/Power%20BI-Direct%20Lake-F2C811?logo=powerbi)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?logo=apachespark)

---

## Architecture

```
Data Sources                Microsoft Fabric                    Delivery
────────────                ────────────────                    ────────
Yahoo Finance API      ──► Lakehouse Bronze (Delta)
NewsAPI                ──► Lakehouse Silver (cleaned)    ──►  Power BI Report
M&G Investments API    ──► Lakehouse Gold (aggregated)   ──►  Power BI App
                            │                                  │
                            Pipeline (daily 18:00 CET)         Shareable link
                            └── nb_01 → nb_02 → nb_03 → nb_04  Mobile ready
```
##Diagram


![Architecture](diagram.png)

### Medallion Architecture

```
Bronze (raw)          Silver (cleaned)        Gold (aggregated)
────────────          ────────────────        ─────────────────
bronze_prices    ──► silver_prices       ──► gold_daily_prices
bronze_news      ──► silver_news         ──► gold_monthly_summary
bronze_mandg          (sentiment scored)     gold_ticker_stats
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Storage | Microsoft Fabric Lakehouse (OneLake) | Bronze/Silver/Gold Delta tables |
| Processing | Apache Spark (PySpark) | Data transformation, window functions |
| Orchestration | Fabric Data Pipeline | Daily automated refresh at 18:00 CET |
| BI & Viz | Power BI (Direct Lake mode) | 3-page dashboard |
| Delivery | Power BI App | Shareable link, no installation needed |
| Languages | Python, PySpark, DAX, T-SQL | End-to-end stack |
| NLP | TextBlob | News sentiment scoring |

---

## Data Sources

### 1. Yahoo Finance (via yfinance)
- **11 tickers:** AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, JPM, BAC, GS, SPY
- **History:** 2 years of daily OHLCV data (5,489 rows)
- **Refresh:** Daily via pipeline

### 2. NewsAPI
- **Coverage:** Last 30 days of financial news per ticker
- **Volume:** 200 articles (20 per company)
- **Enrichment:** TextBlob sentiment scoring (polarity + subjectivity)

### 3. M&G Investments (reverse-engineered API)
- **Source:** `api.fundpress.io` discovered via Chrome DevTools → Network tab
- **Coverage:** 282 OEIC funds with NAV prices, price change %, yield
- **Update:** Daily at ~17:00 CET (M&G publication time)
- **Note:** Token-based authentication, refresh token from DevTools if expired

---

## Notebooks

| Notebook | Purpose | Output table |
|---|---|---|
| `nb_01_bronze_ingestion` | Yahoo Finance → Lakehouse | `bronze_prices` |
| `nb_02_silver_transform` | PySpark transformations + Gold load | `silver_prices`, `gold_*` |
| `nb_03_bronze_news` | NewsAPI + TextBlob sentiment | `bronze_news`, `silver_news` |
| `nb_04_bronze_mandg` | M&G API scraper | `bronze_mandg_funds` |

---

## Silver Transformations

Computed in PySpark using Window functions:

```python
window_ticker = Window.partitionBy("ticker").orderBy("date")

df_silver = df_bronze \
    .withColumn("daily_return_pct",
        F.round((col("close") - lag("close",1).over(w)) / lag("close",1).over(w) * 100, 2)) \
    .withColumn("volatility_7d",
        F.round(F.stddev("daily_return_pct").over(w.rowsBetween(-6, 0)), 4)) \
    .withColumn("volume_change_pct", ...) \
    .withColumn("price_range_pct", ...)
```

---

## Gold Layer (Aggregated)

Three Gold tables optimized for Power BI:

- **`gold_daily_prices`** – Clean daily prices with all metrics (5,478 rows)
- **`gold_monthly_summary`** – Monthly aggregates per ticker (positive/negative days, avg volatility)
- **`gold_ticker_stats`** – Overall ranking with Sharpe Ratio per ticker

---

## Power BI Dashboard

### Page 1 – Market Overview
- KPI cards: Avg Close Price, Avg Daily Return %, Avg Volatility, Total Volume
- Line chart: price trend for all 11 tickers (2 years)
- Bar chart: ranking by avg daily return
- Slicer: ticker filter (Tile style)

### Page 2 – Sentiment Analysis
- KPI cards: Avg Sentiment score, Positive News %
- Bar chart: sentiment per ticker with conditional formatting (green/red)
- Table: latest news headlines with sentiment label and score

### Page 3 – M&G Funds
- KPI cards: Avg NAV Price, Avg Price Change %
- Bar chart: Top 15 funds by NAV price
- Slicer: Asset Class filter (Convertibles, Equities, Fixed Income, Multi Asset, Real Estate)

---

## Pipeline

Automated daily pipeline in Fabric Data Factory:

```
18:00 CET
└── Bronze Yahoo Finance (nb_01)
    └── Silver Transform + Gold Load (nb_02)
        └── Bronze News + Sentiment (nb_03)
            └── Bronze M&G Funds (nb_04)
```

- Failure notifications via email
- Schedule: daily, timezone Warsaw (UTC+1/UTC+2)
- End date: April 2027

---

## Power BI App

Dashboard published as a Power BI App – accessible via browser link, no Power BI installation required:

- Direct Lake storage mode (reads Delta files directly from OneLake)
- Shareable link per workspace audience
- Mobile layout available

---

## Key Insights

**NVDA vs TSLA risk-return tradeoff**
NVDA delivered the second-highest avg daily return (0.19%) with lower volatility (9.33) than TSLA (0.22% return, 11.28 volatility) — better risk-adjusted performance over the 2-year period.

**M&G UK Income Distribution Fund**
Highest NAV price among all 282 OEIC funds scraped from M&G Investments public API.

**Market sentiment**
All 11 monitored tickers show positive average news sentiment over the last 30 days. BAC and GS have the lowest scores, closest to neutral.

---

## Quick Start

### Prerequisites
- Microsoft Fabric workspace (Trial or capacity F2+)
- NewsAPI key (free tier: newsapi.org)
- M&G API token (from Chrome DevTools → Network → api.fundpress.io request headers)

### Setup

```bash
# 1. Clone repo
git clone https://github.com/MarcinPaszko/market-intelligence-platform.git

# 2. Create Fabric workspace: market-intelligence-platform
# 3. Create Lakehouse: lh_market_intelligence
# 4. Import notebooks to Fabric workspace
# 5. Configure API keys in nb_03 (NewsAPI) and nb_04 (M&G token)
# 6. Run notebooks in order: nb_01 → nb_02 → nb_03 → nb_04
# 7. Create Data Pipeline: pl_daily_market_refresh
# 8. Connect Power BI Desktop to semantic model
# 9. Publish report and create App
```

---

## Project Structure

```
market-intelligence-platform/
├── notebooks/
│   ├── nb_01_bronze_ingestion.ipynb
│   ├── nb_02_silver_transform.ipynb
│   ├── nb_03_bronze_news.ipynb
│   └── nb_04_bronze_mandg.ipynb
├── powerbi/
│   └── FabricPB.pbix
├── docs/
│   ├── market_overview.png
│   ├── sentiment_analysis.png
│   └── mandg_funds.png
└── README.md
```

---

## Roadmap

- [ ] ML predictions – RandomForest trend UP/DOWN per ticker
- [ ] Data Activator alerts – price drop >3% triggers email/Teams notification
- [ ] KQL Database – real-time streaming prices via Eventstream
- [ ] FinBERT – production-grade financial sentiment model (vs TextBlob)
- [ ] M&G token auto-refresh – headless browser session management

---

## Author

Built by **Marcin Paszko** as a portfolio project demonstrating Microsoft Fabric capabilities combined with financial domain expertise from 10 years in fund accounting (State Street, UBS).

**Stack demonstrated:** Microsoft Fabric · PySpark · Delta Lake · Power BI Direct Lake · DAX · Python · REST APIs · Web scraping · NLP · Data Pipeline orchestration
