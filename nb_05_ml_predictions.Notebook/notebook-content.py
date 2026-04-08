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

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import mlflow
import mlflow.sklearn
import warnings
warnings.filterwarnings('ignore')


df = spark.table("silver_prices").toPandas()
df = df.sort_values(["ticker", "trade_date"]).reset_index(drop=True)

print(f"✓ Loaded: {len(df)} rows, {df['ticker'].nunique()} tickers")
print(df[["ticker", "trade_date", "close", "daily_return_pct", "volatility_7d"]].head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tworzenie features i labela
def create_features(df):
    result = []
    
    for ticker in df['ticker'].unique():
        df_t = df[df['ticker'] == ticker].copy().reset_index(drop=True)
        
        # Label Y – zwrot za 3 dni w przód
        df_t['future_return'] = df_t['close'].shift(-3) / df_t['close'] - 1
        df_t['future_return_pct'] = df_t['future_return'] * 100
        
        # Kategoryzacja
        def categorize(x):
            if pd.isna(x):
                return None
            elif x > 1.0:
                return 'UP'
            elif x < -1.0:
                return 'DOWN'
            else:
                return 'NEUTRAL'
        
        df_t['label'] = df_t['future_return_pct'].apply(categorize)
        result.append(df_t)
    
    df_all = pd.concat(result, ignore_index=True)
    return df_all

df_featured = create_features(df)

# Usuń nulle
df_ml = df_featured.dropna(subset=[
    'daily_return_pct', 'volatility_7d', 
    'volume_change_pct', 'price_range_pct', 'label'
])

print(f"✓ ML dataset: {len(df_ml)} rows")
print(f"Label distribution:\n{df_ml['label'].value_counts()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Features i label
FEATURES = ['daily_return_pct', 'volatility_7d', 
            'volume_change_pct', 'price_range_pct']

X = df_ml[FEATURES]
y = df_ml['label']

# Split 80/20 – shuffle=False zachowuje chronologię
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, shuffle=False
)

print(f"✓ Train: {len(X_train)} rows")
print(f"✓ Test:  {len(X_test)} rows")

# Trening bez MLflow
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=10,
    random_state=42,
    class_weight='balanced'
)
model.fit(X_train, y_train)

# Ewaluacja
y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)
accuracy = accuracy_score(y_test, y_pred)

print(f"\n✓ Accuracy: {accuracy:.2%}")
print(f"\nClassification Report:")
print(classification_report(y_test, y_pred))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

# Predykcje na CAŁYM datasecie
X_all = df_ml[FEATURES]
df_ml = df_ml.copy()
df_ml['predicted_trend'] = model.predict(X_all)
df_ml['confidence'] = model.predict_proba(X_all).max(axis=1).round(4)

# Feature importance
fi = pd.DataFrame({
    'feature': FEATURES,
    'importance': model.feature_importances_.round(4)
}).sort_values('importance', ascending=False)

print("Feature importance:")
print(fi)

# Zapisz do Gold
df_predictions = df_ml[[
    'ticker', 'trade_date', 'close',
    'daily_return_pct', 'volatility_7d',
    'label', 'predicted_trend', 'confidence'
]].copy()

df_predictions['model_version'] = 'random_forest_v1'
df_predictions['accuracy'] = round(accuracy, 4)
df_predictions['trained_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

spark_pred = spark.createDataFrame(df_predictions)

spark_pred.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_ml_predictions")

print(f"\n✓ gold_ml_predictions saved: {spark_pred.count()} rows")
display(df_predictions[['ticker', 'trade_date', 'predicted_trend', 'confidence', 'label']].head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
