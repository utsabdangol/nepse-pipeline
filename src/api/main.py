from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pickle
import numpy as np
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv()

app = FastAPI(
    title="NEPSE Price Direction API",
    description="Predicts 5-day price direction for NEPSE stocks",
    version="1.0.0"
)

# load model once at startup
with open("models/nepse_model.pkl", "rb") as f:
    model_package = pickle.load(f)

model = model_package["model"]
features = model_package["features"]

print("Model loaded successfully")
print("Model type:", type(model))
print("Features:", features)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "nepse"),
        user=os.getenv("DB_USER", "nepse_user"),
        password=os.getenv("DB_PASSWORD", "nepse_pass")
    )


class PredictionRequest(BaseModel):
    symbol: str


class PredictionResponse(BaseModel):
    symbol: str
    prediction: str
    probability_up: float
    probability_down: float
    features_used: dict
    model_version: str
    disclaimer: str


@app.get("/")
def root():
    return {
        "message": "NEPSE Price Direction API",
        "version": "1.0.0",
        "endpoints": ["/predict", "/stock/{symbol}", "/health"]
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "model": model_package["model_version"],
        "accuracy": model_package["accuracy"]
    }


@app.post("/predict", response_model=PredictionResponse)
def predict(request: PredictionRequest):
    symbol = request.symbol.upper()

    # fetch latest features from gold layer
    conn = get_db_connection()
    query = """
        SELECT 
            symbol,
            scraped_date,
            ltp,
            avg_7_day,
            avg_30_day,
            max_30_day,
            min_30_day
        FROM gold_nepse_features
        WHERE symbol = %s
        ORDER BY scraped_date DESC
        LIMIT 10
    """
    df = pd.read_sql(query, conn, params=(symbol,))
    conn.close()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol '{symbol}' not found in database"
        )

    # get latest row
    latest = df.iloc[0]

    # calculate features
    price_vs_7day = latest['ltp'] / latest['avg_7_day']
    trend_strength = latest['avg_7_day'] / latest['avg_30_day']
    price_position = (
        (latest['ltp'] - latest['min_30_day']) /
        (latest['max_30_day'] - latest['min_30_day'] + 0.0001)
    )

    # calculate roc_10 from last 10 rows
    if len(df) >= 10:
        roc_10 = (df.iloc[0]['ltp'] - df.iloc[9]['ltp']) / df.iloc[9]['ltp']
    else:
        roc_10 = 0.0

    # calculate volatility_7
    if len(df) >= 3:
        returns = df['ltp'].pct_change().dropna()
        volatility_7 = returns.head(7).std()
        if pd.isna(volatility_7) or np.isinf(volatility_7):
            volatility_7 = 0.0
    else:
        volatility_7 = 0.0

    feature_values = {
        "price_vs_7day": round(float(price_vs_7day), 4),
        "trend_strength": round(float(trend_strength), 4),
        "price_position": round(float(price_position), 4),
        "roc_10": round(float(roc_10), 4),
        "volatility_7": round(float(volatility_7), 4)
    }

    # make prediction
    X_df = pd.DataFrame([feature_values])
    X_df = X_df[features]

    prediction = model.predict(X_df)[0]
    probabilities = model.predict_proba(X_df)[0]

    return PredictionResponse(
        symbol=symbol,
        prediction="up" if prediction == 1 else "down",
        probability_up=round(float(probabilities[1]), 3),
        probability_down=round(float(probabilities[0]), 3),
        features_used=feature_values,
        model_version=model_package["model_version"],
        disclaimer="This is not financial advice. Model accuracy is ~52%. NEPSE is sentiment-driven and unpredictable."
    )


@app.get("/stock/{symbol}")
def get_stock_history(symbol: str, days: int = 30):
    symbol = symbol.upper()

    conn = get_db_connection()
    query = """
        SELECT symbol, scraped_date, ltp, avg_7_day, avg_30_day
        FROM gold_nepse_features
        WHERE symbol = %s
        ORDER BY scraped_date DESC
        LIMIT %s
    """
    df = pd.read_sql(query, conn, params=(symbol, days))
    conn.close()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol '{symbol}' not found"
        )

    # Convert date/datetime columns to strings for JSON serialization
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].apply(lambda x: isinstance(x, date)).any():
            df[col] = df[col].astype(str)

    return {
        "symbol": symbol,
        "days": len(df),
        "latest_price": float(df.iloc[0]['ltp']),
        "latest_date": str(df.iloc[0]['scraped_date']),
        "data": df.to_dict(orient='records')
    }