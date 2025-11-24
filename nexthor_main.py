# Nexthor Ai Form D Burn API – Deal Sourcing & Due Diligence Hub
import json
import os
import pandas as pd
import plotly.express as px
import base64
from io import BytesIO
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from functools import wraps
import time
import hashlib
from sqlalchemy import create_engine, text

app = FastAPI(title="Nexthor Ai Form D Burn API", description="Private raises with runway scores for deal sourcing")

# CORS for OpenBB/Bubble
app.add_middleware(CORSMiddleware, allow_origins=["https://pro.openbb.co", "*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- DATABASE CONNECTION FIX (The Critical Update) ---
DB_URL = os.getenv("DB_URL")

# Fallback for local testing if Env Var is missing
if not DB_URL:
    print("⚠️ WARNING: No DB_URL found. Using local SQLite.")
    # Keep your local path for testing on your machine
    DB_URL = "sqlite:///C:/Users/super/Desktop/Nexthor Ai/reg_d_treasure.db"

# Fix: Add pool_pre_ping=True to handle Supabase disconnects
if "sqlite" in DB_URL:
    engine = create_engine(DB_URL)
else:
    # Production: Ensure SSL is required and pool recycling is active
    engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=300)

CACHE_DURATION = 5 * 60  # 5 min cache
cache_store = {}

def create_cache_key(func_name: str, **kwargs) -> str:
    key_parts = [func_name]
    for k, v in sorted(kwargs.items()):
        key_parts.append(f"{k}={str(v)}") # Ensure value is string
    return hashlib.md5("|".join(key_parts).encode()).hexdigest()

def cache_response(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract query params from kwargs for cache key
        cache_key = create_cache_key(func.__name__, **kwargs)
        if cache_key in cache_store:
            data, timestamp = cache_store[cache_key]
            if time.time() - timestamp < CACHE_DURATION:
                return data
            del cache_store[cache_key]
        result = func(*args, **kwargs)
        cache_store[cache_key] = (result, time.time())
        return result
    return wrapper

@app.get("/latest_filings")
@cache_response
def latest_filings(limit: int = Query(10, ge=1, le=100)):
    # Fix: Explicitly select columns to match DataFrame expectations if needed
    query = text("SELECT * FROM filings ORDER BY filing_date DESC LIMIT :limit")
    try:
        df = pd.read_sql(query, engine, params={"limit": limit})
        # Handle empty DB case gracefully
        if df.empty:
            return []
        # Convert date objects to string for JSON serialization
        if 'filing_date' in df.columns:
            df['filing_date'] = df['filing_date'].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"Database Error: {e}")
        return {"error": "Database connection failed", "details": str(e)}

@app.get("/high_burn_leads")
@cache_response
def high_burn_leads(min_score: int = Query(70, ge=0, le=100), industry: str = None):
    query_str = "SELECT * FROM filings WHERE ai_score >= :min_score"
    params = {"min_score": min_score}
    
    if industry:
        query_str += " AND company_name ILIKE :industry" # Postgres ILIKE for case-insensitive
        params["industry"] = f"%{industry}%"
        
    query = text(query_str)
    try:
        df = pd.read_sql(query, engine, params=params)
        if df.empty:
            return []
        if 'filing_date' in df.columns:
            df['filing_date'] = df['filing_date'].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"Database Error: {e}")
        return {"error": "Database connection failed"}

@app.get("/security_types_pie")
@cache_response
def security_pie(year: str = "all"):
    # Fix: Adjusted query for Postgres compatibility
    query = text("""
        SELECT 
            CASE 
                WHEN raise_amount LIKE '%K' THEN 'Small Raise (<$1M)' 
                WHEN raise_amount LIKE '%M' THEN 'Medium Raise ($1M-$10M)' 
                ELSE 'Large Raise (>$10M)' 
            END as type, 
            COUNT(*) as count 
        FROM filings 
        GROUP BY 1
    """)
    try:
        df = pd.read_sql(query, engine)
        if df.empty:
            # Return a placeholder if no data exists yet
            df = pd.DataFrame([{'type': 'No Data', 'count': 1}])
            
        fig = px.pie(df, names='type', values='count', title="Equity/Debt/Fund Breakdown")
        img_bytes = fig.to_image(format="png")
        encoded = base64.b64encode(img_bytes).decode()
        return {"image_b64": encoded}
    except Exception as e:
        print(f"Viz Error: {e}")
        return {"error": "Visualization failed"}

# OpenBB Widgets
@app.get("/widgets.json")
def widgets():
    return [
        {"type": "markdown", "content": "# Nexthor Ai Burn Index\nPrivate raises scored for funding need (0-100%)."},
        {"type": "table", "endpoint": "/latest_filings?limit=20"},
        {"type": "chart", "endpoint": "/security_types_pie"}
    ]

@app.get("/apps.json")
def apps():
    return [{"name": "Nexthor Burn Dashboard", "widgets": ["/widgets.json"]}]

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    print(f"🚀 Nexthor Ai API starting on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
