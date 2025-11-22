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

# Supabase/SQLite DB (env var)
DB_URL = os.getenv("DB_URL", "sqlite:///C:/Users/super/Desktop/Nexthor Ai/reg_d_treasure.db")
engine = create_engine(DB_URL)

CACHE_DURATION = 5 * 60  # 5 min cache
cache_store = {}

def create_cache_key(func_name: str, **kwargs) -> str:
    key_parts = [func_name]
    for k, v in sorted(kwargs.items()):
        key_parts.append(f"{k}={v}")
    return hashlib.md5("|".join(key_parts).encode()).hexdigest()

def cache_response(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
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
    query = text("SELECT * FROM filings ORDER BY filing_date DESC LIMIT :limit")
    df = pd.read_sql(query, engine, params={"limit": limit})
    return df.to_dict(orient="records")  # Paginated raises for due diligence

@app.get("/high_burn_leads")
@cache_response
def high_burn_leads(min_score: int = Query(70, ge=0, le=100), industry: str = None):
    query = text("SELECT * FROM filings WHERE ai_score >= :min_score")
    params = {"min_score": min_score}
    if industry:
        query = text(query.text + " AND company_name LIKE :industry")
        params["industry"] = f"%{industry}%"
    df = pd.read_sql(query, engine, params=params)
    return df.to_dict(orient="records")  # Moat: Scored leads for founder outreach

@app.get("/security_types_pie")
@cache_response
def security_pie(year: str = "all"):
    query = text("""
                        SELECT CASE 
                        WHEN raise_amount LIKE '%K' THEN 'Small Raise (<$1M)' 
                        WHEN raise_amount LIKE '%M' THEN 'Medium Raise ($1M-$10M)' 
                        ELSE 'Large Raise (>$10M)' END as type, COUNT(*) as count FROM filings 
                        GROUP BY CASE 
                        WHEN raise_amount LIKE '%K' THEN 'Small Raise (<$1M)' 
                        WHEN raise_amount LIKE '%M' THEN 'Medium Raise ($1M-$10M)' 
                        ELSE 'Large Raise (>$10M)' END
                """)
    df = pd.read_sql(query, engine)
    fig = px.pie(df, names='type', values='count', title="Equity/Debt/Fund Breakdown")
    img_bytes = fig.to_image(format="png")
    encoded = base64.b64encode(img_bytes).decode()
    return {"image_b64": encoded}  # Embeddable chart

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