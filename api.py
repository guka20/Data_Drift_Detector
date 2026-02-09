from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from clickhouse_driver import Client
from typing import List, Dict, Any

app = FastAPI(title="Real-Time Drift Detection API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Clickhouse–ის კონფიგურაცია
CH_CONFIG = {
    "host": "localhost",
    "port": 9000,
    "database": "default",
    "user": "default",
    "password": "1234"
}

# მხოლოდ და მხოლოდ ყველა endpoint-ის ნახვა და API-ის მუშაობის შემოწმება
@app.get("/")
def root():
    return {
        "status": "running",
        "message": "Real-Time Drift Detection API",
        "endpoints": [
            "/drift/current",
            "/drift/history",
            "/drift/stats",
            "/sources"
        ]
    }

# მივიღოთ ის მონაცემები რომლებიც უკვე DRIFTING რეჟიმშია
@app.get("/drift/current")
def get_current_drift():
    client = Client(**CH_CONFIG)
    
    # ვიპოვოთ უახლესი DRIFT რეჟიმში მყოფი მონაცემები
    query = """
        WITH latest_events AS (
            SELECT 
                source_id,
                metric,
                drift_status,
                detected_at,
                z_score,
                ROW_NUMBER() OVER (PARTITION BY source_id, metric ORDER BY detected_at DESC) as rn
            FROM drift_events
        )
        SELECT 
            source_id,
            metric,
            drift_status,
            detected_at,
            z_score
        FROM latest_events
        WHERE rn = 1 AND drift_status = 'STARTED'
    """
    
    results = client.execute(query)
    
    return {
        "currently_drifting": [
            {
                "source_id": r[0],
                "metric": r[1],
                "status": "DRIFTING",
                "drift_started_at": str(r[3]),
                "z_score": round(r[4], 2)
            }
            for r in results
        ],
        "count": len(results)
    }
# ბოლოს 100 იმ მონაცემის მიღება რომელმაც Drift რეჟიმი გამოიწვია. 
@app.get("/drift/history")
def get_drift_history(limit: int = 100):
    client = Client(**CH_CONFIG)
    
    query = f"""
        SELECT 
            source_id,
            metric,
            drift_status,
            detected_at,
            z_score
        FROM drift_events
        ORDER BY detected_at DESC
        LIMIT {limit}
    """
    
    results = client.execute(query)
    
    return {
        "drift_events": [
            {
                "source_id": r[0],
                "metric": r[1],
                "status": r[2],
                "detected_at": str(r[3]),
                "z_score": round(r[4], 2)
            }
            for r in results
        ],
        "count": len(results)
    }

# Drfit-ის სტატისტიკა
@app.get("/drift/stats")
def get_drift_stats():
    client = Client(**CH_CONFIG)
    
    query = """
        SELECT 
            source_id,
            metric,
            drift_status,
            count() as event_count,
            avg(z_score) as avg_z_score,
            max(z_score) as max_z_score
        FROM drift_events
        GROUP BY source_id, metric, drift_status
        ORDER BY source_id, metric, drift_status
    """
    
    results = client.execute(query)
    
    return {
        "statistics": [
            {
                "source_id": r[0],
                "metric": r[1],
                "status": r[2],
                "event_count": r[3],
                "avg_z_score": round(r[4], 2),
                "max_z_score": round(r[5], 2)
            }
            for r in results
        ]
    }
