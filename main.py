# main.py

from fastapi import FastAPI
from typing import List, Dict

# 1. Initialize the FastAPI app
app = FastAPI(
    title="Data Engineering Source API",
    description="API for fetching mock data for ingestion into ADLS/Blob Storage.",
    version="1.0.0"
)

# --- 2. Mock Data Source (Simulates your actual data) ---
# This list simulates data records that your data engineering pipeline will consume.
MOCK_DATA = [
    {"record_id": 101, "name": "Sensor_X", "value": 45.2, "unit": "Celcius", "timestamp": "2025-10-24T18:00:00Z"},
    {"record_id": 102, "name": "Sensor_Y", "value": 88.9, "unit": "Humidity", "timestamp": "2025-10-24T18:05:00Z"},
    {"record_id": 103, "name": "Sensor_Z", "value": 1.5, "unit": "Pressure", "timestamp": "2025-10-24T18:10:00Z"},
]


# --- 3. Define API Endpoints (Routes) ---

@app.get("/", tags=["Health"])
def read_root():
    """
    A simple health check endpoint to confirm the API is running.
    """
    return {"message": "Data Engineering API is running successfully!"}

@app.get("/api/v1/data", tags=["Data"], response_model=List[Dict])
def get_all_data():
    """
    Retrieves all available data records for ingestion by the data pipeline.
    """
    # In a real project, this is where you'd connect to your source system (DB, etc.)
    return MOCK_DATA

# main.py (New addition)

@app.get("/api/v1/data_filtered", tags=["Data"], response_model=List[Dict])
def get_filtered_data(start_id: int = 101, limit: int = 2):
    """
    Retrieves data based on filters, useful for pagination or batch processing.
    
    - **start_id**: The starting record_id to fetch (inclusive).
    - **limit**: The maximum number of records to return.
    """
    
    # 1. Filter the data based on the starting ID
    filtered_data = [
        item for item in MOCK_DATA 
        if item["record_id"] >= start_id
    ]
    
    # 2. Apply the limit (pagination)
    final_data = filtered_data[:limit]
    
    return final_data