"""
FastAPI Serving Layer — Auto-Correcting Query Merger API Routes
"""
from fastapi import FastAPI

app = FastAPI(
    title="Lambda Lakehouse Serving API",
    description="Auto-Correcting Query Merger for Batch & Speed Views",
    version="0.1.0"
)

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "serving_layer"}

@app.get("/")
async def root():
    return {"message": "Lambda Lakehouse Serving Layer API"}
