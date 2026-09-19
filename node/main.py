from fastapi import FastAPI
import os

app = FastAPI(title="LATTICE Storage Node")
DATA_DIR = "/app/data"

os.makedirs(DATA_DIR, exist_ok=True)

@app.get("/")
def read_root():
    return {"status": "Storage node is running"}

@app.get("/ping")
def ping():
    """Simple ping endpoint for the coordinator to verify node health"""
    return {"status": "ok", "message": "pong from storage node"}
