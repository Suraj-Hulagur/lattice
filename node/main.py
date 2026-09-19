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

from fastapi import UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import shutil

@app.put("/data/{object_name}")
async def upload_object(object_name: str, file: UploadFile = File(...)):
    """Store a file on disk"""
    file_path = os.path.join(DATA_DIR, object_name)
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"message": "Object stored", "object_name": object_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/{object_name}")
def download_object(object_name: str):
    """Retrieve a file from disk"""
    file_path = os.path.join(DATA_DIR, object_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Object not found")
    return FileResponse(file_path)
