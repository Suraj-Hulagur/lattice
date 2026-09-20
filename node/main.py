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

def _object_path(object_name: str) -> str:
    """Resolve an object name to a path inside DATA_DIR, or reject it.

    Without this an object name like `../../etc/passwd` would escape the data
    directory -- which matters more now that the handoff pass can also delete.
    """
    file_path = os.path.normpath(os.path.join(DATA_DIR, object_name))
    if os.path.dirname(file_path) != os.path.normpath(DATA_DIR):
        raise HTTPException(status_code=400, detail="Invalid object name")
    return file_path


@app.put("/data/{object_name}")
async def upload_object(object_name: str, file: UploadFile = File(...)):
    """Store a file on disk"""
    file_path = _object_path(object_name)
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"message": "Object stored", "object_name": object_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/{object_name}")
def download_object(object_name: str):
    """Retrieve a file from disk"""
    file_path = _object_path(object_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Object not found")
    return FileResponse(file_path)


@app.delete("/data/{object_name}")
def delete_object(object_name: str):
    """Drop a local replica. Used to clear a hinted copy after handoff."""
    file_path = _object_path(object_name)
    if not os.path.exists(file_path):
        # Already gone is the desired end state, so this isn't an error.
        return {"message": "Object absent", "object_name": object_name}
    try:
        os.remove(file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": "Object deleted", "object_name": object_name}


@app.get("/data")
def list_objects():
    """Every object this node holds. The repair pass will want this."""
    return {"objects": sorted(os.listdir(DATA_DIR))}
