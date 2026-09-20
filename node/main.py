from fastapi import FastAPI
import os

import httpx

app = FastAPI(title="LATTICE Storage Node")
DATA_DIR = "/app/data"

# How long a node waits when pinging a peer for the coordinator.
VERIFY_TIMEOUT = 2.0

os.makedirs(DATA_DIR, exist_ok=True)

@app.get("/")
def read_root():
    return {"status": "Storage node is running"}

@app.get("/ping")
def ping():
    """Simple ping endpoint for the coordinator to verify node health"""
    return {"status": "ok", "message": "pong from storage node"}


@app.get("/verify")
async def verify_peer(target: str):
    """Ping another node on the coordinator's behalf.

    The coordinator calls this on healthy nodes when it suspects a peer, so a
    failure is judged from more than one vantage point: if the coordinator
    can't reach a node but its peers can, the problem is the coordinator's
    link, not the node.

    `target` is a host:port on the cluster network, e.g. "node3:8000".
    """
    url = f"http://{target}/ping"
    try:
        async with httpx.AsyncClient(timeout=VERIFY_TIMEOUT) as client:
            response = await client.get(url)
        return {"target": target, "reachable": response.status_code == 200}
    except Exception as e:
        return {"target": target, "reachable": False, "error": str(e)}

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
