import os
import shutil
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
import httpx

# Where a node keeps its objects. Inside docker-compose this is the mounted
# volume; the override lets a test stand several nodes up side by side.
DATA_DIR = os.environ.get("LATTICE_DATA_DIR", "/app/data")

# How long a node waits when pinging a peer for the coordinator.
VERIFY_TIMEOUT = 2.0


def create_app(data_dir: str = None) -> FastAPI:
    """Build a storage node rooted at `data_dir`.

    A factory rather than a module-level app so a test can run a whole cluster
    in one process, each node with its own directory. Docker still gets a
    single app from the module-level `app` below.
    """
    data_dir = data_dir or DATA_DIR
    os.makedirs(data_dir, exist_ok=True)

    # Built once and kept: constructing an httpx client loads the system trust
    # store, which costs far more than the ping it is being used for, and the
    # coordinator asks for a peer check on every sweep during a failure.
    peer_client = {"client": None}

    def peers() -> httpx.AsyncClient:
        if peer_client["client"] is None or peer_client["client"].is_closed:
            peer_client["client"] = httpx.AsyncClient(timeout=VERIFY_TIMEOUT)
        return peer_client["client"]

    @asynccontextmanager
    async def lifespan(_app):
        yield
        if peer_client["client"] is not None:
            await peer_client["client"].aclose()

    app = FastAPI(title="LATTICE Storage Node", lifespan=lifespan)

    def _object_path(object_name: str) -> str:
        """Resolve an object name to a path inside `data_dir`, or reject it.

        Without this an object name like `../../etc/passwd` would escape the
        data directory -- which matters more now that the handoff and repair
        passes can also delete.
        """
        file_path = os.path.normpath(os.path.join(data_dir, object_name))
        if os.path.dirname(file_path) != os.path.normpath(data_dir):
            raise HTTPException(status_code=400, detail="Invalid object name")
        return file_path

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

        The coordinator calls this on healthy nodes when it suspects a peer, so
        a failure is judged from more than one vantage point: if the
        coordinator can't reach a node but its peers can, the problem is the
        coordinator's link, not the node.

        `target` is a host:port on the cluster network, e.g. "node3:8000".
        """
        url = f"http://{target}/ping"
        try:
            response = await peers().get(url)
            return {"target": target, "reachable": response.status_code == 200}
        except Exception as e:
            return {"target": target, "reachable": False, "error": str(e)}

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
        return {"objects": sorted(os.listdir(data_dir))}

    return app


_app = None


def __getattr__(name: str):
    """Build the module-level `app` the first time something asks for it.

    `uvicorn node.main:app` is unchanged, but importing create_app() from a
    test no longer creates the container's data directory as a side effect of
    the import.
    """
    if name == "app":
        global _app
        if _app is None:
            _app = create_app()
        return _app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
