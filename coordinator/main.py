import asyncio

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import Response
import httpx

from common.models import NodeInfo, NodeState
from coordinator.hashing import ConsistentHashRing
from coordinator import placement

app = FastAPI(title="LATTICE Coordinator")

# Static discovery of the 8 storage nodes based on docker-compose service names
NODE_ADDRESSES = [f"node{i}:8000" for i in range(1, 9)]

# In-memory state of nodes
nodes = {
    f"node{i}": NodeInfo(id=f"node{i}", address=f"node{i}:8000", state=NodeState.HEALTHY)
    for i in range(1, 9)
}

ring = ConsistentHashRing()
for node_id in nodes:
    ring.add_node(node_id)

# object_name -> list of node ids actually holding a replica.
# Repair (phase 8) needs to know where things ended up, not just where the ring
# would have put them.
placement_index = {}


@app.get("/")
def read_root():
    return {"status": "Coordinator is running"}


@app.get("/nodes")
def get_nodes():
    return nodes


@app.get("/test-ping")
async def test_ping(node_id: str):
    """Test endpoint to manually trigger a ping from coordinator to a node"""
    if node_id not in nodes:
        raise HTTPException(status_code=404, detail="Node not found")

    node = nodes[node_id]
    url = f"http://{node.address}/ping"

    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(url)
            if response.status_code == 200:
                return {"status": "success", "node": node_id, "response": response.json()}
            else:
                return {"status": "failed", "node": node_id, "status_code": response.status_code}
    except Exception as e:
        return {"status": "error", "node": node_id, "detail": str(e)}


async def _put_replica(client: httpx.AsyncClient, node_id: str, object_name: str, payload: bytes):
    """Store one replica. Returns True on success, False on any failure."""
    import os
    node_address = nodes[node_id].address
    if not os.environ.get("DOCKER_ENV"):
        node_address = "localhost:8000"
    url = f"http://{node_address}/data/{object_name}"
    try:
        response = await client.put(url, files={"file": (object_name, payload)})
        return response.status_code == 200
    except Exception:
        return False


@app.put("/objects/{object_name}")
async def upload_object(object_name: str, file: UploadFile = File(...)):
    """Upload an object to 3 distinct nodes chosen by the hash ring."""
    targets = placement.select_replicas(ring, object_name, nodes)
    if len(targets) < placement.WRITE_QUORUM:
        raise HTTPException(
            status_code=503,
            detail=f"Only {len(targets)} healthy nodes available, need {placement.WRITE_QUORUM}",
        )

    # Buffered in memory so the same bytes can be sent to every replica; an
    # UploadFile stream can only be consumed once.
    payload = await file.read()

    async with httpx.AsyncClient(timeout=10.0) as client:
        results = await asyncio.gather(
            *(_put_replica(client, node_id, object_name, payload) for node_id in targets)
        )

    stored = [node_id for node_id, ok in zip(targets, results) if ok]
    failed = [node_id for node_id, ok in zip(targets, results) if not ok]

    if len(stored) < placement.WRITE_QUORUM:
        raise HTTPException(
            status_code=500,
            detail=f"Write failed: only {len(stored)} of {len(targets)} replicas stored",
        )

    placement_index[object_name] = stored

    return {
        "message": "Object uploaded successfully",
        "object_name": object_name,
        "replicas": stored,
        "failed_replicas": failed,
        "replication_factor": placement.REPLICATION_FACTOR,
        "size": len(payload),
    }


@app.get("/objects/{object_name}")
async def download_object(object_name: str):
    """Download an object, trying each replica in turn until one answers."""
    candidates = placement.read_order(ring, object_name, nodes)
    if not candidates:
        raise HTTPException(status_code=503, detail="No storage nodes available")

    attempted = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        for node_id in candidates[: placement.REPLICATION_FACTOR]:
            attempted.append(node_id)
            import os
            node_address = nodes[node_id].address
            if not os.environ.get("DOCKER_ENV"):
                node_address = "localhost:8000"
            url = f"http://{node_address}/data/{object_name}"
            try:
                response = await client.get(url)
            except Exception:
                continue  # node unreachable -- fall through to the next replica
            if response.status_code == 200:
                return Response(
                    content=response.content,
                    media_type="application/octet-stream",
                    headers={
                        "Content-Disposition": f"attachment; filename={object_name}",
                        "X-Served-By": node_id,
                    },
                )

    raise HTTPException(
        status_code=404,
        detail=f"Object not found on any replica (tried: {', '.join(attempted)})",
    )


@app.get("/objects/{object_name}/placement")
def get_placement(object_name: str):
    """Where the ring would put an object, and where it actually lives."""
    return {
        "object_name": object_name,
        "preference_list": ring.get_nodes(object_name, count=placement.REPLICATION_FACTOR),
        "stored_on": placement_index.get(object_name, []),
    }
