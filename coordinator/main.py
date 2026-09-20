from fastapi import FastAPI, HTTPException
import httpx
import os
from common.models import NodeInfo, NodeState

app = FastAPI(title="LATTICE Coordinator")

# Static discovery of the 8 storage nodes based on docker-compose service names
NODE_ADDRESSES = [f"node{i}:8000" for i in range(1, 9)]

# In-memory state of nodes
nodes = {
    f"node{i}": NodeInfo(id=f"node{i}", address=f"node{i}:8000", state=NodeState.HEALTHY)
    for i in range(1, 9)
}

from coordinator.hashing import ConsistentHashRing
ring = ConsistentHashRing()
for node_id in nodes:
    ring.add_node(node_id)

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

from fastapi import UploadFile, File
from fastapi.responses import StreamingResponse

@app.put("/objects/{object_name}")
async def upload_object(object_name: str, file: UploadFile = File(...)):
    """Upload an object using consistent hashing."""
    node_id = ring.get_node(object_name)
    if not node_id:
        raise HTTPException(status_code=503, detail="No storage nodes available")
    
    node_address = nodes[node_id].address
    
    # In a local test environment without docker, fallback to localhost
    if not os.environ.get("DOCKER_ENV"):
        # Map nodeX:8000 to localhost:8000 for local testing
        node_address = "localhost:8000"
        
    url = f"http://{node_address}/data/{object_name}"
    
    try:
        async with httpx.AsyncClient() as client:
            # We stream the file content to the node
            response = await client.put(
                url, 
                files={"file": (file.filename, file.file, file.content_type)}
            )
            if response.status_code == 200:
                return {"message": "Object uploaded successfully", "node": node_id, "address": node_address, "object_name": object_name}
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to node: {str(e)}")

@app.get("/objects/{object_name}")
async def download_object(object_name: str):
    """Download an object using consistent hashing."""
    node_id = ring.get_node(object_name)
    if not node_id:
        raise HTTPException(status_code=503, detail="No storage nodes available")
        
    node_address = nodes[node_id].address
    
    if not os.environ.get("DOCKER_ENV"):
        node_address = "localhost:8000"
        
    url = f"http://{node_address}/data/{object_name}"
    
    # Use StreamingResponse to proxy the file back to the client
    client = httpx.AsyncClient()
    req = client.build_request("GET", url)
    r = await client.send(req, stream=True)
    if r.status_code != 200:
        await r.aclose()
        raise HTTPException(status_code=r.status_code, detail="Object not found on node")
        
    return StreamingResponse(
        r.aiter_raw(), 
        headers={"Content-Disposition": f"attachment; filename={object_name}"},
        background=r.aclose
    )

