from fastapi import FastAPI, HTTPException
import httpx
from common.models import NodeInfo, NodeState

app = FastAPI(title="LATTICE Coordinator")

# Static discovery of the 8 storage nodes based on docker-compose service names
NODE_ADDRESSES = [f"node{i}:8000" for i in range(1, 9)]

# In-memory state of nodes
nodes = {
    f"node{i}": NodeInfo(id=f"node{i}", address=f"node{i}:8000", state=NodeState.HEALTHY)
    for i in range(1, 9)
}

@app.get("/")
def read_root():
    return {"status": "Coordinator is running"}

@app.get("/nodes")
def get_nodes():
    return nodes

@app.post("/test-ping")
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
