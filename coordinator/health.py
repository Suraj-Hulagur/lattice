import asyncio
import httpx
import os
from common.models import NodeState

async def check_node_health(node, client):
    node_address = node.address
    if not os.environ.get("DOCKER_ENV"):
        node_address = "localhost:8000"
        
    url = f"http://{node_address}/ping"
    try:
        response = await client.get(url, timeout=2.0)
        if response.status_code == 200:
            if node.state != NodeState.HEALTHY:
                print(f"[HEALTH] Node {node.id} is healthy again.")
            node.state = NodeState.HEALTHY
            return
    except Exception:
        pass
        
    # If we get here, the node failed to respond
    if node.state == NodeState.HEALTHY:
        print(f"[HEALTH] Node {node.id} missed a ping, marking as SUSPECTED.")
        node.state = NodeState.SUSPECTED

async def health_check_loop(nodes: dict):
    print("[HEALTH] Starting background health check loop...")
    async with httpx.AsyncClient() as client:
        while True:
            await asyncio.gather(*(check_node_health(node, client) for node in nodes.values()))
            await asyncio.sleep(5)
