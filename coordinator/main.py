import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import Response
import httpx

from common.models import NodeInfo, NodeState
from coordinator.hashing import ConsistentHashRing
from coordinator import health, hints, placement
from coordinator.addressing import node_endpoint


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run the failure detector for as long as the coordinator is up."""
    detector = asyncio.create_task(
        health.health_check_loop(nodes, after_sweep=handoff_pass)
    )
    try:
        yield
    finally:
        detector.cancel()
        try:
            await detector
        except asyncio.CancelledError:
            pass


app = FastAPI(title="LATTICE Coordinator", lifespan=lifespan)

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


@app.get("/health")
def cluster_health():
    """Cluster-level view of what the failure detector currently believes."""
    by_state = {}
    for node in nodes.values():
        by_state.setdefault(node.state.value, []).append(node.id)

    healthy = len(by_state.get(NodeState.HEALTHY.value, []))
    return {
        "nodes_total": len(nodes),
        "healthy": healthy,
        # Below quorum every write is rejected, so this is the number to watch.
        "writable": healthy >= placement.WRITE_QUORUM,
        "fully_replicated": healthy >= placement.REPLICATION_FACTOR,
        "by_state": by_state,
        "pending_hints": hints.count(),
        "misses": {
            node_id: health.miss_count(node_id)
            for node_id in nodes
            if health.miss_count(node_id)
        },
        "verdicts": {
            node_id: health.last_verdict(node_id)
            for node_id in nodes
            if health.last_verdict(node_id)
        },
    }


@app.post("/nodes/{node_id}/verify")
async def verify_node(node_id: str):
    """Ask other healthy nodes whether they can reach this one, on demand.

    The same check the sweep runs automatically, exposed so a failure can be
    corroborated without waiting for the next probe interval.
    """
    if node_id not in nodes:
        raise HTTPException(status_code=404, detail="Node not found")

    async with httpx.AsyncClient() as client:
        verdict = await health.verify_failure(nodes[node_id], nodes, client)
    return verdict


@app.get("/test-ping")
async def test_ping(node_id: str):
    """Test endpoint to manually trigger a ping from coordinator to a node"""
    if node_id not in nodes:
        raise HTTPException(status_code=404, detail="Node not found")

    node = nodes[node_id]
    url = f"http://{node_endpoint(node)}/ping"

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
    url = f"http://{node_endpoint(nodes[node_id])}/data/{object_name}"
    try:
        response = await client.put(url, files={"file": (object_name, payload)})
        return response.status_code == 200
    except Exception:
        return False


async def _handoff_object(
    client: httpx.AsyncClient, object_name: str, holder: str, owner: str
) -> bool:
    """Move one hinted replica from its stand-in back to its real owner.

    Copy first, delete second: if the delete fails we're left with an extra
    replica, which the repair pass can tidy up. Doing it the other way round
    could lose the only copy on that side of the ring.
    """
    holder_url = f"http://{node_endpoint(nodes[holder])}/data/{object_name}"
    owner_url = f"http://{node_endpoint(nodes[owner])}/data/{object_name}"

    try:
        response = await client.get(holder_url)
        if response.status_code == 404:
            # The holder no longer has it, so nothing can be delivered. Drop the
            # hint rather than retrying it every sweep forever.
            print(f"[HANDOFF] {object_name}: holder {holder} lost its copy, dropping hint")
            hints.drop(object_name, owner)
            return False
        if response.status_code != 200:
            return False
        payload = response.content

        put = await client.put(owner_url, files={"file": (object_name, payload)})
        if put.status_code != 200:
            return False
    except Exception as e:
        print(f"[HANDOFF] {object_name}: {holder} -> {owner} failed, will retry ({e})")
        return False

    hints.drop(object_name, owner)

    stored = placement_index.get(object_name, [])
    if owner not in stored:
        stored.append(owner)

    try:
        await client.delete(holder_url)
        if holder in stored:
            stored.remove(holder)
    except Exception:
        print(f"[HANDOFF] {object_name}: delivered, but {holder} still has a stale copy")

    placement_index[object_name] = stored
    print(f"[HANDOFF] {object_name}: {holder} -> {owner}")
    return True


async def handoff_pass():
    """Deliver hinted replicas whose real owner is healthy again.

    Runs after every health sweep. Owners that are still down are skipped and
    picked up on a later pass.
    """
    ready = [owner for owner in hints.owners() if nodes[owner].state == NodeState.HEALTHY]
    if not ready:
        return

    async with httpx.AsyncClient(timeout=10.0) as client:
        for owner in ready:
            pending = hints.for_owner(owner)
            print(f"[HANDOFF] {owner} is back, {len(pending)} replica(s) to deliver")
            for object_name, holder in pending:
                await _handoff_object(client, object_name, holder, owner)


@app.get("/hints")
def get_hints():
    """Replicas currently parked on a stand-in node, waiting to go home."""
    return {"pending": hints.count(), "hints": hints.snapshot()}


@app.post("/hints/flush")
async def flush_hints():
    """Force a handoff pass instead of waiting for the next health sweep."""
    before = hints.count()
    await handoff_pass()
    return {"delivered": before - hints.count(), "remaining": hints.count()}


@app.put("/objects/{object_name}")
async def upload_object(object_name: str, file: UploadFile = File(...)):
    """Upload an object to 3 distinct nodes chosen by the hash ring."""
    targets, handoffs = placement.plan_write(ring, object_name, nodes)
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

    # Only hint for stand-ins whose write actually landed -- a hint pointing at
    # a holder with no copy would make the handoff pass chase a 404 forever.
    recorded = []
    for holder, intended_owner in handoffs:
        if holder in stored:
            hints.record(object_name, holder, intended_owner)
            recorded.append({"held_by": holder, "intended_owner": intended_owner})

    return {
        "message": "Object uploaded successfully",
        "object_name": object_name,
        "replicas": stored,
        "failed_replicas": failed,
        "replication_factor": placement.REPLICATION_FACTOR,
        "size": len(payload),
        "hinted": recorded,
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
            url = f"http://{node_endpoint(nodes[node_id])}/data/{object_name}"
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
