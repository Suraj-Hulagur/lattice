import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import Response
import httpx

from common.models import NodeInfo, NodeState
from coordinator.hashing import ConsistentHashRing
from coordinator import health, hints, placement
from coordinator.addressing import node_endpoint
from coordinator.repair import repair_replicas
from common.erasure import encode as ec_encode, decode as ec_decode, reconstruct_shard
from common.erasure import DATA_SHARDS, PARITY_SHARDS, TOTAL_SHARDS


# EC objects: object_name -> {"shards": {shard_index: node_id}, "original_size": int}
#
# Shard indexes are ints throughout, and an index is simply absent when that
# shard isn't stored anywhere -- either because its node failed or because the
# cluster was too degraded to place it at write time. Both cases are what the
# EC repair pass looks for, so "missing" needs only one representation.
ec_placement_index = {}


async def after_sweep_pass():
    """Run after each health sweep: handoff + repair."""
    await handoff_pass()
    await repair_replicas(nodes, placement_index, ring, placement.REPLICATION_FACTOR)
    await repair_ec_shards()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run the failure detector for as long as the coordinator is up."""
    detector = asyncio.create_task(
        health.health_check_loop(nodes, after_sweep=after_sweep_pass)
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


# ============================== STORAGE MODES ==============================
#
# From phase 9 an object can be stored two ways, chosen per write with the
# X-Storage-Mode header:
#
#   replication  3 whole copies on 3 nodes. 3x storage, a read touches 1 node.
#   ec           4 data + 2 parity shards on 6 nodes. 1.5x storage, survives
#                the same 2 failures, but a read has to gather 4 shards.
#
# Reads don't take the header. The coordinator remembers how each object went
# in and dispatches on that, so a client can fetch an object without knowing
# how it was written.

REPLICATION = "replication"
EC = "erasure_coding"

# What callers may send in X-Storage-Mode, mapped to the names above.
_MODE_ALIASES = {
    "replication": REPLICATION,
    "rep": REPLICATION,
    "ec": EC,
    "erasure": EC,
    "erasure_coding": EC,
}

# An EC write is accepted once this many of the 6 shards are durable -- exactly
# the number a read needs. The repair pass rebuilds the rest, which is the same
# bargain WRITE_QUORUM makes for replication.
EC_WRITE_QUORUM = DATA_SHARDS


def _parse_mode(header_value: str | None) -> str:
    """Resolve the X-Storage-Mode header. Absent means replication."""
    if header_value is None or not header_value.strip():
        return REPLICATION
    mode = _MODE_ALIASES.get(header_value.strip().lower())
    if mode is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown X-Storage-Mode '{header_value}'. "
                "Expected 'replication' or 'ec'."
            ),
        )
    return mode


def stored_mode(object_name: str) -> str | None:
    """How an object was written, or None if this coordinator has no record."""
    if object_name in ec_placement_index:
        return EC
    if object_name in placement_index:
        return REPLICATION
    return None


def _shard_name(object_name: str, shard_index: int) -> str:
    """On-disk name of one shard. Nodes store shards as ordinary objects."""
    return f"{object_name}__shard_{shard_index}"


def _shard_label(shard_index: int) -> str:
    """D1..D4 for data shards, P1..P2 for parity -- for humans reading logs."""
    if shard_index < DATA_SHARDS:
        return f"D{shard_index + 1}"
    return f"P{shard_index - DATA_SHARDS + 1}"


async def _put_shard(
    client: httpx.AsyncClient, node_id: str, object_name: str, shard_index: int, payload: bytes
) -> bool:
    """Store one shard. Returns True on success, False on any failure."""
    shard = _shard_name(object_name, shard_index)
    url = f"http://{node_endpoint(nodes[node_id])}/data/{shard}"
    try:
        response = await client.put(url, files={"file": (shard, payload)})
        return response.status_code == 200
    except Exception:
        return False


async def _get_shard(
    client: httpx.AsyncClient, node_id: str, object_name: str, shard_index: int
):
    """Fetch one shard, or None if this node can't produce it."""
    shard = _shard_name(object_name, shard_index)
    url = f"http://{node_endpoint(nodes[node_id])}/data/{shard}"
    try:
        response = await client.get(url)
    except Exception:
        return None
    return response.content if response.status_code == 200 else None


async def _delete_quietly(client: httpx.AsyncClient, node_id: str, stored_name: str):
    """Best-effort delete. A failure here only costs disk, never correctness."""
    url = f"http://{node_endpoint(nodes[node_id])}/data/{stored_name}"
    try:
        await client.delete(url)
    except Exception:
        pass


async def _forget_other_mode(object_name: str, keeping: str):
    """Clear out an earlier copy of this object written in the other mode.

    Re-uploading `report.pdf` as EC when it was already replicated would
    otherwise leave three orphaned whole copies on disk that nothing tracks and
    nothing will ever delete. Called only after the new write has succeeded, so
    a rejected write never destroys what was already there.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        if keeping == EC and object_name in placement_index:
            for node_id in placement_index.pop(object_name):
                await _delete_quietly(client, node_id, object_name)
            # Hints point at replicas that no longer exist.
            hints.drop_object(object_name)
            print(f"[MODE] {object_name}: replicated copies dropped, now EC")
        elif keeping == REPLICATION and object_name in ec_placement_index:
            shards = ec_placement_index.pop(object_name)["shards"]
            for shard_index, node_id in shards.items():
                await _delete_quietly(client, node_id, _shard_name(object_name, shard_index))
            print(f"[MODE] {object_name}: shards dropped, now replicated")


# ============================== WRITE PATHS ==============================


async def _store_replicated(object_name: str, payload: bytes):
    """Write 3 whole copies to 3 distinct nodes chosen by the hash ring."""
    targets, handoffs = placement.plan_write(ring, object_name, nodes)
    if len(targets) < placement.WRITE_QUORUM:
        raise HTTPException(
            status_code=503,
            detail=f"Only {len(targets)} healthy nodes available, need {placement.WRITE_QUORUM}",
        )

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
        "mode": REPLICATION,
        "replicas": stored,
        "failed_replicas": failed,
        "replication_factor": placement.REPLICATION_FACTOR,
        "size": len(payload),
        "hinted": recorded,
    }


async def _store_ec(object_name: str, payload: bytes):
    """Write the object as 4 data + 2 parity shards across 6 distinct nodes."""
    shards = ec_encode(payload)
    targets = placement.ec_plan_write(ring, object_name, nodes, TOTAL_SHARDS)

    if len(targets) < EC_WRITE_QUORUM:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Only {len(targets)} healthy nodes available, "
                f"need {EC_WRITE_QUORUM} to store a readable EC object"
            ),
        )

    # Shard i goes to targets[i]. With fewer than 6 healthy nodes the tail of
    # the shard list simply isn't placed; repair_ec_shards() rebuilds those the
    # moment there's somewhere to put them.
    async with httpx.AsyncClient(timeout=10.0) as client:
        results = await asyncio.gather(
            *(
                _put_shard(client, node_id, object_name, shard_index, shards[shard_index])
                for shard_index, node_id in enumerate(targets)
            )
        )

    shard_map = {
        shard_index: node_id
        for shard_index, (node_id, ok) in enumerate(zip(targets, results))
        if ok
    }

    if len(shard_map) < EC_WRITE_QUORUM:
        raise HTTPException(
            status_code=500,
            detail=(
                f"EC write failed: only {len(shard_map)} of {TOTAL_SHARDS} shards stored, "
                f"need {EC_WRITE_QUORUM}"
            ),
        )

    ec_placement_index[object_name] = {
        "shards": shard_map,
        "original_size": len(payload),
    }

    unwritten = [i for i in range(TOTAL_SHARDS) if i not in shard_map]
    if unwritten:
        print(
            f"[EC] {object_name}: stored {len(shard_map)}/{TOTAL_SHARDS} shards, "
            f"{[_shard_label(i) for i in unwritten]} awaiting repair"
        )

    return {
        "message": "Object uploaded successfully",
        "object_name": object_name,
        "mode": EC,
        "scheme": f"{DATA_SHARDS}+{PARITY_SHARDS}",
        "size": len(payload),
        "shards": [
            {"shard": _shard_label(i), "index": i, "node": node_id}
            for i, node_id in sorted(shard_map.items())
        ],
        "missing_shards": [
            {"shard": _shard_label(i), "index": i} for i in unwritten
        ],
        # True when the object is readable but not yet at full 6-shard
        # protection. The repair pass clears this on its own.
        "degraded_write": bool(unwritten),
    }


@app.put("/objects/{object_name}")
async def upload_object(
    object_name: str,
    file: UploadFile = File(...),
    x_storage_mode: str | None = Header(
        default=None,
        description="replication (default) or ec",
    ),
):
    """Upload an object, replicated or erasure coded per X-Storage-Mode."""
    mode = _parse_mode(x_storage_mode)

    # Buffered in memory so the same bytes can be sent to every replica or
    # sliced into shards; an UploadFile stream can only be consumed once.
    payload = await file.read()

    if mode == EC:
        result = await _store_ec(object_name, payload)
    else:
        result = await _store_replicated(object_name, payload)

    # Only once the new write is durable -- this deletes the old one.
    await _forget_other_mode(object_name, keeping=mode)
    return result


# ============================== READ PATHS ==============================


async def _fetch_replicated(object_name: str):
    """Read a replicated object, trying each holder in turn until one answers."""
    known = placement_index.get(object_name, [])
    ring_order = placement.read_order(ring, object_name, nodes)

    # Known holders first: repair and handoff both move replicas off the nodes
    # the ring would have picked, and the index is the only record of that.
    # Ring order still follows as a fallback, because the index is in memory
    # and a restarted coordinator has to be able to find data that outlived it.
    candidates = []
    for group in (
        [n for n in known if nodes[n].state == NodeState.HEALTHY],
        [n for n in ring_order if nodes[n].state == NodeState.HEALTHY],
        known,
        ring_order,
    ):
        for node_id in group:
            if node_id not in candidates:
                candidates.append(node_id)

    if not candidates:
        raise HTTPException(status_code=503, detail="No storage nodes available")

    attempted = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        for node_id in candidates:
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
                        "X-Storage-Mode": REPLICATION,
                        "X-Served-By": node_id,
                    },
                )

    raise HTTPException(
        status_code=404,
        detail=f"Object not found on any replica (tried: {', '.join(attempted)})",
    )


async def _fetch_ec(object_name: str):
    """Read an EC object, reconstructing from whatever shards answer.

    A full read collects all 6 shards and uses the 4 data shards as they are.
    A degraded read -- any shard missing because its node is down or was never
    written -- solves for the original data from any 4 of the 6.
    """
    ec_info = ec_placement_index[object_name]
    shard_map = ec_info["shards"]
    original_size = ec_info["original_size"]

    # Every shard is fetched at once: a degraded read is only as slow as the
    # slowest surviving node, not the sum of six sequential hops.
    placed = list(shard_map.items())
    fetched = [None] * TOTAL_SHARDS
    async with httpx.AsyncClient(timeout=10.0) as client:
        results = await asyncio.gather(
            *(
                _get_shard(client, node_id, object_name, shard_index)
                for shard_index, node_id in placed
            )
        )
    for (shard_index, _), data in zip(placed, results):
        fetched[shard_index] = data

    available = [i for i, data in enumerate(fetched) if data is not None]
    missing = [i for i in range(TOTAL_SHARDS) if i not in available]

    if len(available) < DATA_SHARDS:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Only {len(available)} shard(s) available, "
                f"need {DATA_SHARDS} to reconstruct"
            ),
        )

    try:
        data = ec_decode(fetched, original_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reconstruction failed: {e}")

    degraded = bool(missing)
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={object_name}",
            "X-Storage-Mode": EC,
            "X-Degraded-Read": str(degraded).lower(),
            "X-Shards-Available": str(len(available)),
            "X-Shards-Missing": str(len(missing)),
            "X-Shards-Missing-List": ",".join(_shard_label(i) for i in missing),
        },
    )


@app.get("/objects/{object_name}")
async def download_object(object_name: str):
    """Download an object. The mode it was written in is looked up, not asked for."""
    if stored_mode(object_name) == EC:
        return await _fetch_ec(object_name)
    return await _fetch_replicated(object_name)


@app.get("/objects/{object_name}/placement")
def get_placement(object_name: str):
    """Where the ring would put an object, and where it actually lives."""
    if object_name in ec_placement_index:
        ec_info = ec_placement_index[object_name]
        shard_map = ec_info["shards"]

        # A shard index in the map still counts as lost if the node holding it
        # has failed. Reporting only the map would show 6 healthy shards for an
        # object that is one failure from unreadable.
        unwritten = [i for i in range(TOTAL_SHARDS) if i not in shard_map]
        on_failed = [
            i for i, node in shard_map.items() if nodes[node].state == NodeState.FAILED
        ]
        live = TOTAL_SHARDS - len(unwritten) - len(on_failed)
        return {
            "object_name": object_name,
            "mode": EC,
            "scheme": f"{DATA_SHARDS}+{PARITY_SHARDS}",
            "shards": {_shard_label(i): node for i, node in sorted(shard_map.items())},
            "missing_shards": [_shard_label(i) for i in unwritten],
            "on_failed_nodes": [_shard_label(i) for i in sorted(on_failed)],
            "live_shards": live,
            "readable": live >= DATA_SHARDS,
            "fully_protected": live == TOTAL_SHARDS,
            "original_size": ec_info["original_size"],
        }

    stored = placement_index.get(object_name, [])
    live = [n for n in stored if nodes[n].state != NodeState.FAILED]
    return {
        "object_name": object_name,
        "mode": REPLICATION,
        "preference_list": ring.get_nodes(object_name, count=placement.REPLICATION_FACTOR),
        "stored_on": stored,
        "live_replicas": len(live),
        "readable": bool(live),
        "fully_protected": len(live) >= placement.REPLICATION_FACTOR,
    }


# ======================= BACK-COMPAT EC ENDPOINTS =======================
# Phase 9 originally exposed EC on its own path. The header is the real
# interface now; these stay as thin aliases so existing scripts keep working.


@app.put("/ec/objects/{object_name}")
async def upload_ec_object(object_name: str, file: UploadFile = File(...)):
    """Alias for PUT /objects/{name} with X-Storage-Mode: ec."""
    payload = await file.read()
    result = await _store_ec(object_name, payload)
    await _forget_other_mode(object_name, keeping=EC)
    return result


@app.get("/ec/objects/{object_name}")
async def download_ec_object(object_name: str):
    """Alias for GET /objects/{name} on an EC object."""
    if object_name not in ec_placement_index:
        raise HTTPException(status_code=404, detail="EC object not found")
    return await _fetch_ec(object_name)


# ============================== EC REPAIR ==============================


async def repair_ec_shards():
    """Rebuild EC shards that aren't stored anywhere.

    A shard goes missing two ways: the node holding it failed, or it was never
    written because the cluster was too degraded at upload time. Both show up
    the same way -- no live node in the shard map -- and both are fixed the
    same way, by solving for the shard from any 4 survivors and placing it on a
    node that isn't already holding one.

    Reconstruction is why EC repair is cheaper than it looks: rebuilding one
    128KB shard reads 4 shards, not a whole extra copy of the object.
    """
    failed_nodes = {nid for nid, n in nodes.items() if n.state == NodeState.FAILED}
    healthy_nodes = {nid for nid, n in nodes.items() if n.state == NodeState.HEALTHY}
    if not healthy_nodes:
        return []

    repairs = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        for object_name, ec_info in list(ec_placement_index.items()):
            shard_map = ec_info["shards"]

            lost = [
                shard_index
                for shard_index in range(TOTAL_SHARDS)
                if shard_index not in shard_map or shard_map[shard_index] in failed_nodes
            ]
            if not lost:
                continue

            # Check for somewhere to put the rebuilt shards before reading the
            # survivors -- otherwise a permanently under-placed object would
            # re-read 4 shards off the cluster on every 5-second sweep.
            spares = placement.repair_candidates(
                ring, object_name, nodes, exclude=set(shard_map.values())
            )
            if not spares:
                continue

            survivors = [None] * TOTAL_SHARDS
            for shard_index, node_id in shard_map.items():
                if shard_index in lost:
                    continue
                survivors[shard_index] = await _get_shard(
                    client, node_id, object_name, shard_index
                )

            available = sum(1 for s in survivors if s is not None)
            if available < DATA_SHARDS:
                print(
                    f"[EC-REPAIR] {object_name}: only {available} shard(s) readable, "
                    f"need {DATA_SHARDS} -- cannot rebuild"
                )
                continue

            rebuilt = []
            for shard_index in lost:
                if not spares:
                    print(
                        f"[EC-REPAIR] {object_name} {_shard_label(shard_index)}: "
                        "no spare healthy node"
                    )
                    break

                try:
                    shard = reconstruct_shard(survivors, shard_index)
                except Exception as e:
                    print(
                        f"[EC-REPAIR] {object_name} {_shard_label(shard_index)}: "
                        f"reconstruct failed: {e}"
                    )
                    continue

                target = spares.pop(0)
                if not await _put_shard(client, target, object_name, shard_index, shard):
                    print(
                        f"[EC-REPAIR] {object_name} {_shard_label(shard_index)}: "
                        f"write to {target} failed"
                    )
                    continue

                # The old holder may still have a stale copy of this shard on
                # disk; it is failed, so there's nobody to ask. Harmless -- the
                # shard map no longer points at it.
                previous = shard_map.get(shard_index)
                shard_map[shard_index] = target
                rebuilt.append(
                    {
                        "shard": _shard_label(shard_index),
                        "index": shard_index,
                        "lost_from": previous,
                        "rebuilt_on": target,
                    }
                )
                print(
                    f"[EC-REPAIR] {object_name} {_shard_label(shard_index)}: "
                    f"{previous or 'unwritten'} -> {target}"
                )

            if rebuilt:
                repairs.append(
                    {
                        "object": object_name,
                        "rebuilt": rebuilt,
                        "shards_held": len(shard_map),
                        "scheme": f"{DATA_SHARDS}+{PARITY_SHARDS}",
                    }
                )

    if repairs:
        print(f"[EC-REPAIR] Rebuilt shards for {len(repairs)} object(s)")
    return repairs


@app.post("/repair")
async def trigger_repair():
    """Force a repair sweep for both replicated and EC objects."""
    rep_repairs = await repair_replicas(
        nodes, placement_index, ring, placement.REPLICATION_FACTOR
    )
    ec_repairs = await repair_ec_shards()
    return {
        "replication_repairs": rep_repairs,
        "ec_repairs": ec_repairs,
        "message": "Repair sweep completed",
    }
