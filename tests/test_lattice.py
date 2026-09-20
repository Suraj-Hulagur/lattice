"""End-to-end tests for phases 8-11, run without Docker.

Eight real storage-node apps are built in-process, each with its own data
directory, and every HTTP call the coordinator makes is routed to them through
an in-memory transport.  Stopping a node means removing it from that routing
table, which the coordinator sees exactly as it sees a stopped container: a
connection error.

Run with:  pytest -q
"""

import asyncio
import itertools
import os

import pytest

# The coordinator collapses onto localhost:8000 unless it thinks it is in
# docker-compose; here the node ids really are distinct hosts.
os.environ["DOCKER_ENV"] = "1"

import httpx

from common.erasure import DATA_SHARDS, PARITY_SHARDS, TOTAL_SHARDS, decode, encode
from common.models import NodeState
from coordinator import health, hints
from coordinator import main as coord
from node.main import create_app

NODE_IDS = [f"node{i}" for i in range(1, 9)]


# --------------------------------------------------------------- test cluster


class _Cluster:
    """Eight storage nodes in one process, each stoppable."""

    def __init__(self, root):
        self._transports = {}
        for node_id in NODE_IDS:
            data_dir = root / node_id
            data_dir.mkdir()
            app = create_app(str(data_dir))
            self._transports[node_id] = httpx.ASGITransport(app=app)
        self.live = dict(self._transports)

    def stop(self, node_id: str):
        """Pull a node off the network. Its data survives, as a container's would."""
        self.live.pop(node_id, None)

    def start(self, node_id: str):
        self.live[node_id] = self._transports[node_id]

    def fail(self, node_id: str):
        """Stop a node and tell the coordinator the failure is already confirmed."""
        self.stop(node_id)
        coord.nodes[node_id].state = NodeState.FAILED

    def recover(self, node_id: str):
        self.start(node_id)
        coord.nodes[node_id].state = NodeState.HEALTHY

    async def objects_on(self, node_id: str) -> list:
        async with httpx.AsyncClient(transport=self._transports[node_id]) as client:
            response = await client.get(f"http://{node_id}:8000/data")
        return response.json()["objects"]


class _ClusterTransport(httpx.AsyncBaseTransport):
    """Routes coordinator traffic to whichever nodes are currently up."""

    def __init__(self, cluster: _Cluster):
        self._cluster = cluster

    async def handle_async_request(self, request):
        transport = self._cluster.live.get(request.url.host)
        if transport is None:
            raise httpx.ConnectError(f"{request.url.host} is down", request=request)
        return await transport.handle_async_request(request)


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    live = _Cluster(tmp_path)

    real_client = httpx.AsyncClient

    class _RoutedClient(real_client):
        # setdefault, so a caller that names its own transport (the tests
        # talking to the coordinator) is left alone.
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", _ClusterTransport(live))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _RoutedClient)

    # The coordinator caches one client; each test runs in its own event loop
    # and its own patched httpx, so the cached one must not survive.
    coord._node_client = None

    coord.placement_index.clear()
    coord.ec_placement_index.clear()
    hints._hints.clear()
    health._misses.clear()
    health._verdicts.clear()
    for node in coord.nodes.values():
        node.state = NodeState.HEALTHY

    return live


def coordinator() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=coord.app),
        base_url="http://coordinator:9700",
    )


def run(coro_fn):
    """Run one async test body. Avoids a pytest-asyncio dependency."""
    return asyncio.run(coro_fn())


async def put_object(client, name, payload, mode=None):
    headers = {"X-Storage-Mode": mode} if mode else {}
    return await client.put(
        f"/objects/{name}", files={"file": (name, payload)}, headers=headers
    )


async def placement_of(client, name):
    return (await client.get(f"/objects/{name}/placement")).json()


# ------------------------------------------------------- phase 9: the encoding


def test_any_four_of_six_shards_reconstruct():
    """The 4+2 code is MDS: every 4-shard subset recovers the original bytes."""
    data = os.urandom(3001)  # deliberately not a multiple of 4
    shards = encode(data)
    assert len(shards) == TOTAL_SHARDS

    subsets = list(itertools.combinations(range(TOTAL_SHARDS), DATA_SHARDS))
    assert len(subsets) == 15
    for keep in subsets:
        partial = [shards[i] if i in keep else None for i in range(TOTAL_SHARDS)]
        assert decode(partial, len(data)) == data, f"failed on subset {keep}"


def test_ec_storage_overhead_is_one_and_a_half():
    data = os.urandom(4096)
    stored = sum(len(s) for s in encode(data))
    assert stored == len(data) * TOTAL_SHARDS / DATA_SHARDS
    assert stored < len(data) * 3  # cheaper than 3-way replication


# --------------------------------------------- phase 9: X-Storage-Mode header


def test_default_mode_is_replication(cluster):
    async def body():
        data = os.urandom(2048)
        async with coordinator() as client:
            written = await put_object(client, "plain.bin", data)
            assert written.status_code == 200
            assert written.json()["mode"] == "replication"
            assert len(written.json()["replicas"]) == 3

            read = await client.get("/objects/plain.bin")
            assert read.status_code == 200
            assert read.content == data
            assert read.headers["x-storage-mode"] == "replication"

    run(body)


def test_ec_mode_selected_by_header(cluster):
    async def body():
        data = os.urandom(4096)
        async with coordinator() as client:
            written = await put_object(client, "coded.bin", data, mode="ec")
            assert written.status_code == 200

            info = written.json()
            assert info["mode"] == "erasure_coding"
            assert info["scheme"] == f"{DATA_SHARDS}+{PARITY_SHARDS}"
            assert info["degraded_write"] is False

            holders = [s["node"] for s in info["shards"]]
            assert len(holders) == TOTAL_SHARDS
            assert len(set(holders)) == TOTAL_SHARDS, "shards must not share a node"

            read = await client.get("/objects/coded.bin")
            assert read.content == data
            assert read.headers["x-storage-mode"] == "erasure_coding"
            assert read.headers["x-degraded-read"] == "false"

    run(body)


def test_unknown_mode_is_rejected(cluster):
    async def body():
        async with coordinator() as client:
            written = await put_object(client, "x.bin", b"hello", mode="raid5")
            assert written.status_code == 400
            assert "raid5" in written.json()["detail"]

    run(body)


def test_rewriting_in_the_other_mode_cleans_up(cluster):
    """A replicated object rewritten as EC must not leave orphaned copies."""

    async def body():
        first = os.urandom(1024)
        second = os.urandom(1024)
        async with coordinator() as client:
            await put_object(client, "swap.bin", first)
            replica_nodes = (await placement_of(client, "swap.bin"))["stored_on"]
            assert replica_nodes

            await put_object(client, "swap.bin", second, mode="ec")

            read = await client.get("/objects/swap.bin")
            assert read.content == second
            assert read.headers["x-storage-mode"] == "erasure_coding"

        for node_id in replica_nodes:
            assert "swap.bin" not in await cluster.objects_on(node_id)

    run(body)


# ------------------------------------------------- phase 10: degraded EC reads


def test_degraded_read_survives_two_lost_shards(cluster):
    async def body():
        data = os.urandom(4096)
        async with coordinator() as client:
            await put_object(client, "coded.bin", data, mode="ec")
            holders = list((await placement_of(client, "coded.bin"))["shards"].values())

            for node_id in holders[:PARITY_SHARDS]:
                cluster.stop(node_id)

            read = await client.get("/objects/coded.bin")
            assert read.status_code == 200
            assert read.content == data, "degraded read must be byte-identical"
            assert read.headers["x-degraded-read"] == "true"
            assert read.headers["x-shards-available"] == str(DATA_SHARDS)
            assert read.headers["x-shards-missing"] == str(PARITY_SHARDS)

    run(body)


def test_read_fails_cleanly_below_four_shards(cluster):
    async def body():
        data = os.urandom(4096)
        async with coordinator() as client:
            await put_object(client, "coded.bin", data, mode="ec")
            holders = list((await placement_of(client, "coded.bin"))["shards"].values())

            for node_id in holders[:3]:
                cluster.stop(node_id)

            read = await client.get("/objects/coded.bin")
            assert read.status_code == 503
            assert "need 4" in read.json()["detail"]

    run(body)


# ------------------------------------ phases 13/14: what the tools depend on


def test_object_catalogue_reports_protection(cluster):
    """The dashboard lists objects from here, so it has to tell the truth."""

    async def body():
        async with coordinator() as client:
            await put_object(client, "plain.bin", os.urandom(1024))
            await put_object(client, "coded.bin", os.urandom(4096), mode="ec")

            catalogue = (await client.get("/objects")).json()
            assert catalogue["count"] == 2
            assert catalogue["at_risk"] == []

            by_name = {o["object_name"]: o for o in catalogue["objects"]}
            assert by_name["plain.bin"]["mode"] == "replication"
            assert by_name["plain.bin"]["live_copies"] == 3
            assert by_name["coded.bin"]["mode"] == "erasure_coding"
            assert by_name["coded.bin"]["live_copies"] == TOTAL_SHARDS

            # Losing a shard holder has to show up as an at-risk object.
            cluster.fail(by_name["coded.bin"]["held_by"][0])
            catalogue = (await client.get("/objects")).json()
            assert catalogue["at_risk"] == ["coded.bin"]
            assert not [o for o in catalogue["objects"]
                        if o["object_name"] == "coded.bin"][0]["fully_protected"]

    run(body)


def test_simulate_missing_forces_a_degraded_read(cluster):
    """The benchmark measures reconstruction with this, so it must really degrade.

    Killing nodes to force a degraded read races the repair pass, which heals
    the object within a sweep.
    """

    async def body():
        data = os.urandom(4096)
        async with coordinator() as client:
            await put_object(client, "coded.bin", data, mode="ec")

            read = await client.get("/objects/coded.bin", params={"simulate_missing": 2})
            assert read.status_code == 200
            assert read.content == data
            assert read.headers["x-degraded-read"] == "true"
            assert read.headers["x-shards-available"] == str(DATA_SHARDS)

            # Nothing actually failed, so the object is still fully protected.
            assert (await placement_of(client, "coded.bin"))["fully_protected"] is True

            too_many = await client.get(
                "/objects/coded.bin", params={"simulate_missing": 3}
            )
            assert too_many.status_code == 400

            await put_object(client, "plain.bin", data)
            wrong_mode = await client.get(
                "/objects/plain.bin", params={"simulate_missing": 1}
            )
            assert wrong_mode.status_code == 400

    run(body)


# --------------------------------------------------- phase 8: replica  repair


def test_replica_repair_restores_the_third_copy(cluster):
    async def body():
        data = os.urandom(2048)
        async with coordinator() as client:
            await put_object(client, "plain.bin", data)
            holders = (await placement_of(client, "plain.bin"))["stored_on"]
            assert len(holders) == 3

            cluster.fail(holders[0])

            repaired = (await client.post("/repair")).json()["replication_repairs"]
            assert len(repaired) == 1
            assert repaired[0]["total_replicas"] == 3

            after = (await placement_of(client, "plain.bin"))["stored_on"]
            assert len(after) == 3
            assert holders[0] not in after
            assert len(set(after)) == 3

            # The repaired copy must be reachable, including when the read
            # lands on the node repair chose rather than a ring favourite.
            read = await client.get("/objects/plain.bin")
            assert read.content == data
            assert read.headers["x-served-by"] in after

    run(body)


def test_read_finds_a_replica_repair_moved_off_the_ring(cluster):
    """Repair can place a copy outside the ring's top choices for an object.

    The read path has to consult where replicas actually are, not just where
    the ring would have put them, or the repaired copy is invisible.
    """

    async def body():
        data = os.urandom(2048)
        async with coordinator() as client:
            await put_object(client, "plain.bin", data)
            original = (await placement_of(client, "plain.bin"))["stored_on"]

            cluster.fail(original[0])
            await client.post("/repair")

            repaired = (await placement_of(client, "plain.bin"))["stored_on"]
            new_holder = [n for n in repaired if n not in original][0]

            # Leave the repaired copy as the only one standing.
            for node_id in original[1:]:
                cluster.stop(node_id)

            read = await client.get("/objects/plain.bin")
            assert read.status_code == 200
            assert read.content == data
            assert read.headers["x-served-by"] == new_holder

    run(body)


def test_replica_repair_is_a_no_op_when_nothing_failed(cluster):
    async def body():
        async with coordinator() as client:
            await put_object(client, "plain.bin", os.urandom(512))
            assert (await client.post("/repair")).json()["replication_repairs"] == []

    run(body)


# -------------------------------------------------------- phase 11: EC repair


def test_ec_repair_rebuilds_shards_lost_with_a_node(cluster):
    async def body():
        data = os.urandom(4096)
        async with coordinator() as client:
            await put_object(client, "coded.bin", data, mode="ec")
            before = (await placement_of(client, "coded.bin"))["shards"]
            lost_nodes = list(before.values())[:PARITY_SHARDS]

            for node_id in lost_nodes:
                cluster.fail(node_id)

            rebuilt = (await client.post("/repair")).json()["ec_repairs"]
            assert len(rebuilt) == 1
            assert len(rebuilt[0]["rebuilt"]) == PARITY_SHARDS

            after = await placement_of(client, "coded.bin")
            assert len(after["shards"]) == TOTAL_SHARDS
            assert not set(after["shards"].values()) & set(lost_nodes)
            assert len(set(after["shards"].values())) == TOTAL_SHARDS
            assert after["fully_protected"] is True

            # Back to full protection: the read is no longer degraded.
            read = await client.get("/objects/coded.bin")
            assert read.content == data
            assert read.headers["x-degraded-read"] == "false"

    run(body)


def test_ec_repair_completes_a_degraded_write(cluster):
    """Shards that were never written must heal too, not just shards lost later.

    With only 4 nodes up the write is accepted but under-placed. Nothing has
    failed, so a repair pass that only looked for shards on failed nodes would
    leave this object one failure from unreadable forever.
    """

    async def body():
        data = os.urandom(4096)
        for node_id in NODE_IDS[4:]:
            cluster.fail(node_id)

        async with coordinator() as client:
            written = await put_object(client, "coded.bin", data, mode="ec")
            assert written.status_code == 200
            assert written.json()["degraded_write"] is True
            assert len(written.json()["shards"]) == DATA_SHARDS
            assert len(written.json()["missing_shards"]) == PARITY_SHARDS

            # Readable already, but with no protection left.
            assert (await client.get("/objects/coded.bin")).content == data

            for node_id in NODE_IDS[4:]:
                cluster.recover(node_id)

            rebuilt = (await client.post("/repair")).json()["ec_repairs"]
            assert len(rebuilt[0]["rebuilt"]) == PARITY_SHARDS
            assert all(r["lost_from"] is None for r in rebuilt[0]["rebuilt"])

            after = await placement_of(client, "coded.bin")
            assert len(after["shards"]) == TOTAL_SHARDS
            assert after["missing_shards"] == []
            assert after["fully_protected"] is True
            assert (await client.get("/objects/coded.bin")).headers[
                "x-degraded-read"
            ] == "false"
            assert (await client.get("/objects/coded.bin")).content == data

    run(body)


def test_ec_write_rejected_below_quorum(cluster):
    async def body():
        for node_id in NODE_IDS[3:]:
            cluster.fail(node_id)
        async with coordinator() as client:
            written = await put_object(client, "coded.bin", os.urandom(1024), mode="ec")
            assert written.status_code == 503

    run(body)


def test_ec_repair_gives_up_safely_below_four_shards(cluster):
    async def body():
        data = os.urandom(4096)
        async with coordinator() as client:
            await put_object(client, "coded.bin", data, mode="ec")
            holders = list((await placement_of(client, "coded.bin"))["shards"].values())

            for node_id in holders[:3]:
                cluster.fail(node_id)

            # Three shards left: not enough to solve for the missing three.
            assert (await client.post("/repair")).json()["ec_repairs"] == []

            # The map still names the dead nodes -- nothing was rebuilt -- but
            # the object must be reported as unreadable, not as fully placed.
            after = await placement_of(client, "coded.bin")
            assert len(after["on_failed_nodes"]) == 3
            assert after["live_shards"] == TOTAL_SHARDS - 3
            assert after["readable"] is False
            assert after["fully_protected"] is False

    run(body)
