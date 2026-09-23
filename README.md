# LATTICE

A distributed object store built from scratch: a coordinator, eight storage
nodes, and the machinery that keeps objects readable while nodes fail.

Objects can be stored two ways, chosen per write:

| | replication | erasure coding |
|---|---|---|
| layout | 3 whole copies on 3 nodes | 4 data + 2 parity shards on 6 nodes |
| survives | 2 lost nodes | 2 lost nodes |
| storage cost | 3.0x | 1.5x |
| healthy read | one node serves it whole | four shards, concatenated |
| repairing one loss | copy a whole object | read 4 shards, solve for the missing one |

Both tolerate the same two failures. Erasure coding halves the storage bill and
pays for it in CPU on every read. Putting numbers on that trade is what
`benchmark/run.py` is for.

## Running it

```bash
docker compose up -d --build         # coordinator on :9700, node1..node8
curl localhost:9700/health
```

```bash
# store an object three ways over, then read it back
curl -X PUT localhost:9700/objects/report.pdf -F file=@report.pdf
curl localhost:9700/objects/report.pdf -o out.pdf

# store the same object as 4+2 shards instead
curl -X PUT localhost:9700/objects/report.pdf \
     -H "X-Storage-Mode: ec" -F file=@report.pdf

# where did it actually go?
curl localhost:9700/objects/report.pdf/placement
```

## Running method

```bash
python chaos/demo.py
```

Stops real containers and narrates what happens: a replica is lost and the
repair pass puts the third copy back; shards are lost and reads keep working
while the missing shards are rebuilt; a node stops answering and the
coordinator asks its peers before believing it is dead.

The demo is deterministic. Payloads come from a fixed seed, object names are
constant, and the nodes it stops are read back from the object's own placement,
so the same command tells the same story every time. `--restore` brings
everything back up.

## Measuring the trade-off

```bash
python benchmark/run.py                      # writes benchmark/results.csv
python benchmark/run.py --sizes 64KB,1MB --reps 7
```

Times writes, healthy reads and degraded reads for both modes, checks every
read against the bytes that were written, and reports storage overhead taken
from each object's real placement. Degraded reads are forced with
`?simulate_missing=2` rather than by killing nodes, because killing nodes also
wakes the repair pass, which heals the object before the next read lands.

## Watching it live & running benchmarks from the UI

```bash
docker compose up -d --build         # start the 8-node LATTICE cluster
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

Runs on the host, outside compose, and talks to the coordinator over HTTP like
any other client:
- **Cluster tab**: Node states as the failure detector sees them, peer verdicts, and hinted handoff.
- **Objects tab**: Object catalogue, placement inspector, interactive download with simulated missing shards, and upload.
- **Benchmark tab**: Run a fresh benchmark against the live cluster directly with the **"Run Benchmark"** button (configuring sizes, modes, and reps), or inspect existing CSV results and performance comparison charts.


## Tests

```bash
pytest -q
```

`tests/test_lattice.py` stands eight real storage-node apps up in one process
and routes the coordinator's HTTP calls to them through an in-memory transport.
Stopping a node means dropping it from that routing table, which the
coordinator sees exactly as it sees a stopped container. No Docker needed.

## API

| | |
|---|---|
| `GET /health` | what the failure detector currently believes |
| `GET /nodes` | every node and its state |
| `POST /nodes/{id}/verify` | ask other nodes whether they can reach this one |
| `PUT /objects/{name}` | store; `X-Storage-Mode: replication` (default) or `ec` |
| `GET /objects/{name}` | read; `?simulate_missing=N` forces a degraded EC read |
| `GET /objects` | every known object and how well protected it is |
| `GET /objects/{name}/placement` | where the ring wanted it, where it actually is |
| `POST /repair` | force a repair sweep instead of waiting for the next one |
| `GET /hints` | replicas parked on a stand-in node |
| `POST /hints/flush` | force a handoff pass |

Reads never need the mode: the coordinator remembers how each object went in.

## How it fits together

```
                    ┌──────────────────────────────────┐
   client ─────────▶│           coordinator            │
                    │                                  │
                    │  ring          consistent hash,  │
                    │                100 vnodes/node   │
                    │  placement     who gets a copy   │
                    │  health        probe + peer vote │
                    │  hints         diverted replicas │
                    │  repair        restore 3x / 4+2  │
                    │  erasure       GF(256) 4+2 RS    │
                    └───────┬──────────────────────────┘
                            │  PUT/GET/DELETE /data/{name}
        ┌───────────┬───────┴───┬───────────┬───────────┐
      node1       node2       node3  ...  node8
     (files on disk, plus /ping and /verify)
```

Storage nodes are deliberately dumb: they put bytes on disk, say whether
they're alive, and ping a peer when asked. Every decision lives in the
coordinator.

```
common/erasure.py        GF(256) tables, 6x4 encode matrix, decode, reconstruct
common/models.py         NodeInfo, NodeState
coordinator/hashing.py   consistent hash ring with virtual nodes
coordinator/placement.py where writes go, what order reads try
coordinator/health.py    probe loop, peer-corroborated failure detection
coordinator/hints.py     hinted handoff bookkeeping
coordinator/repair.py    replica repair
coordinator/main.py      the API, erasure coding paths, EC repair
node/main.py             a storage node
chaos/demo.py            deterministic failure demo
benchmark/run.py         replication vs EC, to CSV
dashboard/app.py         Streamlit dashboard
tests/test_lattice.py    end-to-end tests without Docker
```

## Implementation Method

**Nothing here is durable across a coordinator restart.** The placement index
lives in memory. The data survives on the nodes, and a replicated read falls
back to probing the ring, but an EC object whose shard map is lost cannot be
reassembled. A real system writes that index down; see the Design section below.

**Writes are accepted before they are fully protected.** Replication commits at
2 of 3 copies, erasure coding at 4 of 6 shards -- in both cases the point at
which the object is readable. The repair pass restores the rest, and the write
response says `degraded_write` when it happens.

**A missed ping is not a failure.** The coordinator asks three other nodes to
ping the suspect before it believes anything. One peer reaching it is enough to
keep it merely SUSPECTED, which is what stops a coordinator with a bad link
from evicting healthy nodes.

**Reed-Solomon is implemented here, not imported.** `common/erasure.py` builds
its own GF(256) log/exp tables and a 6x4 Vandermonde-style encode matrix; the
decoder inverts the 4x4 submatrix of whichever shards turned up. The tests
check every one of the 15 four-shard subsets. It is pure Python and slow,
around 1 MB/s, which is exactly what the benchmark shows.

## A five-minute demo script

```bash
docker compose up -d --build
curl localhost:9700/health                       # 8/8 healthy

# the two modes, same object, different shape
curl -X PUT localhost:9700/objects/a.bin -F file=@test.txt
curl -X PUT localhost:9700/objects/b.bin -H "X-Storage-Mode: ec" -F file=@test.txt
curl localhost:9700/objects/a.bin/placement      # 3 nodes
curl localhost:9700/objects/b.bin/placement      # 6 shards, D1..D4 P1 P2

python chaos/demo.py                             # kill nodes, watch it recover
python benchmark/run.py                          # the numbers
streamlit run dashboard/app.py                   # the live picture
pytest -q                                        # and it's all tested
```

The single most convincing moment is in the Erasure Coding scenario: two of the six nodes holding an object are stopped, and the very next read returns bytes that hash identically to what was written, reconstructed from four shards—followed a few seconds later by the repair pass rebuilding the two missing shards onto fresh nodes, with no client involvement at all.
