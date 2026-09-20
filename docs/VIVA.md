# LATTICE: the design, and the questions it invites

This is the document to read before a viva. It goes through what the system
does, why each decision was made rather than an obvious alternative, and where
the honest weaknesses are. Every claim points at the code that backs it.

---

## 1. What problem is this solving?

Store objects on a cluster of machines so that losing machines doesn't lose
data, and do it without paying three times over for the privilege.

That splits into four questions, which are the four things LATTICE actually
implements:

1. **Which machine holds what?** Consistent hashing with virtual nodes.
2. **How is the data made redundant?** 3-way replication, or 4+2 Reed-Solomon.
3. **How do we know a machine is gone?** Probing, corroborated by its peers.
4. **How does redundancy come back afterwards?** Hinted handoff and repair.

---

## 2. Placement: consistent hashing

`coordinator/hashing.py`

Each node is hashed onto a ring 100 times (`virtual_nodes=100`). An object is
hashed onto the same ring, and the walk clockwise from that point gives its
preference list: the first distinct physical nodes encountered.

**Why not just `hash(object) % 8`?** Because 8 becomes 9. With modulo, adding a
node remaps roughly every key in the cluster; with a ring, only the keys
between the new node and its predecessor move -- about `1/n` of them.

**Why 100 virtual nodes each?** With one point per node, 8 random points divide
the ring into 8 wildly uneven arcs, and the unlucky node gets several times its
share of the data. 100 points each averages that out. The cost is 800 entries
in a sorted list and a binary search per lookup, which is nothing.

**Why must the walk skip duplicates?**
`ConsistentHashRing.get_nodes()` collects *distinct physical* nodes. Without
that check, the three closest ring points after an object could easily be three
virtual nodes belonging to the same machine, and all three "replicas" would sit
on one box.

> **Likely question: what happens to placement when a node is removed?**
> `remove_node()` deletes its 100 points, so its arcs merge into their
> clockwise neighbours and only its keys move. Note that LATTICE never calls
> this on failure -- a failed node stays on the ring and is filtered out by
> health state instead (`placement.plan_write`). That keeps placement stable
> while a node is merely down, so a five-minute outage doesn't trigger a
> cluster-wide reshuffle.

---

## 3. Redundancy, option one: replication

`coordinator/placement.py`, `coordinator/main.py::_store_replicated`

Three whole copies on the first three healthy nodes of the preference list.

**Why walk past unhealthy nodes instead of stopping at them?** So a write can
still reach three copies while part of the cluster is down. The replica that
ends up on a stand-in is recorded as a hint (section 6) so it can go home
later.

**Why is a write accepted at 2 copies and not 3?** `WRITE_QUORUM = 2`. Two
copies already survive one further failure, and holding the client's request
open until a slow third node answers trades availability for a guarantee the
repair pass will provide anyway, within one sweep. The response reports what
actually landed.

> **Likely question: is this strongly consistent?**
> No, and deliberately so. There is no read quorum and no versioning: a read
> takes the first replica that answers. Two concurrent writes to the same name
> can leave different bytes on different nodes, and nothing detects it. Fixing
> that properly means vector clocks and read repair (Dynamo) or a consensus
> group per partition (Spanner). LATTICE is AP: it stays writable while nodes
> are down, and accepts that replicas can diverge.

---

## 4. Redundancy, option two: erasure coding

`common/erasure.py`

The object is padded to a multiple of 4 and split into 4 data shards. Two
parity shards are computed from them. Any 4 of the 6 rebuild the original.

### The maths

Arithmetic is over GF(2^8), the field of 256 elements, so every value fits in
one byte. `_init_tables()` builds log and exp tables using the primitive
polynomial `0x11d`; multiplication becomes `exp[log[a] + log[b]]`, and addition
is XOR.

The encode matrix is 6x4: the top 4 rows are the identity, so the data shards
are literally the original bytes, and the bottom 2 rows are `(i+1)^j`, giving
`[1,1,1,1]` and `[1,2,4,8]`.

Encoding is `shards = M · data`. To decode, take the 4 rows of `M`
corresponding to shards that survived, invert that 4x4 submatrix by Gauss-Jordan
(`_gf_invert`), and multiply: `data = M_sub⁻¹ · surviving`.

**Why does that always work?** Because every 4x4 submatrix of `M` is
invertible -- the MDS property. `tests/test_lattice.py::test_any_four_of_six_shards_reconstruct`
checks all 15 subsets against random data, including a length deliberately not
a multiple of 4 so the padding path is exercised.

**Why identity rows on top?** A healthy read then needs no arithmetic at all:
concatenate shards 0-3 and trim to the original length. Reconstruction cost is
only paid when something is actually missing. This is a *systematic* code.

**Why store the original size?** Padding to a multiple of 4 adds up to 3 bytes.
Without the recorded length, a decoded object would come back with trailing
zeros, so `ec_placement_index[name]["original_size"]` is what `decode()` trims
to.

**Why 4+2?** It matches replication's failure tolerance -- both survive 2 losses
-- which makes the comparison honest. The storage cost is `6/4 = 1.5x` against
`3.0x`.

**Why NumPy and not `reedsolo`?** The plan left it open. A 4+2 code needs
multiplication, a Vandermonde matrix and Gauss-Jordan elimination, all of which
are short and are the interesting part of the project. NumPy holds the matrices;
the field arithmetic is ours.

> **Likely question: why is one parity row `[1,1,1,1]`?**
> That row is a plain XOR of the data shards, exactly RAID-5 parity. The second
> row, with distinct multipliers, is what lifts it to RAID-6-like behaviour and
> lets any *two* losses be recovered rather than only one.

> **Likely question: could you extend to 8+3?**
> Yes -- `DATA_SHARDS` and `PARITY_SHARDS` drive everything, and the matrix
> builder generalises. The constraint is that the encode matrix must stay MDS,
> which a Vandermonde or Cauchy construction over GF(256) gives you for any
> `k + m <= 255`.

---

## 5. Failure detection

`coordinator/health.py`

Every 5 seconds the coordinator pings all eight nodes. A node that misses a
probe becomes **SUSPECTED**, never FAILED. To be declared **FAILED**, up to
three other healthy nodes are asked to ping it themselves
(`node/main.py::verify_peer`), and they all have to agree it is unreachable.

**Why the extra round trip?** A missed ping says as much about the coordinator's
network as about the node. If the coordinator can't reach node3 but node1,
node2 and node4 all can, the problem is the coordinator's link -- and marking
node3 FAILED would evict a perfectly healthy machine from the write set and
trigger a pointless repair of every object it holds.

**What if nobody can be asked?** If every other node is down too, there are no
peers, so the coordinator falls back to a miss count (`FAILED_THRESHOLD = 3`).
Otherwise a total outage would leave every node SUSPECTED forever.

**Why three states rather than up/down?** SUSPECTED is the state where the
coordinator is uncertain, and it behaves differently in each direction:
suspected nodes are excluded from *write* targets (don't send new data
somewhere doubtful) but kept in the *read* order and in the repair source list
(`placement.read_order`, `repair.py`) -- a copy that might be readable beats
giving up on the object.

> **Likely question: this is a single point of failure.**
> It is. The coordinator holds all the placement state and every request goes
> through it. Failure *detection* is distributed -- the vote is what makes it
> more than one machine's opinion -- but decision-making is not. Section 9.

---

## 6. Getting redundancy back

Three separate mechanisms, for three different situations.

### Hinted handoff -- the node was down when we wrote

`coordinator/hints.py`, `main.py::handoff_pass`

The ring wanted node3, node3 was down, node6 took the replica instead. A hint
records "node6 holds this on node3's behalf". When node3 comes back, the
handoff pass copies it over and deletes the stand-in's copy.

The copy happens before the delete. If the delete fails we have an extra
replica, which repair tidies up; the other order could lose the only copy on
that side of the ring.

### Replica repair -- the node is not coming back

`coordinator/repair.py`

Once a node is FAILED, the hint can never be delivered. The repair pass finds
objects that have dropped below 3 copies, reads one from a surviving holder,
and writes it to the most ring-preferred healthy node that doesn't already have
one.

**Why ring-preferred rather than any healthy node?** So repaired copies drift
back towards where the ring would have put them, instead of the cluster
scattering further out of shape with every failure. It also makes the chaos
demo deterministic.

### EC repair -- a shard is missing

`coordinator/main.py::repair_ec_shards`

A shard goes missing two ways: its node failed, or it was never written because
the cluster was too degraded at upload time. Both look the same -- no live node
in the shard map -- and both are fixed the same way: read any 4 surviving
shards, solve for the missing one, place it on a node not already holding one.

**Why does this matter more than it sounds?** Because the second case has no
failure to trigger it. An earlier version only looked for shards on FAILED
nodes, so an object written while the cluster was short of nodes stayed at 4 of
6 shards permanently -- one failure from unreadable, with nothing to notice.
`test_ec_repair_completes_a_degraded_write` is that bug.

> **Likely question: how much traffic does repair cost?**
> This is erasure coding's real weakness. Replacing a lost *replica* copies one
> object. Replacing a lost *shard* reads 4 shards -- the whole object's worth of
> data -- to produce a quarter of it. Repair traffic is roughly 4x the shard
> size where replication's is 1x the object. Production systems use local
> reconstruction codes (Azure LRC) that add local parity groups so a single
> failure is repaired from 2 or 3 nearby shards instead of 4 across the cluster.

---

## 7. What the benchmark shows

`benchmark/run.py`, and `dashboard/app.py` renders the CSV.

Storage overhead is exactly as predicted: 3.00x against 1.50x, at every size,
taken from each object's real placement rather than the nominal factor.

Latency is where it gets interesting. Replication is roughly flat with object
size at these sizes -- the cost is per-request, not per-byte. Erasure coding
grows with size, because encoding and decoding are per-byte loops in Python
over GF(256). On one developer machine, 16KB objects showed the two modes
within about 10% of each other, while 256KB objects had EC reads around 11x
slower, and degraded reads slower again.

**Be ready to say why that is not an indictment of erasure coding.** The
asymmetry is an artifact of this implementation, not of the scheme. Production
Reed-Solomon uses SIMD table lookups and runs at GB/s; `common/erasure.py` runs
at roughly 1 MB/s. What the benchmark demonstrates properly is the *shape*: EC
trades CPU for storage, and the CPU cost scales with object size while the
storage saving is constant.

**Why are degraded reads forced with `?simulate_missing=2` instead of by
killing nodes?** Because killing nodes wakes the repair pass, which rebuilds
the shards within one 5-second sweep -- the next read is a full read again. The
measurement would be racing the thing being measured. The parameter takes
shards out of the read deliberately, which is the same code path a real
degraded read takes.

---

## 8. Things worth admitting before you're asked

**The placement index is in memory.** Restart the coordinator and it forgets
where everything is. Replicated objects survive this -- the read path falls back
to probing the ring -- but an EC object's shard map is gone, and with it the
object. This is the single biggest gap. The fix is to write the index to disk
on every mutation, or to make it derivable by scanning the nodes at startup.

**Writes are not atomic.** A write that fails after 2 of 3 replicas leaves 2
replicas and returns success. A write that fails partway through 6 shards is
recorded as whatever landed. The system is designed around repairing this
rather than preventing it, which is a legitimate choice, but it isn't the same
as a transaction.

**No versioning, no conflict detection.** Two clients writing the same name
concurrently can leave different bytes on different replicas, permanently, with
no way to tell. Dynamo solves this with vector clocks and lets the application
merge; LATTICE doesn't try.

**No authentication, no encryption, no tenancy.** Anything that can reach the
coordinator can read and overwrite anything.

**Node object names are flat.** `_object_path()` rejects names that would
escape the data directory, so `../../etc/passwd` is blocked, but there's no
namespacing, no buckets and no listing beyond a directory scan.

**Deletion is missing.** Nodes have `DELETE /data/{name}`, used internally by
handoff and mode switching, but there is no object-level delete in the
coordinator API. Objects can only be overwritten.

---

## 9. What would have to change to make this real

| Gap | What it needs |
|---|---|
| Coordinator is a single point of failure | Replicate the metadata across a Raft group; nodes gossip instead of being probed from one place |
| Placement index is volatile | Persist it on write; rebuild from node scans at startup |
| No consistency guarantees | Vector clocks and read repair, or per-partition consensus |
| Repair reads 4x the data | Local reconstruction codes with per-group parity |
| Reed-Solomon is ~1 MB/s | SIMD-accelerated field arithmetic, or `ISA-L` |
| Whole objects in memory | Stream and chunk large objects; erasure code per chunk |
| Nothing is authenticated | Signed requests, encryption at rest and in transit |

---

## 10. A five-minute demo script

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

The single most convincing moment is in the EC scenario: two of the six nodes
holding an object are stopped, and the very next read returns bytes that hash
identically to what was written, reconstructed from four shards -- followed a
few seconds later by the repair pass rebuilding the two missing shards onto
fresh nodes, with no client involvement at all.
