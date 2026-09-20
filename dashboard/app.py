"""Phase 14: a live view of the cluster.

Runs on the host, outside docker-compose, and talks to the coordinator over
HTTP like any other client. Nothing here reaches into the cluster's internals,
which is the point: everything on screen is something the coordinator already
tells ordinary callers.

    pip install -r dashboard/requirements.txt
    streamlit run dashboard/app.py
"""

import io
import os
import sys
import time

import pandas as pd
import requests
import streamlit as st

DEFAULT_COORDINATOR = os.environ.get("LATTICE_COORDINATOR", "http://localhost:9700")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_RESULTS = os.path.join(REPO_ROOT, "benchmark", "results.csv")

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from benchmark.run import run_benchmark

STATE_STYLE = {
    "healthy": ("#1a7f37", "HEALTHY"),
    "suspected": ("#9a6700", "SUSPECTED"),
    "failed": ("#b3261e", "FAILED"),
    "unknown": ("#57606a", "UNKNOWN"),
}

st.set_page_config(page_title="LATTICE", page_icon="::", layout="wide")


# ----------------------------------------------------------------- transport


class Coordinator:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")

    def _get(self, path, **params):
        response = requests.get(f"{self.base_url}{path}", params=params, timeout=30)
        response.raise_for_status()
        return response

    def health(self):
        return self._get("/health").json()

    def nodes(self):
        return self._get("/nodes").json()

    def objects(self):
        return self._get("/objects").json()

    def placement(self, name):
        return self._get(f"/objects/{name}/placement").json()

    def hints(self):
        return self._get("/hints").json()

    def download(self, name, simulate_missing=0):
        params = {"simulate_missing": simulate_missing} if simulate_missing else {}
        return self._get(f"/objects/{name}", **params)

    def upload(self, name, payload, mode):
        response = requests.put(
            f"{self.base_url}/objects/{name}",
            files={"file": (name, payload)},
            headers={"X-Storage-Mode": mode},
            timeout=300,
        )
        response.raise_for_status()
        return response.json()

    def repair(self):
        response = requests.post(f"{self.base_url}/repair", timeout=300)
        response.raise_for_status()
        return response.json()

    def verify(self, node_id):
        response = requests.post(f"{self.base_url}/nodes/{node_id}/verify", timeout=60)
        response.raise_for_status()
        return response.json()


def badge(text, colour):
    return (
        f"<span style='background:{colour};color:white;padding:2px 8px;"
        f"border-radius:10px;font-size:0.75rem;font-weight:600'>{text}</span>"
    )


def state_of(node_id, health):
    for state, members in health.get("by_state", {}).items():
        if node_id in members:
            return state
    return "unknown"


# --------------------------------------------------------------------- panes


def cluster_pane(api, health, nodes):
    left, middle, right, far_right = st.columns(4)
    left.metric("Nodes healthy", f"{health['healthy']} / {health['nodes_total']}")
    middle.metric("Accepting writes", "yes" if health["writable"] else "no")
    right.metric(
        "Full replication possible", "yes" if health["fully_replicated"] else "no"
    )
    far_right.metric("Pending hints", health["pending_hints"])

    st.markdown("#### Storage nodes")
    st.caption(
        "The coordinator probes every node every 5 seconds. A missed probe makes "
        "a node SUSPECTED; it only becomes FAILED once other nodes agree they "
        "can't reach it either."
    )

    grid = st.columns(4)
    for index, node_id in enumerate(sorted(nodes, key=lambda n: int(n.replace("node", "")))):
        state = state_of(node_id, health)
        colour, label = STATE_STYLE[state]
        misses = health.get("misses", {}).get(node_id, 0)
        with grid[index % 4]:
            st.markdown(
                f"**{node_id}** &nbsp; {badge(label, colour)}", unsafe_allow_html=True
            )
            st.caption(
                f"{nodes[node_id]['address']}"
                + (f" &middot; {misses} missed probe(s)" if misses else "")
            )
            if st.button("Ask its peers", key=f"verify-{node_id}"):
                st.session_state[f"verdict-{node_id}"] = api.verify(node_id)
            verdict = st.session_state.get(f"verdict-{node_id}")
            if verdict:
                st.caption(verdict["basis"])

    verdicts = health.get("verdicts", {})
    if verdicts:
        st.markdown("#### Failure verdicts")
        st.caption("What the suspect's peers said when they were asked to ping it.")
        rows = []
        for node_id, verdict in verdicts.items():
            names = {True: "reachable", False: "unreachable", None: "no answer"}
            rows.append(
                {
                    "node": node_id,
                    "asked": ", ".join(verdict["asked"]) or "nobody",
                    "votes": ", ".join(
                        f"{peer}: {names[vote]}" for peer, vote in verdict["votes"].items()
                    ),
                    "confirmed failed": verdict["confirmed"],
                    "basis": verdict["basis"],
                }
            )
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    hints = api.hints()
    if hints["pending"]:
        st.markdown("#### Hinted handoff")
        st.caption(
            "Replicas parked on a stand-in node because the ring's choice was "
            "down. They go home when their owner recovers."
        )
        st.dataframe(pd.DataFrame(hints["hints"]), width="stretch", hide_index=True)


def objects_pane(api, health):
    catalogue = api.objects()

    if catalogue["at_risk"]:
        st.warning(
            f"{len(catalogue['at_risk'])} object(s) below full protection: "
            + ", ".join(catalogue["at_risk"])
        )

    head, tail = st.columns([3, 1])
    head.markdown("#### Objects")
    if tail.button("Run repair now", width="stretch"):
        result = api.repair()
        rebuilt = len(result["replication_repairs"]) + len(result["ec_repairs"])
        if rebuilt:
            st.success(f"repaired {rebuilt} object(s)")
            st.json(result)
        else:
            st.info("nothing needed repairing")

    if catalogue["objects"]:
        table = pd.DataFrame(catalogue["objects"])
        table["protection"] = table.apply(
            lambda row: f"{row['live_copies']}/{row['target_copies']}", axis=1
        )
        table["held_by"] = table["held_by"].apply(", ".join)
        st.dataframe(
            table[
                ["object_name", "mode", "protection", "held_by", "readable", "fully_protected"]
            ],
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("No objects yet. Upload one below, or run `python benchmark/run.py`.")

    st.markdown("#### Where an object lives")
    names = [o["object_name"] for o in catalogue["objects"]]
    if names:
        chosen = st.selectbox("Object", names, key="placement-pick")
        place = api.placement(chosen)

        if place["mode"] == "erasure_coding":
            st.caption(
                f"{place['scheme']} Reed-Solomon. Any 4 of the 6 shards rebuild "
                f"the object, so it survives 2 lost nodes on 1.5x the storage."
            )
            rows = [
                {"shard": label, "node": node, "state": state_of(node, health)}
                for label, node in place["shards"].items()
            ]
            for label in place["missing_shards"]:
                rows.append({"shard": label, "node": "-- not stored --", "state": "missing"})
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

            left, right = st.columns(2)
            left.metric("Live shards", f"{place['live_shards']} / 6")
            right.metric("Readable", "yes" if place["readable"] else "no")
        else:
            st.caption(
                "3-way replication. The preference list is where the ring wanted "
                "the copies; repair and handoff can move them elsewhere."
            )
            rows = [
                {
                    "node": node,
                    "state": state_of(node, health),
                    "ring preferred": node in place["preference_list"],
                }
                for node in place["stored_on"]
            ]
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
            left, right = st.columns(2)
            left.metric("Live copies", f"{place['live_replicas']} / 3")
            right.metric("Readable", "yes" if place["readable"] else "no")

        st.markdown("##### Read it back")
        simulate = 0
        if place["mode"] == "erasure_coding":
            simulate = st.slider(
                "Ignore this many shards on purpose (forces a degraded read)",
                0,
                2,
                0,
                key="simulate-missing",
            )
        if st.button("Download", key="download-object"):
            response = api.download(chosen, simulate_missing=simulate)
            st.success(f"{len(response.content)} bytes read back")
            details = {
                "served as": response.headers.get("X-Storage-Mode", "?"),
                "served by": response.headers.get("X-Served-By", "(reconstructed)"),
                "degraded read": response.headers.get("X-Degraded-Read", "n/a"),
                "shards available": response.headers.get("X-Shards-Available", "n/a"),
            }
            st.json(details)
            st.download_button(
                "Save file", response.content, file_name=chosen, key="save-object"
            )

    st.markdown("#### Upload")
    upload = st.file_uploader("File", key="upload-file")
    mode_label = st.radio(
        "Storage mode",
        ["replication", "ec"],
        horizontal=True,
        key="upload-mode",
        help="Sent as the X-Storage-Mode header. 3 whole copies, or 4+2 shards.",
    )
    if upload is not None and st.button("Store it", key="do-upload"):
        payload = upload.getvalue()
        try:
            result = api.upload(upload.name, payload, mode_label)
            st.success(f"{upload.name} stored ({len(payload)} bytes)")
            st.json(result)
        except requests.HTTPError as e:
            st.error(f"{e.response.status_code}: {e.response.text}")


def benchmark_pane(results_path, base_url=DEFAULT_COORDINATOR):
    st.markdown("#### Live Cluster Benchmark")
    st.caption(
        "Execute a fresh benchmark against the running LATTICE cluster or inspect saved/uploaded results."
    )

    with st.expander("Benchmark Configuration & Controls", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            sizes_input = st.text_input(
                "Payload Sizes",
                value="16KB,64KB,256KB",
                help="Comma-separated sizes (e.g. 16KB,64KB,256KB,1MB)",
                key="bench-sizes-input",
            )
        with col2:
            modes_selected = st.multiselect(
                "Storage Modes",
                options=["replication", "ec"],
                default=["replication", "ec"],
                help="Storage modes to test",
                key="bench-modes-select",
            )
        with col3:
            reps_count = st.number_input(
                "Reps / Cell",
                min_value=1,
                max_value=20,
                value=3,
                help="Measured repetitions per cell",
                key="bench-reps-input",
            )

        run_btn = st.button("Run Benchmark", type="primary", use_container_width=True, key="do-benchmark")

    if run_btn:
        if not modes_selected:
            st.error("Please select at least one storage mode to benchmark.")
        else:
            with st.status(f"Running benchmark against `{base_url}`...", expanded=True) as status_box:
                progress_bar = st.progress(0.0)

                def on_progress(msg, fraction):
                    status_box.write(msg)
                    progress_bar.progress(min(1.0, max(0.0, fraction)))

                try:
                    sizes_list = [s.strip() for s in sizes_input.split(",") if s.strip()]
                    result = run_benchmark(
                        coordinator_url=base_url,
                        sizes=sizes_list,
                        reps=int(reps_count),
                        warmup=1,
                        modes=modes_selected,
                        out=results_path,
                        verbose=False,
                        progress_callback=on_progress,
                    )
                    progress_bar.progress(1.0)
                    status_box.update(
                        label="Benchmark completed successfully!",
                        state="complete",
                        expanded=False,
                    )
                    st.session_state["benchmark_fresh_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state["benchmark_fresh_target"] = base_url
                    st.session_state["benchmark_fresh_rows"] = len(result["rows"])
                    st.success(
                        f"Fresh benchmark completed against {base_url}! "
                        f"{len(result['rows'])} measurements recorded to `{results_path}`."
                    )
                except requests.RequestException as e:
                    status_box.update(label="Benchmark failed!", state="error", expanded=True)
                    st.error(f"Benchmark failed: coordinator at {base_url} is unreachable ({e}).")
                except Exception as e:
                    status_box.update(label="Benchmark failed!", state="error", expanded=True)
                    st.error(f"Benchmark failed: {e}")

    uploaded = st.file_uploader("Or load a CSV", type="csv", key="bench-csv")
    if uploaded is not None:
        frame = pd.read_csv(io.BytesIO(uploaded.getvalue()))
        st.caption("Displaying measurements from uploaded CSV.")
    elif os.path.exists(results_path):
        frame = pd.read_csv(results_path)
        fresh_time = st.session_state.get("benchmark_fresh_time")
        fresh_target = st.session_state.get("benchmark_fresh_target", base_url)
        if fresh_time:
            st.info(f"Displaying fresh benchmark data recorded at **{fresh_time}** against `{fresh_target}`.")
        else:
            st.caption(f"Reading `{results_path}` -- produced by `python benchmark/run.py` or previous run.")
    else:
        st.info("No results yet. Click **'Run Benchmark'** above or run `python benchmark/run.py`.")
        return

    if frame.empty:
        st.warning("The benchmark result set is empty.")
        return

    c1, c2, c3, c4 = st.columns(4)
    total_ops = len(frame)
    all_verified = frame["verified"].all() if "verified" in frame else False
    c1.metric("Total Measurements", total_ops)
    c2.metric("Data Integrity", "100% Verified" if all_verified else "Verification Failed")
    rep_writes = frame[(frame["mode"] == "replication") & (frame["operation"] == "write")]
    ec_writes = frame[(frame["mode"] == "ec") & (frame["operation"] == "write")]
    if not rep_writes.empty:
        c3.metric("Replication Median Write", f"{rep_writes['seconds'].median() * 1000:.1f} ms")
    if not ec_writes.empty:
        c4.metric("EC Median Write", f"{ec_writes['seconds'].median() * 1000:.1f} ms")

    st.markdown("---")
    st.markdown("#### Performance Comparison")

    medians = (
        frame.groupby(["mode", "size_bytes", "operation"])["seconds"]
        .median()
        .reset_index()
    )
    medians["ms"] = (medians["seconds"] * 1000).round(2)
    medians["size"] = medians["size_bytes"].apply(
        lambda b: f"{b // 1024}KB" if b < 1024 * 1024 else f"{b // (1024 * 1024)}MB"
    )

    for operation, caption in (
        ("write", "Object in, every copy or shard durable."),
        ("read", "Healthy read. Replication serves it whole from one node."),
        ("degraded_read", "EC only: two shards ignored, so the data is solved for."),
    ):
        subset = medians[medians["operation"] == operation]
        if subset.empty:
            continue
        st.markdown(f"**{operation.replace('_', ' ')}** -- {caption}")
        st.bar_chart(
            subset.pivot(index="size", columns="mode", values="ms"),
            y_label="median ms",
        )

    st.markdown("**Storage overhead** -- bytes held per byte of object.")
    overhead = (
        frame.groupby(["mode", "size_bytes"])["overhead_ratio"].max().reset_index()
    )
    overhead["size"] = overhead["size_bytes"].apply(
        lambda b: f"{b // 1024}KB" if b < 1024 * 1024 else f"{b // (1024 * 1024)}MB"
    )
    st.bar_chart(
        overhead.pivot(index="size", columns="mode", values="overhead_ratio"),
        y_label="x original size",
    )

    if not frame["verified"].all():
        st.error("Some measured reads did not match what was written.")
    else:
        st.success("Every measured read matched the bytes that were written.")

    with st.expander("Raw measurements"):
        st.dataframe(frame, width="stretch", hide_index=True)


# ---------------------------------------------------------------------- main


def main():
    st.title("LATTICE")
    st.caption("Distributed object storage: consistent hashing, replication, erasure coding")

    with st.sidebar:
        st.header("Connection")
        base_url = st.text_input("Coordinator", DEFAULT_COORDINATOR)
        results_path = st.text_input("Benchmark CSV", DEFAULT_RESULTS)
        st.divider()
        auto = st.checkbox("Auto refresh", value=False)
        interval = st.slider("Every (seconds)", 2, 30, 5, disabled=not auto)
        if st.button("Refresh now", width="stretch"):
            st.rerun()

    api = Coordinator(base_url)

    try:
        health = api.health()
        nodes = api.nodes()
    except requests.RequestException as e:
        st.error(f"Can't reach the coordinator at {base_url}")
        st.code(str(e))
        st.caption("Start the cluster with:  docker compose up -d --build")
        benchmark_pane(results_path, base_url=base_url)
        return

    cluster_tab, objects_tab, bench_tab = st.tabs(["Cluster", "Objects", "Benchmark"])
    with cluster_tab:
        cluster_pane(api, health, nodes)
    with objects_tab:
        objects_pane(api, health)
    with bench_tab:
        benchmark_pane(results_path, base_url=base_url)

    if auto:
        time.sleep(interval)
        st.rerun()


main()
