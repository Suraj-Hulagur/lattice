"""Unit tests for the benchmark runner and module."""

import csv
import os
import tempfile
import pytest
import requests

from benchmark.run import (
    human_size,
    parse_size,
    run_benchmark,
    throughput,
    CSV_COLUMNS,
)


def test_size_helpers():
    assert parse_size("16KB") == 16 * 1024
    assert parse_size("64KB") == 64 * 1024
    assert parse_size("1MB") == 1024 * 1024
    assert parse_size("1024") == 1024
    assert parse_size("256B") == 256

    assert human_size(1024 * 1024) == "1MB"
    assert human_size(64 * 1024) == "64KB"
    assert human_size(500) == "500B"

    assert throughput(1024 * 1024, 1.0) == 1.0
    assert throughput(1024 * 1024, 0.5) == 2.0
    assert throughput(1024, 0) == 0.0


def test_run_benchmark_unreachable_coordinator():
    with pytest.raises(requests.RequestException):
        run_benchmark(
            coordinator_url="http://localhost:59999",
            sizes=[1024],
            reps=1,
            warmup=0,
            modes=["replication"],
            out=None,
        )


def test_run_benchmark_progress_and_csv(monkeypatch, tmp_path):
    """Test run_benchmark with a mocked Coordinator session to verify CSV output and callbacks."""
    out_csv = tmp_path / "results.csv"
    progress_records = []

    def progress_cb(msg, frac):
        progress_records.append((msg, frac))

    # Mock Coordinator network calls
    from benchmark.run import Coordinator

    original_init = Coordinator.__init__

    class MockResponse:
        def __init__(self, json_data=None, content=b"test_content", headers=None, status_code=200):
            self._json = json_data or {}
            self.content = content
            self.headers = headers or {}
            self.status_code = status_code

        def json(self):
            return self._json

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"HTTP {self.status_code}")

    def mock_health(self):
        return {"nodes_total": 8, "healthy": 8, "writable": True, "fully_replicated": True}

    def mock_put(self, name, payload, mode):
        return 0.005, {"message": "ok"}

    def mock_get(self, name, simulate_missing=0):
        # Generate the matching payload for the mock read to pass verification
        from benchmark.run import payload_of
        # Determine mode and size from name
        parts = name.split("-")
        mode = parts[1]
        payload = payload_of(1024, mode)
        headers = {"X-Degraded-Read": "true"} if simulate_missing else {}
        return 0.002, MockResponse(content=payload, headers=headers)

    def mock_placement(self, name):
        if "ec" in name:
            return {
                "shards": {"D1": "node1", "D2": "node2", "D3": "node3", "D4": "node4", "P1": "node5", "P2": "node6"}
            }
        return {"stored_on": ["node1", "node2", "node3"]}

    monkeypatch.setattr(Coordinator, "health", mock_health)
    monkeypatch.setattr(Coordinator, "put", mock_put)
    monkeypatch.setattr(Coordinator, "get", mock_get)
    monkeypatch.setattr(Coordinator, "placement", mock_placement)

    result = run_benchmark(
        coordinator_url="http://mocked:9700",
        sizes=[1024],
        reps=2,
        warmup=1,
        modes=["replication", "ec"],
        out=str(out_csv),
        progress_callback=progress_cb,
    )

    assert os.path.exists(str(out_csv))
    assert len(result["rows"]) > 0
    assert len(progress_records) > 0
    assert result["summary"] is not None

    with open(str(out_csv), "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == len(result["rows"])
        for col in CSV_COLUMNS:
            assert col in reader.fieldnames
