"""HTTP-level tests: status codes, distinguishable error bodies, concurrency."""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

os.environ.setdefault("TASKQUEUE_DB", ":memory:")  # overridden per test below


@pytest.fixture
def client(tmp_path, monkeypatch):
    # Build the app with a file DB so each client instance is isolated.
    monkeypatch.setenv("TASKQUEUE_DB", str(tmp_path / "api.db"))
    monkeypatch.setenv("TASKQUEUE_CONCURRENCY", "4")
    monkeypatch.setenv("TASKQUEUE_BASE_DELAY", "0.02")

    import importlib

    import main as main_module
    importlib.reload(main_module)
    from fastapi.testclient import TestClient

    with TestClient(main_module.app) as c:
        yield c


def _wait_state(client, task_id, states, timeout=5.0):
    deadline = time.time() + timeout
    while True:
        body = client.get(f"/tasks/{task_id}").json()
        if body["state"] in states:
            return body
        if time.time() > deadline:
            raise AssertionError(f"timeout waiting for {states}, got {body}")
        time.sleep(0.01)


def test_submit_get_cancel_flow(client):
    r = client.post("/tasks", json={"task_type": "echo", "payload": {"v": 1}})
    assert r.status_code == 202
    body = r.json()
    assert body["created"] is True
    assert body["state"] in ("PENDING", "RUNNING", "SUCCEEDED")

    done = _wait_state(client, body["id"], {"SUCCEEDED"})
    assert done["attempts"] == 1


def test_error_bodies_are_distinguishable(client):
    # unknown task id
    r1 = client.get("/tasks/unknown-id")
    assert r1.status_code == 404
    assert r1.json()["detail"]["code"] == "task_not_found"

    # unknown task type
    r2 = client.post("/tasks", json={"task_type": "missing"})
    assert r2.status_code == 422
    assert r2.json()["detail"]["code"] == "unknown_task_type"

    # cancel terminal -> invalid state transition
    r3 = client.post("/tasks", json={"task_type": "echo", "payload": {}})
    tid = r3.json()["id"]
    _wait_state(client, tid, {"SUCCEEDED"})
    r4 = client.post(f"/tasks/{tid}/cancel")
    assert r4.status_code == 409
    detail = r4.json()["detail"]
    assert detail["code"] == "invalid_state_transition"
    assert detail["from_state"] == "SUCCEEDED"
    assert detail["to_state"] == "CANCELLED"


def test_concurrent_idempotent_submit_runs_once(client):
    def submit(_):
        return client.post(
            "/tasks",
            json={"task_type": "echo", "payload": {"x": _}, "idempotency_key": "http-race"},
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(submit, range(16)))

    assert all(r.status_code == 202 for r in responses)
    ids = {r.json()["id"] for r in responses}
    assert len(ids) == 1
    created = [r.json()["created"] for r in responses]
    assert created.count(True) == 1 and created.count(False) == 15

    body = _wait_state(client, ids.pop(), {"SUCCEEDED"})
    assert body["attempts"] == 1


def test_dead_letter_listed_with_reason(client):
    r = client.post(
        "/tasks",
        json={"task_type": "flaky", "payload": {"key": "api-dl", "fail_times": 9}, "max_attempts": 2},
    )
    tid = r.json()["id"]
    body = _wait_state(client, tid, {"DEAD_LETTER"}, timeout=6)
    assert body["attempts"] == 2
    assert "attempts exhausted: 2/2" in body["error"]

    dls = client.get("/tasks/dead-letter/list").json()
    assert any(d["id"] == tid and d["error"] == body["error"] for d in dls)
