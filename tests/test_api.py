"""HTTP API 测试：端点、状态码与可区分错误原因。"""
from __future__ import annotations

import os
import time

import pytest
from fastapi.testclient import TestClient

from taskqueue.app import create_app
from taskqueue.config import RetryConfig, TaskQueueConfig
from taskqueue.errors import RetryableTaskError


@pytest.fixture
def client(tmp_path):
    db_path = os.path.join(str(tmp_path), "api.db")
    cfg = TaskQueueConfig(
        db_path=db_path, max_concurrency=4, poll_interval=0.005,
        default_task_timeout=5.0,
        retry=RetryConfig(max_attempts=2, base_delay=0.01),
    )

    async def echo(payload, ctx):
        return payload

    async def always_fail(payload, ctx):
        raise RetryableTaskError("api boom")

    def configure(registry):
        registry.register("echo", echo)
        registry.register("always_fail", always_fail)

    import logging
    log = logging.getLogger("taskqueue.api-test")
    log.setLevel(logging.CRITICAL)
    app = create_app(config=cfg, configure=configure, logger=log)
    with TestClient(app) as c:
        yield c


def _poll(client, task_id, expect, tries=200):
    for _ in range(tries):
        g = client.get(f"/tasks/{task_id}").json()
        if g["state"] in expect:
            return g
        time.sleep(0.01)
    return g


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_submit_and_query_success(client):
    r = client.post("/tasks", json={
        "task_type": "echo", "payload": {"x": 1}, "idempotency_key": "k1"
    })
    assert r.status_code == 201
    body = r.json()
    assert body["state"] == "PENDING"
    tid = body["id"]

    done = _poll(client, tid, ["SUCCEEDED"])
    assert done["state"] == "SUCCEEDED"
    assert done["result"] == {"x": 1}
    assert done["attempts"] == 1


def test_idempotent_submit_returns_same_task_and_no_dup(client):
    payload = {"task_type": "echo", "payload": {"a": 1}, "idempotency_key": "idem"}
    first = client.post("/tasks", json=payload)
    second = client.post("/tasks", json=payload)
    assert first.status_code == second.status_code == 201
    assert first.json()["id"] == second.json()["id"]


def test_idempotent_conflict_returns_distinguishable_409(client):
    client.post("/tasks", json={
        "task_type": "echo", "payload": {"v": 1}, "idempotency_key": "conf"
    })
    r = client.post("/tasks", json={
        "task_type": "echo", "payload": {"v": 2}, "idempotency_key": "conf"
    })
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "idempotency_conflict"
    assert "existing_task_id" in detail


def test_unknown_task_404(client):
    r = client.get("/tasks/missing")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "unknown_task"


def test_cancel_terminal_task_409_invalid_state(client):
    r = client.post("/tasks", json={"task_type": "echo", "payload": {}})
    tid = r.json()["id"]
    _poll(client, tid, ["SUCCEEDED"])
    cancel = client.post(f"/tasks/{tid}/cancel")
    assert cancel.status_code == 409
    detail = cancel.json()["detail"]
    assert detail["error"] == "invalid_state"
    assert detail["current_state"] == "SUCCEEDED"


def test_unknown_task_type_400(client):
    r = client.post("/tasks", json={"task_type": "ghost"})
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "validation_error"


def test_invalid_payload_shape_422(client):
    # pydantic 层校验：max_attempts 必须 >= 1
    r = client.post("/tasks", json={"task_type": "echo", "max_attempts": 0})
    assert r.status_code == 422


def test_dead_letter_queryable_via_api(client):
    r = client.post("/tasks", json={"task_type": "always_fail", "payload": {}})
    tid = r.json()["id"]
    dead = _poll(client, tid, ["DEAD_LETTER", "FAILED"])
    assert dead["state"] == "DEAD_LETTER"
    assert dead["attempts"] == 2
    assert dead["error_kind"] == "RETRYABLE"
    assert "api boom" in dead["last_error"]


def test_get_by_idempotency_key(client):
    client.post("/tasks", json={
        "task_type": "echo", "payload": {"q": 1}, "idempotency_key": "lookme"
    })
    r = client.get("/tasks/by-idempotency-key/lookme")
    assert r.status_code == 200
    assert r.json()["idempotency_key"] == "lookme"
    assert client.get("/tasks/by-idempotency-key/nope").status_code == 404
