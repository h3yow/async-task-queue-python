"""Two worker processes (here: two services sharing one SQLite file) must
never claim the same task, even when hammering claim_next concurrently."""

from __future__ import annotations

import asyncio

from taskqueue.models import TaskRecord


async def test_parallel_claim_next_never_double_claims(db_path):
    from taskqueue.store import TaskStore

    s1 = TaskStore(db_path)
    s2 = TaskStore(db_path)
    await s1.init()
    await s2.init()
    try:
        for i in range(50):
            await s1.insert(TaskRecord.new(f"t{i:03d}", "echo", "{}"))

        claimed_ids: list[str] = []

        async def claim_many(store, token, n):
            got = []
            for _ in range(n):
                rec = await store.claim_next(token)
                if rec is None:
                    break
                got.append(rec.id)
                await asyncio.sleep(0)
            return got

        results = await asyncio.gather(
            claim_many(s1, "worker-1", 30),
            claim_many(s2, "worker-2", 30),
        )
        claimed_ids = results[0] + results[1]
        assert len(claimed_ids) == 50
        assert len(set(claimed_ids)) == 50, "no task may be claimed twice"

        # Nothing left to claim
        assert await s1.claim_next("worker-1") is None
        assert await s2.claim_next("worker-2") is None

        # Both stores agree on RUNNING ownership split
        rows = await s1.list_tasks(limit=100)
        assert all(r.state.value == "RUNNING" for r in rows)
        tokens = {r.run_token for r in rows}
        assert tokens == {"worker-1", "worker-2"}
    finally:
        await s1.close()
        await s2.close()
