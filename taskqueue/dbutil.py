"""数据库阻塞调用的单线程执行器（DB actor）。

为什么是单线程而不是线程池:
    系统使用单个 SQLite 连接（check_same_thread=False），所有写操作本就需要
    串行化。若用线程池 + 一把 threading.Lock，某个线程可能在**持锁等待 SQLite
    文件锁**（busy_timeout 可达数十秒）期间一直占着线程槽，高并发提交会链式
    耗尽线程池，形成饥饿型死锁。

    这里改为 actor 模型：所有 DB 调用进入 FIFO 队列，由**唯一**专用线程顺序
    执行。由此获得:
    - 单连接、严格串行，无需在存储层再加锁，也不存在跨线程并发使用连接；
    - 调用方只需等待“前一个调用完成”，不会因持锁等待文件锁而占满多个线程；
    - 事件循环侧通过 ``run_db`` await，结果经 Future 回传，永不阻塞事件循环。
"""
from __future__ import annotations

import asyncio
import functools
import queue
import threading
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

_SENTINEL: Callable[[], Any] = lambda: None


class DBActor:
    def __init__(self) -> None:
        self._queue: queue.Queue[tuple[Callable[[], Any], Future[Any]]] = (
            queue.Queue()
        )
        self._stopping = False
        self._thread = threading.Thread(
            target=self._loop, name="taskqueue-db-actor", daemon=True
        )
        self._thread.start()

    def _loop(self) -> None:
        while True:
            func, future = self._queue.get()
            if func is _SENTINEL:
                break
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(func())
            except BaseException as exc:  # noqa: BLE001 - 必须回传任意异常
                future.set_exception(exc)

    def submit(self, func: Callable[[], Any]) -> Future[Any]:
        if self._stopping:
            raise RuntimeError("DB actor is shutting down")
        future: Future[Any] = Future()
        self._queue.put((func, future))
        return future

    def shutdown(self) -> None:
        self._stopping = True
        self._queue.put((_SENTINEL, None))  # type: ignore[arg-type]


# 模块级单例：整个进程共用一个 DB actor（也即一条 SQLite 访问通道）
_actor = DBActor()


def shutdown_actor() -> None:
    """停止 DB actor（主要用于进程退出与测试清理）。"""
    _actor.shutdown()


async def run_db[T](
    func: Callable[..., T], *args: Any, **kwargs: Any
) -> T:
    """在唯一 DB 线程上同步执行 ``func(*args, **kwargs)`` 并异步等待结果。"""
    if args or kwargs:
        bound = functools.partial(func, *args, **kwargs)
    else:
        bound = func
    future = _actor.submit(bound)
    return await asyncio.wrap_future(future)
