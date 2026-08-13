
import asyncio

import pytest

from shutils.dag.lib.aio_queue import PriorityQueue, Queue, QueueEmpty, QueueFull


class TestSyncQueue:
    def test_put_and_get(self):
        q = Queue()
        q.sync_q.put("item1")
        result = q.sync_q.get()
        assert result == "item1"

    def test_fifo_order(self):
        q = Queue()
        q.sync_q.put("first")
        q.sync_q.put("second")
        assert q.sync_q.get() == "first"
        assert q.sync_q.get() == "second"

    def test_qsize(self):
        q = Queue()
        assert q.sync_q.qsize() == 0
        q.sync_q.put("item")
        assert q.sync_q.qsize() == 1

    def test_empty(self):
        q = Queue()
        assert q.sync_q.empty() is True
        q.sync_q.put("item")
        assert q.sync_q.empty() is False

    def test_put_nowait(self):
        q = Queue()
        q.sync_q.put_nowait("item")
        assert q.sync_q.get() == "item"

    def test_get_nowait_empty_raises(self):
        q = Queue()
        with pytest.raises(QueueEmpty):
            q.sync_q.get_nowait()

    def test_maxsize_full(self):
        q = Queue(maxsize=1)
        q.sync_q.put("item1")
        with pytest.raises(QueueFull):
            q.sync_q.put_nowait("item2")

    def test_task_done_and_join(self):
        q = Queue()
        q.sync_q.put("item")
        q.sync_q.get()
        q.sync_q.task_done()
        q.sync_q.join()  # should not block


class TestAsyncQueue:
    async def test_put_and_get(self):
        q = Queue()
        await q.async_q.put("item1")
        result = await q.async_q.get()
        assert result == "item1"

    async def test_fifo_order(self):
        q = Queue()
        await q.async_q.put("first")
        await q.async_q.put("second")
        assert await q.async_q.get() == "first"
        assert await q.async_q.get() == "second"

    async def test_qsize(self):
        q = Queue()
        assert q.async_q.qsize() == 0
        await q.async_q.put("item")
        assert q.async_q.qsize() == 1

    async def test_task_done_and_join(self):
        q = Queue()
        await q.async_q.put("item")
        await q.async_q.get()
        q.async_q.task_done()
        await q.async_q.join()

    async def test_cancelled_waiter_after_wakeup_scheduled(self, monkeypatch):
        q = Queue()
        waiter = asyncio.get_running_loop().create_future()
        scheduled_callbacks = []

        def capture_callback(callback, *args):
            scheduled_callbacks.append((callback, args))

        monkeypatch.setattr(waiter.get_loop(), "call_soon_threadsafe", capture_callback)
        q._async_getters.append(waiter)
        q._wakeup_next(q._async_getters)

        waiter.cancel()
        callback, args = scheduled_callbacks.pop()
        callback(*args)

        assert waiter.cancelled()


class TestSyncPriorityQueue:
    def test_priority_ordering(self):
        q = PriorityQueue()
        q.sync_q.put((2, "low"))
        q.sync_q.put((0, "high"))
        q.sync_q.put((1, "mid"))
        assert q.sync_q.get() == (0, "high")
        assert q.sync_q.get() == (1, "mid")
        assert q.sync_q.get() == (2, "low")


class TestAsyncPriorityQueue:
    async def test_priority_ordering(self):
        q = PriorityQueue()
        await q.async_q.put((2, "low"))
        await q.async_q.put((0, "high"))
        await q.async_q.put((1, "mid"))
        assert await q.async_q.get() == (0, "high")
        assert await q.async_q.get() == (1, "mid")
        assert await q.async_q.get() == (2, "low")
