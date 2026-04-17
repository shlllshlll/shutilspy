from shutils.dag.context import Context
from shutils.dag.context_queue import AsyncContextQueue, ContextPriority, ContextQueue, SyncContextQueue


class TestContextPriority:
    def test_values(self):
        assert ContextPriority.LIFO.value == 0
        assert ContextPriority.FIFO_HIGH.value == 1
        assert ContextPriority.FIFO_LOW.value == 2


class TestContextQueue:
    def test_init(self):
        cq = ContextQueue()
        assert cq._sync_queue is None
        assert cq._async_queue is None

    def test_sync_queue_property(self):
        cq = ContextQueue()
        sq = cq.sync_queue
        assert isinstance(sq, SyncContextQueue)
        assert cq.sync_queue is sq  # same instance

    def test_async_queue_property(self):
        cq = ContextQueue()
        aq = cq.async_queue
        assert isinstance(aq, AsyncContextQueue)
        assert cq.async_queue is aq


class TestSyncContextQueue:
    def test_put_and_get(self):
        cq = ContextQueue()
        ctx = Context(None, name="test")
        cq.sync_queue.put(ctx)
        result = cq.sync_queue.get()
        assert result is ctx

    def test_priority_ordering(self):
        cq = ContextQueue()
        ctx_low = Context(None, name="low")
        ctx_high = Context(None, name="high")

        cq.sync_queue.put(ctx_low, ContextPriority.FIFO_LOW)
        cq.sync_queue.put(ctx_high, ContextPriority.FIFO_HIGH)

        # FIFO_HIGH has higher priority (lower value)
        first = cq.sync_queue.get()
        assert first is ctx_high
        second = cq.sync_queue.get()
        assert second is ctx_low


class TestAsyncContextQueue:
    async def test_put_and_get(self):
        cq = ContextQueue()
        ctx = Context(None, name="test")
        await cq.async_queue.put(ctx)
        result = await cq.async_queue.get()
        assert result is ctx

    async def test_priority_ordering(self):
        cq = ContextQueue()
        ctx_low = Context(None, name="low")
        ctx_high = Context(None, name="high")

        await cq.async_queue.put(ctx_low, ContextPriority.FIFO_LOW)
        await cq.async_queue.put(ctx_high, ContextPriority.FIFO_HIGH)

        first = await cq.async_queue.get()
        assert first is ctx_high
        second = await cq.async_queue.get()
        assert second is ctx_low
