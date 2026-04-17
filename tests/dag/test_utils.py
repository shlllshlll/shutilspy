import pytest

from shutils.dag.context import Context
from shutils.dag.task import AsyncFunctionTask
from shutils.dag.utils import (
    ResourcePool,
    mask_upstream_task_async,
    mask_upstream_task_sync,
)


class TestResourcePool:
    async def test_create_and_acquire(self):
        pool = ResourcePool(default_size=2, create_func=list)
        async with pool.acquire() as resource:
            assert isinstance(resource, list)

    async def test_provided_resources(self):
        pool = ResourcePool(resources=["a", "b"])
        async with pool.acquire() as r1:
            assert r1 == "a"
            async with pool.acquire() as r2:
                assert r2 == "b"

    async def test_close(self):
        released = []
        pool = ResourcePool(resources=["x"], release_func=lambda r: released.append(r))
        pool.close()
        assert "x" in released

    async def test_acquire_after_close_raises(self):
        pool = ResourcePool(resources=["a"])
        pool.close()
        with pytest.raises(RuntimeError, match="closed"):
            async with pool.acquire():
                pass


class TestMaskUpstreamTask:
    def test_sync(self):
        task1 = AsyncFunctionTask(None, name="A")
        task2 = AsyncFunctionTask(None, name="B")
        task3 = AsyncFunctionTask(None, name="C")
        task2.add_upstream(task1)
        task3.add_upstream(task2)

        ctx = Context(None)
        # Complete task3's upstream to make it available
        ctx.sync_context.complete(task1)
        ctx.sync_context.complete(task2)

        result = mask_upstream_task_sync(ctx.sync_context, task3)
        assert result is not None

    async def test_async(self):
        task1 = AsyncFunctionTask(None, name="A")
        task2 = AsyncFunctionTask(None, name="B")
        task2.add_upstream(task1)

        ctx = Context(None)
        result = await mask_upstream_task_async(ctx.async_context, task2)
        assert result is not None
