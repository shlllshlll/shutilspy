import pytest

from shutils.dag.context import AsyncContext, Context, SyncContext
from shutils.dag.runtime import Runtime
from shutils.dag.task import (
    AsyncFunctionShutdownTask,
    AsyncFunctionTask,
    AsyncStreamTask,
    Environment,
    ForegroundSyncFunctionTask,
    ForegroundSyncStreamTask,
    SinkNode,
    SourceNode,
    SyncFunctionShutdownTask,
    SyncFunctionTask,
    SyncStreamTask,
    TaskConfig,
)


class TestTaskBase:
    def test_id_auto_generated(self):
        task = AsyncFunctionTask(None)
        assert task.id is not None

    def test_id_from_name(self):
        task = AsyncFunctionTask(None, name="my_task")
        assert task.id == "my_task"

    def test_hash_and_eq(self):
        task1 = AsyncFunctionTask(None, name="same_id")
        task2 = AsyncFunctionTask(None, name="same_id")
        assert task1 == task2
        assert hash(task1) == hash(task2)

    def test_repr(self):
        task = AsyncFunctionTask(None, name="my_task")
        assert "my_task" in repr(task)

    def test_add_upstream(self):
        task1 = AsyncFunctionTask(None, name="task1")
        task2 = AsyncFunctionTask(None, name="task2")
        task2.add_upstream(task1)
        assert task1 in task2.upstream_tasks
        assert task2 in task1.downstream_tasks


class TestTaskConfig:
    def test_defaults(self):
        config = TaskConfig()
        assert config.retry_times == 0
        assert config.parallel_num == 0
        assert config.limiter is None


class TestSyncFunctionTask:
    def test_call_returns_list(self):
        def my_func(sync_ctx: SyncContext) -> SyncContext:
            return sync_ctx

        task = SyncFunctionTask(my_func, name="sync_func")
        ctx = Context(None)
        env = Environment(Runtime(), None, None)
        result = task(ctx, env)
        assert isinstance(result, list)

    def test_call_returns_none(self):
        def my_func(sync_ctx: SyncContext) -> None:
            return None

        task = SyncFunctionTask(my_func, name="sync_none")
        ctx = Context(None)
        env = Environment(Runtime(), None, None)
        result = task(ctx, env)
        assert result == []


class TestAsyncFunctionTask:
    async def test_call_returns_list(self):
        async def my_func(async_ctx: AsyncContext) -> AsyncContext:
            return async_ctx

        task = AsyncFunctionTask(my_func, name="async_func")
        ctx = Context(None)
        env = Environment(Runtime(), None, None)
        result = await task(ctx, env)
        assert isinstance(result, list)

    async def test_call_returns_none(self):
        async def my_func(async_ctx: AsyncContext) -> None:
            return None

        task = AsyncFunctionTask(my_func, name="async_none")
        ctx = Context(None)
        env = Environment(Runtime(), None, None)
        result = await task(ctx, env)
        assert result == []


class TestSyncStreamTask:
    def test_generator_task(self):
        def my_gen():
            ctx = yield
            while ctx is not None:
                yield ctx

        task = SyncStreamTask(my_gen, name="stream_task")
        ctx = Context(None)
        env = Environment(Runtime(), None, None)
        result = task(ctx, env)
        assert isinstance(result, list)

    def test_non_generator_raises(self):
        def not_a_gen():
            return None

        with pytest.raises(ValueError, match="generator"):
            SyncStreamTask(not_a_gen, name="bad_stream")


class TestAsyncStreamTask:
    async def test_async_generator_task(self):
        async def my_gen():
            ctx = yield
            while ctx is not None:
                yield ctx

        task = AsyncStreamTask(my_gen, name="async_stream")
        ctx = Context(None)
        env = Environment(Runtime(), None, None)
        result = await task(ctx, env)
        assert isinstance(result, list)

    def test_non_async_generator_raises(self):
        def not_an_async_gen():
            return None

        with pytest.raises(ValueError, match="async generator"):
            AsyncStreamTask(not_an_async_gen, name="bad_async_stream")


class TestSourceNode:
    async def test_call(self):
        node = SourceNode()
        ctx = Context(None)
        env = Environment(Runtime(), None, None)
        result = await node(ctx, env)
        assert isinstance(result, list)
        assert len(result) == 1


class TestSinkNode:
    async def test_call(self):
        node = SinkNode()
        ctx = Context(None)
        env = Environment(Runtime(), None, None)
        result = await node(ctx, env)
        assert isinstance(result, list)


class TestShutdownTask:
    def test_sync_shutdown(self):
        class MyShutdown:
            def __call__(self, ctx):
                return ctx
            def shutdown(self):
                self.called = True

        callable_obj = MyShutdown()
        task = SyncFunctionShutdownTask(callable_obj, name="shutdown_task")
        task.shutdown()
        assert callable_obj.called is True

    async def test_async_shutdown(self):
        class MyAsyncShutdown:
            async def __call__(self, ctx):
                return ctx
            async def shutdown(self):
                self.called = True

        callable_obj = MyAsyncShutdown()
        task = AsyncFunctionShutdownTask(callable_obj, name="async_shutdown_task")
        await task.shutdown()
        assert callable_obj.called is True


class TestForegroundTask:
    def test_foreground_sync_function(self):
        def my_func(sync_ctx):
            return sync_ctx

        task = ForegroundSyncFunctionTask(my_func, name="fg_func")
        assert isinstance(task, SyncFunctionTask)

    def test_foreground_sync_stream(self):
        def my_gen():
            ctx = yield
            yield ctx

        task = ForegroundSyncStreamTask(my_gen, name="fg_stream")
        assert isinstance(task, SyncStreamTask)
