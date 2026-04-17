from shutils.dag.context import (
    AsyncContext,
    Context,
    LoopContext,
    OutputContext,
    RateLimitContext,
    StopContext,
    SyncContext,
)
from shutils.dag.runtime import Runtime
from shutils.dag.task import AsyncFunctionTask


class TestContext:
    def test_init(self):
        ctx = Context(None)
        assert ctx.id is not None
        assert ctx.is_destory() is False

    def test_init_with_name(self):
        ctx = Context(None, name="test_ctx")
        assert ctx.id == "test_ctx"

    def test_init_with_runtime(self):
        runtime = Runtime()
        Context(runtime)
        assert runtime.counter == 1

    def test_sync_context_property(self):
        ctx = Context(None)
        sync_ctx = ctx.sync_context
        assert isinstance(sync_ctx, SyncContext)
        # Same instance returned
        assert ctx.sync_context is sync_ctx

    def test_async_context_property(self):
        ctx = Context(None)
        async_ctx = ctx.async_context
        assert isinstance(async_ctx, AsyncContext)
        assert ctx.async_context is async_ctx

    def test_parent_child(self):
        parent = Context(None)
        child = Context(None, parent=parent)
        assert child._parent_context is parent
        assert child in parent._child_context_list

    def test_repr(self):
        ctx = Context(None, name="myctx")
        assert "myctx" in repr(ctx)


class TestSyncContext:
    def test_destroy(self):
        runtime = Runtime()
        ctx = Context(runtime)
        assert runtime.counter == 1
        ctx.sync_context.destory()
        assert ctx.is_destory() is True
        assert runtime.counter == 0

    def test_create(self):
        ctx = Context(None)
        new_ctx = ctx.sync_context.create()
        assert isinstance(new_ctx, SyncContext)
        assert new_ctx.context is not ctx

    def test_create_child(self):
        ctx = Context(None)
        child = ctx.sync_context.create_child()
        assert isinstance(child, SyncContext)
        assert child.context._parent_context is ctx

    def test_create_multiple_children(self):
        ctx = Context(None)
        children = ctx.sync_context.create_child(num=3)
        assert len(children) == 3

    def test_child_context_num(self):
        ctx = Context(None)
        ctx.sync_context.create_child()
        ctx.sync_context.create_child()
        assert ctx.sync_context.child_context_num() == 2

    def test_freeze_thraw(self):
        ctx = Context(None)
        ctx.sync_context.freeze()
        assert ctx.freezing is True
        ctx.sync_context.thraw()
        assert ctx.freezing is False

    def test_destroy_idempotent(self):
        ctx = Context(None)
        ctx.sync_context.destory()
        ctx.sync_context.destory()  # should not raise

    def test_destroy_cascades_children(self):
        parent = Context(None)
        child = Context(None, parent=parent)
        parent.sync_context.destory()
        assert child.is_destory() is True


class TestAsyncContext:
    async def test_destroy(self):
        runtime = Runtime()
        ctx = Context(runtime)
        assert runtime.counter == 1
        await ctx.async_context.destory()
        assert ctx.is_destory() is True
        assert runtime.counter == 0

    async def test_create(self):
        ctx = Context(None)
        new_ctx = await ctx.async_context.create()
        assert isinstance(new_ctx, AsyncContext)

    async def test_create_child(self):
        ctx = Context(None)
        child = await ctx.async_context.create_child()
        assert isinstance(child, AsyncContext)

    async def test_create_multiple_children(self):
        ctx = Context(None)
        children = await ctx.async_context.create_child(num=3)
        assert len(children) == 3

    async def test_child_context_num(self):
        ctx = Context(None)
        await ctx.async_context.create_child()
        await ctx.async_context.create_child()
        assert await ctx.async_context.child_context_num() == 2

    async def test_parent_context(self):
        parent = Context(None)
        child = Context(None, parent=parent)
        result = await child.async_context.parent_context()
        assert result.context is parent

    async def test_destroy_cascades_children(self):
        parent = Context(None)
        child = Context(None, parent=parent)
        await parent.async_context.destory()
        assert child.is_destory() is True


class TestStopContext:
    def test_init(self):
        ctx = StopContext()
        assert isinstance(ctx, Context)

class TestOutputContext:
    def test_init(self):
        ctx = OutputContext()
        assert isinstance(ctx, Context)

class TestLoopContext:
    def test_init(self):
        task = AsyncFunctionTask(None, name="loop_task")
        ctx = LoopContext(None, task)
        assert isinstance(ctx, Context)

class TestRateLimitContext:
    def test_init(self):
        parent = Context(None)
        ctx = RateLimitContext(parent)
        assert ctx.context is parent
