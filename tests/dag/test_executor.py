
import pytest

from shutils.dag.context import Context
from shutils.dag.dag import DAG
from shutils.dag.executor import Executor, ExecutorConfig, _worker_context_var, worker_local
from shutils.dag.runtime import Runtime
from shutils.dag.task import AsyncFunctionTask


class TestExecutorConfig:
    def test_defaults(self):
        config = ExecutorConfig()
        assert config.context_worker_num == 1
        assert config.task_worker_num == 1
        assert config.context_queue_timeout == 1
        assert config.enable_context_gc is True
        assert config.enable_context_bypass is True


class TestWorkerLocal:
    def test_outside_context_raises(self):
        with pytest.raises(AttributeError):
            _ = worker_local.some_attr

    async def test_inside_context(self):
        storage = {}
        token = _worker_context_var.set(storage)
        try:
            worker_local.test_key = "test_value"
            assert worker_local.test_key == "test_value"
            assert "test_key" in worker_local
        finally:
            _worker_context_var.reset(token)


class TestExecutor:
    async def test_simple_pipeline(self):
        """Test a simple A -> B pipeline."""
        dag = DAG()

        async def task_a(async_ctx):
            async_ctx.context.sync_white_board["a"] = "done"
            return async_ctx

        async def task_b(async_ctx):
            async_ctx.context.sync_white_board["b"] = "done"
            return async_ctx

        ta = AsyncFunctionTask(task_a, name="A")
        tb = AsyncFunctionTask(task_b, name="B")
        dag.add_task(ta)
        dag.add_task(tb, [ta])
        dag.build()

        config = ExecutorConfig(context_worker_num=1, context_queue_timeout=0.5)
        executor = Executor(dag, Runtime(), config)
        results = await executor.run()
        assert len(results) > 0

    async def test_parallel_tasks(self):
        """Test two parallel start tasks."""
        dag = DAG()

        async def task_a(async_ctx):
            return async_ctx

        async def task_b(async_ctx):
            return async_ctx

        ta = AsyncFunctionTask(task_a, name="A")
        tb = AsyncFunctionTask(task_b, name="B")
        dag.add_task(ta)
        dag.add_task(tb)
        dag.build()

        config = ExecutorConfig(context_worker_num=2, context_queue_timeout=0.5)
        executor = Executor(dag, Runtime(), config)
        results = await executor.run()
        # Executor may merge parallel paths into fewer output contexts
        assert len(results) >= 1

    async def test_diamond_dag(self):
        """Test diamond DAG: A -> C, B -> C."""
        dag = DAG()

        async def task_a(async_ctx):
            return async_ctx

        async def task_b(async_ctx):
            return async_ctx

        async def task_c(async_ctx):
            return async_ctx

        ta = AsyncFunctionTask(task_a, name="A")
        tb = AsyncFunctionTask(task_b, name="B")
        tc = AsyncFunctionTask(task_c, name="C")
        dag.add_task(ta)
        dag.add_task(tb)
        dag.add_task(tc, [ta, tb])
        dag.build()

        config = ExecutorConfig(context_worker_num=2, context_queue_timeout=0.5)
        executor = Executor(dag, Runtime(), config)
        results = await executor.run()
        assert len(results) >= 1

    async def test_with_input_context(self):
        """Test passing input context to executor."""
        dag = DAG()

        async def task_a(async_ctx):
            return async_ctx

        ta = AsyncFunctionTask(task_a, name="A")
        dag.add_task(ta)
        dag.build()

        runtime = Runtime()
        ctx = Context(runtime, name="input_ctx")

        config = ExecutorConfig(context_worker_num=1, context_queue_timeout=0.5)
        executor = Executor(dag, runtime, config)
        results = await executor.run(ctx)
        assert len(results) >= 1
