
import pytest

from shutils.dag.context import Context
from shutils.dag.dag import DAG
from shutils.dag.executor import ExecutorConfig
from shutils.dag.runtime import Runtime
from shutils.dag.serve_executor import ContextStatus, ServeExecutor
from shutils.dag.task import AsyncFunctionTask


class TestServeExecutor:
    async def test_submit_and_status(self):
        """Test submitting a context and checking status."""
        dag = DAG()

        async def task_a(async_ctx):
            return async_ctx

        ta = AsyncFunctionTask(task_a, name="A")
        dag.add_task(ta)
        dag.build()

        config = ExecutorConfig(context_worker_num=1, context_queue_timeout=0.1)
        executor = ServeExecutor(dag, Runtime(), config)

        ctx = Context(None, name="test_submit")
        task_id = await executor.submit_task(ctx)
        assert task_id == "test_submit"

        status = await executor.get_task_status(task_id)
        assert status in (ContextStatus.INIT, ContextStatus.RUNNING, ContextStatus.FINISH)

    async def test_duplicate_submit_raises(self):
        dag = DAG()
        ta = AsyncFunctionTask(None, name="A")
        dag.add_task(ta)
        dag.build()

        config = ExecutorConfig(context_worker_num=1, context_queue_timeout=0.1)
        executor = ServeExecutor(dag, Runtime(), config)

        ctx = Context(None, name="dup_ctx")
        await executor.submit_task(ctx)
        with pytest.raises(ValueError, match="already submitted"):
            await executor.submit_task(ctx)

    async def test_get_task_status_not_found(self):
        dag = DAG()
        ta = AsyncFunctionTask(None, name="A")
        dag.add_task(ta)
        dag.build()

        config = ExecutorConfig(context_worker_num=1, context_queue_timeout=0.1)
        executor = ServeExecutor(dag, Runtime(), config)

        with pytest.raises(ValueError, match="not found"):
            await executor.get_task_status("nonexistent")

    async def test_get_task_result_not_found(self):
        dag = DAG()
        ta = AsyncFunctionTask(None, name="A")
        dag.add_task(ta)
        dag.build()

        config = ExecutorConfig(context_worker_num=1, context_queue_timeout=0.1)
        executor = ServeExecutor(dag, Runtime(), config)

        with pytest.raises(ValueError, match="not found"):
            await executor.get_task_result("nonexistent")
