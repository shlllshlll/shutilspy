import pytest

from shutils.dag.dag import DAG
from shutils.dag.executor import ExecutorConfig
from shutils.dag.runtime import Runtime
from shutils.dag.task import AsyncFunctionTask


@pytest.fixture
def runtime():
    return Runtime()


@pytest.fixture
def simple_dag():
    """Create a simple linear DAG: A -> B."""
    dag = DAG()

    async def task_a(async_ctx):
        async_ctx.context.sync_white_board["a"] = "done"
        return async_ctx

    async def task_b(async_ctx):
        val = async_ctx.context.sync_white_board.get("a")
        async_ctx.context.sync_white_board["b"] = f"{val}_done"
        return async_ctx

    ta = AsyncFunctionTask(task_a, name="A")
    tb = AsyncFunctionTask(task_b, name="B")
    dag.add_task(ta)
    dag.add_task(tb, [ta])
    dag.build()
    return dag


@pytest.fixture
def diamond_dag():
    """Create a diamond DAG: A -> C, B -> C."""
    dag = DAG()

    async def noop(async_ctx):
        return async_ctx

    ta = AsyncFunctionTask(noop, name="A")
    tb = AsyncFunctionTask(noop, name="B")
    tc = AsyncFunctionTask(noop, name="C")
    dag.add_task(ta)
    dag.add_task(tb)
    dag.add_task(tc, [ta, tb])
    dag.build()
    return dag


@pytest.fixture
def executor_config():
    return ExecutorConfig(
        context_worker_num=2,
        task_worker_num=2,
        context_queue_timeout=0.5,
    )
