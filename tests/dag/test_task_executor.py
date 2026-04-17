from shutils.dag.context import Context
from shutils.dag.dag import DAG
from shutils.dag.executor import ExecutorConfig
from shutils.dag.runtime import Runtime
from shutils.dag.task import AsyncFunctionTask
from shutils.dag.task_executor import TaskExecutor


class TestTaskExecutor:
    async def test_simple_pipeline(self):
        """Test a simple A -> B pipeline with TaskExecutor."""
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

        config = ExecutorConfig(
            task_worker_num=2,
            context_queue_timeout=0.5,
        )
        executor = TaskExecutor(dag, Runtime(), config)
        results = await executor.run()
        assert len(results) > 0

    async def test_parallel_start_tasks(self):
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

        config = ExecutorConfig(
            task_worker_num=2,
            context_queue_timeout=0.5,
        )
        executor = TaskExecutor(dag, Runtime(), config)
        results = await executor.run()
        # Executor may merge parallel paths into fewer output contexts
        assert len(results) >= 1

    async def test_with_input_context(self):
        """Test passing input context."""
        dag = DAG()

        async def task_a(async_ctx):
            return async_ctx

        ta = AsyncFunctionTask(task_a, name="A")
        dag.add_task(ta)
        dag.build()

        runtime = Runtime()
        ctx = Context(runtime, name="input")

        config = ExecutorConfig(
            task_worker_num=2,
            context_queue_timeout=0.5,
        )
        executor = TaskExecutor(dag, runtime, config)
        results = await executor.run(ctx)
        assert len(results) >= 1
