from shutils.dag.context import Context
from shutils.dag.task import AsyncFunctionTask
from shutils.dag.task_queue import TaskItem, TaskPriority, TaskPriorityQueue


class TestTaskPriority:
    def test_values(self):
        assert TaskPriority.LIFO_HIGH.value == 0
        assert TaskPriority.FIFO_HIGH.value == 1
        assert TaskPriority.FIFO_LOW.value == 2


class TestTaskItem:
    def test_ordering(self):
        item1 = TaskItem(priority=0, sequence=0, context=None, task=None)
        item2 = TaskItem(priority=1, sequence=0, context=None, task=None)
        assert item1 < item2

    def test_same_priority_ordering(self):
        item1 = TaskItem(priority=0, sequence=0, context=None, task=None)
        item2 = TaskItem(priority=0, sequence=1, context=None, task=None)
        assert item1 < item2


class TestTaskPriorityQueue:
    async def test_put_and_get(self):
        queue = TaskPriorityQueue()
        ctx = Context(None)
        task = AsyncFunctionTask(None, name="test_task")

        await queue.async_put_task(ctx, task)
        item = await queue.async_get_task()

        assert item.context is ctx
        assert item.task is task

    async def test_priority_ordering(self):
        queue = TaskPriorityQueue()
        ctx = Context(None)
        task_high = AsyncFunctionTask(None, name="high")
        task_low = AsyncFunctionTask(None, name="low")

        await queue.async_put_task(ctx, task_low, TaskPriority.FIFO_LOW)
        await queue.async_put_task(ctx, task_high, TaskPriority.FIFO_HIGH)

        first = await queue.async_get_task()
        assert first.task is task_high
        second = await queue.async_get_task()
        assert second.task is task_low

    async def test_size(self):
        queue = TaskPriorityQueue()
        assert queue.size == 0
        ctx = Context(None)
        task = AsyncFunctionTask(None, name="test")
        await queue.async_put_task(ctx, task)
        assert queue.size == 1

    async def test_async_put_context_tasks(self):
        queue = TaskPriorityQueue()
        ctx = Context(None)
        task1 = AsyncFunctionTask(None, name="task1")
        task2 = AsyncFunctionTask(None, name="task2")

        # Manually add available tasks
        ctx._available_tasks.add(task1)
        ctx._available_tasks.add(task2)

        count = await queue.async_put_context_tasks(ctx)
        assert count == 2
        assert queue.size == 2

    async def test_async_put_context_tasks_destroyed(self):
        queue = TaskPriorityQueue()
        ctx = Context(None)
        ctx.set_destory(True)
        count = await queue.async_put_context_tasks(ctx)
        assert count == 0
