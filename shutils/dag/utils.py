"""DAG utility functions for resource pooling, loop-safe coroutine running, and task masking."""

import asyncio
import concurrent.futures
import threading
from collections import deque
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager

from .context import AsyncContext, SyncContext
from .task import TaskBase

__all__ = [
    "ResourcePool",
    "get_loop_safe_runner",
    "mask_downstream_task_async",
    "mask_downstream_task_sync",
    "mask_upstream_task_async",
    "mask_upstream_task_sync",
]


class ResourcePool[T]:
    """Async resource pool with optional creation and release callbacks."""

    def __init__(
        self,
        default_size: int = 0,
        max_size: int = 0,
        create_func: Callable[[], T] | None = None,
        release_func: Callable[[T], None] | None = None,
        resources: list[T] | None = None,
    ):
        """Initialize the resource pool.

        Args:
            default_size: Number of initial resources to create.
            max_size: Maximum pool size (0 means unlimited).
            create_func: Factory function for creating new resources.
            release_func: Callback for releasing resources on close.
            resources: Pre-existing resources to seed the pool.
        """
        self._resource_queue: asyncio.Queue[T] = asyncio.Queue(maxsize=max_size)
        if resources is not None:
            for resource in resources:
                self._resource_queue.put_nowait(resource)
        elif default_size and create_func:
            for _ in range(default_size):
                self._resource_queue.put_nowait(create_func())
        self._create_func = create_func
        self._release_func = release_func
        self._closed = False

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[T]:
        """Acquire a resource from the pool as an async context manager.

        Returns the resource on entry and returns it to the pool on exit.
        """
        if self._closed:
            raise RuntimeError("ResourcePool is closed")
        try:
            resource = self._resource_queue.get_nowait()
        except asyncio.QueueEmpty:
            if self._create_func:
                resource = self._create_func()
            else:
                resource = await self._resource_queue.get()
        try:
            yield resource
        finally:
            if not self._closed:
                try:
                    self._resource_queue.put_nowait(resource)
                except (asyncio.QueueFull, asyncio.QueueShutDown):
                    if self._release_func:
                        self._release_func(resource)
            else:
                if self._release_func:
                    self._release_func(resource)

    def close(self):
        """Close the pool, release all resources, and shut down the queue."""
        self._closed = True
        while not self._resource_queue.empty():
            resource = self._resource_queue.get_nowait()
            if self._release_func:
                self._release_func(resource)
        self._resource_queue.shutdown()


def get_loop_safe_runner(coro: Coroutine) -> asyncio.Future | concurrent.futures.Future:
    """Run a coroutine safely from any thread.

    Determines whether to use create_task or run_coroutine_threadsafe
    based on the current thread and event loop state.

    Args:
        coro: The coroutine to run.

    Returns:
        A Future or asyncio.Task for the coroutine result.

    Raises:
        RuntimeError: If no running event loop is found.
    """
    try:
        loop = asyncio.get_running_loop()
        # 如果能获取到当前运行的事件循环，说明是在主线程中
        if threading.current_thread() is threading.main_thread():
            # 在主线程事件循环中，可以直接创建一个任务
            return loop.create_task(coro)
        else:
            # 在子线程中，需要使用run_coroutine_threadsafe
            return asyncio.run_coroutine_threadsafe(coro, loop)
    except RuntimeError:
        # 如果没有运行中的事件循环，可能是在另一个线程中
        # 这种情况下可能需要特殊处理
        raise RuntimeError("No running event loop - cannot run coroutine") from None


def __mask_common(task_list: TaskBase | list[TaskBase], mask_self: bool, up_down: str):
    if isinstance(task_list, TaskBase):
        task_list = [task_list]
    task_set = set()
    task_queue = deque()
    for task in task_list:
        for up_or_downtask in getattr(task, up_down):
            if up_or_downtask not in task_set:
                task_set.add(up_or_downtask)
                task_queue.append(up_or_downtask)
        if mask_self and task not in task_set:
            task_set.add(task)
            task_queue.append(task)

    while task_queue:
        cur_task = task_queue.popleft()
        for up_or_downtask in getattr(cur_task, up_down):
            if up_or_downtask not in task_set:
                task_set.add(up_or_downtask)
                task_queue.append(up_or_downtask)
        yield cur_task


def mask_upstream_task_sync(context: SyncContext, task_list: TaskBase | list[TaskBase], mask_self: bool = False):
    """Mark all upstream tasks of the given tasks as completed in a sync context.

    Args:
        context: The context to update.
        task_list: Task(s) whose upstream tasks should be masked.
        mask_self: Whether to also mark the given tasks as completed.

    Returns:
        The updated context.
    """
    for task in __mask_common(task_list, mask_self, "upstream_tasks"):
       context.complete(task)
    return context


async def mask_upstream_task_async(
    context: AsyncContext, task_list: TaskBase | list[TaskBase], mask_self: bool = False
):
    """Mark all upstream tasks of the given tasks as completed in an async context.

    Args:
        context: The context to update.
        task_list: Task(s) whose upstream tasks should be masked.
        mask_self: Whether to also mark the given tasks as completed.

    Returns:
        The updated context.
    """
    for task in __mask_common(task_list, mask_self, "upstream_tasks"):
        await context.complete(task)
    return context


def mask_downstream_task_sync(context: SyncContext, task_list: TaskBase | list[TaskBase], mask_self: bool = False):
    """Mark all downstream tasks of the given tasks as completed in a sync context.

    Args:
        context: The context to update.
        task_list: Task(s) whose downstream tasks should be masked.
        mask_self: Whether to also mark the given tasks as completed.

    Returns:
        The updated context.
    """
    for task in __mask_common(task_list, mask_self, "downstream_tasks"):
        context.complete(task)
    return context


async def mask_downstream_task_async(
    context: AsyncContext, task_list: TaskBase | list[TaskBase], mask_self: bool = False
):
    """Mark all downstream tasks of the given tasks as completed in an async context.

    Args:
        context: The context to update.
        task_list: Task(s) whose downstream tasks should be masked.
        mask_self: Whether to also mark the given tasks as completed.

    Returns:
        The updated context.
    """
    for task in __mask_common(task_list, mask_self, "downstream_tasks"):
        await context.complete(task)
    return context
