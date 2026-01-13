#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: task_queue.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief: [WIP] Task-based priority queue for SimplifiedExecutor
"""

from dataclasses import dataclass, field
import logging
from enum import Enum
import threading
import asyncio
import janus

from .context import Context
from .task import TaskBase
from .context_queue import ContextPriority


logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    """Priority levels for task scheduling.

    LIFO_HIGH (0): Tasks from a context that was just executed.
                   Ensures the same DAG path is prioritized.
    FIFO_HIGH (1): Tasks from newly created contexts.
    FIFO_LOW (2): Tasks from LoopContext (background/loop work).
    """
    LIFO_HIGH = 0
    FIFO_HIGH = 1
    FIFO_LOW = 2


@dataclass(order=True)
class TaskItem:
    """Represents a single task to be executed in the global task queue.

    Combines context and task information for task-level scheduling.
    """
    priority: int
    sequence: int
    context: Context = field(compare=False)
    task: TaskBase = field(compare=False)

class TaskPriorityQueue:
    """Global task queue with priority support.

    Replaces context-based queue with task-based scheduling for
    SimplifiedExecutor. Each task is individually queued with its
    priority, allowing fine-grained concurrency control.
    """

    def __init__(self):
        self._queue: janus.PriorityQueue[TaskItem] = janus.PriorityQueue()
        self._sequence_counters: dict[int, int] = {
            TaskPriority.LIFO_HIGH.value: 0,
            TaskPriority.FIFO_HIGH.value: 0,
            TaskPriority.FIFO_LOW.value: 0,
        }
        self._sync_lock = threading.Lock()
        self._async_lock = asyncio.Lock()

    async def async_put_task(
        self,
        context: Context,
        task: TaskBase,
        priority: TaskPriority = TaskPriority.FIFO_HIGH
    ) -> None:
        """Put a single task into the queue with priority.

        Args:
            context: The context containing this task
            task: The task to execute
            priority: Priority level for this task
            context_priority: Original context priority for tracking
        """
        async with self._async_lock:
            with self._sync_lock:
                seq = self._sequence_counters[priority.value]
                self._sequence_counters[priority.value] += 1

                item = TaskItem(
                    priority=priority.value,
                    sequence=seq,
                    context=context,
                    task=task,
                )
                await self._queue.async_q.put(item)

    async def async_get_task(self) -> TaskItem:
        """Get a task from the queue.

        Returns:
            TaskItem containing the context and task to execute

        Raises:
            asyncio.CancelledError: If the get operation is cancelled
        """
        ret = await self._queue.async_q.get()
        self._queue.async_q.task_done()
        return ret

    async def async_put_context_tasks(
        self,
        context: Context,
        priority: TaskPriority = TaskPriority.FIFO_HIGH
    ) -> int:
        """Put all available tasks from a context into the queue.

        Used when a context first enters the system or when new tasks
        become available after task completion.

        Args:
            context: The context to get tasks from
            priority: Priority level for all tasks from this context
            context_priority: Original context priority for tracking

        Returns:
            Number of tasks enqueued
        """


        if context.is_destory():
            return 0

        available_tasks = await context.async_task_state.avaliable_task()
        count = 0

        logger.debug(f"async_put_context_tasks: context={context}, available_tasks={available_tasks}")
        for task in available_tasks:
            await self.async_put_task(context, task, priority)
            count += 1

        return count

    @property
    def size(self) -> int:
        """Get the current queue size."""
        return self._queue.async_q.qsize()

    async def join(self) -> None:
        """Wait for all items in the queue to be processed."""
        await self._queue.async_q.join()
