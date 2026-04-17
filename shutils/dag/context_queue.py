"""Priority-based context queue for DAG executors."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum

from .context import Context
from .lib.aio_queue import PriorityQueue
from .lib.smart_lock import SmartLock

__all__ = [
    "AsyncContextQueue",
    "ContextPriority",
    "ContextQueue",
    "PrioritizedItem",
    "SyncContextQueue",
]

class ContextPriority(Enum):
    """Priority levels for context scheduling."""

    LIFO = 0
    FIFO_HIGH = 1
    FIFO_LOW = 2

@dataclass(order=True)
class PrioritizedItem:
    """Context queue item with priority and sequence for ordering.

    Attributes:
        priority: Numeric priority value.
        sequence: Sequence number for ordering within the same priority.
        item: The context payload.
    """
    priority: int
    sequence: int
    item: Context = field(compare=False)


class ContextQueue:
    """Priority queue for contexts with sync and async interfaces."""

    def __init__(self):
        """Initialize the context queue with per-priority counters."""
        self._priority_queue: PriorityQueue[PrioritizedItem] = PriorityQueue()
        self._counter_dict: dict[ContextPriority, int] = {}
        self._lock = SmartLock()
        self._sync_queue = None
        self._async_queue = None
        for priority in ContextPriority:
            self._counter_dict[priority] = 0

    @property
    def sync_queue(self) -> "SyncContextQueue":
        """Lazy accessor for the synchronous queue interface."""
        if self._sync_queue is None:
            self._sync_queue = SyncContextQueue(self)
        return self._sync_queue

    @property
    def async_queue(self) -> "AsyncContextQueue":
        """Lazy accessor for the asynchronous queue interface."""
        if self._async_queue is None:
            self._async_queue = AsyncContextQueue(self)
        return self._async_queue

class SyncContextQueue:
    """Synchronous blocking interface for the context queue."""

    def __init__(self, context_queue: ContextQueue):
        """Initialize with the underlying context queue.

        Args:
            context_queue: The shared context queue instance.
        """
        self.__context_queue = context_queue

    def get(self) -> Context:
        """Get the highest-priority context from the queue."""
        return self.__context_queue._priority_queue.sync_q.get().item

    def put(self, context: Context, priority: ContextPriority = ContextPriority.FIFO_HIGH):
        """Put a context into the queue with the given priority.

        Args:
            context: The context to enqueue.
            priority: The priority level for scheduling.
        """
        def runner():
            self.__context_queue._priority_queue.sync_q.put(
                PrioritizedItem(priority.value, self.__context_queue._counter_dict[priority], context)
            )
            if priority == ContextPriority.LIFO:
                self.__context_queue._counter_dict[priority] -= 1
            else:
                self.__context_queue._counter_dict[priority] += 1

        self.__context_queue._lock.sync_run(runner)

class AsyncContextQueue:
    """Asynchronous interface for the context queue."""

    def __init__(self, context_queue: ContextQueue):
        """Initialize with the underlying context queue.

        Args:
            context_queue: The shared context queue instance.
        """
        self.__context_queue = context_queue

    async def get(self) -> Context:
        """Async get the highest-priority context from the queue."""
        ret = await self.__context_queue._priority_queue.async_q.get()
        return ret.item

    @asynccontextmanager
    async def _get_with_context(self) -> AsyncGenerator[Context]:
        get_context = False
        try:
            yield await self.get()
            get_context = True
        finally:
            if get_context:
                self.__context_queue._priority_queue.async_q.task_done()

    async def put(self, context: Context, priority: ContextPriority = ContextPriority.FIFO_HIGH):
        """Async put a context into the queue with the given priority.

        Args:
            context: The context to enqueue.
            priority: The priority level for scheduling.
        """
        async def runner():
            item = PrioritizedItem(priority.value, self.__context_queue._counter_dict[priority], context)
            await self.__context_queue._priority_queue.async_q.put(item)
            if priority == ContextPriority.LIFO:
                self.__context_queue._counter_dict[priority] -= 1
            else:
                self.__context_queue._counter_dict[priority] += 1

        await self.__context_queue._lock.async_run(runner)

