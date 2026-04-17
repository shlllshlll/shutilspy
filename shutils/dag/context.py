"""Context management for DAG execution, including sync/async context wrappers."""

import time
import uuid
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, overload

from .data_white_board import (
    AsyncDataWhiteBoard,
    DataWhiteBoardMixin,
    SyncDataWhiteBoard,
)
from .global_data import debug_mode
from .lib.smart_lock import SmartRWLock
from .task_state import AsyncTaskState, SyncTaskState, TaskStateMixin

if TYPE_CHECKING:
    from .runtime import Runtime
    from .task import TaskBase

__all__ = [
    "AsyncContext",
    "Context",
    "LoopContext",
    "OutputContext",
    "RateLimitContext",
    "StopContext",
    "SyncContext",
]


class Context(DataWhiteBoardMixin, TaskStateMixin):
    """Core context object carrying data and task state through the DAG."""

    def __init__(self, runtime: "Runtime | None", parent: "Context | None" = None, name: str = ""):
        """Initialize a context with optional runtime, parent, and name.

        Args:
            runtime: The runtime for tracking active context count.
            parent: Optional parent context for hierarchical contexts.
            name: Optional context name; defaults to a UUID.
        """
        DataWhiteBoardMixin.__init__(self)
        TaskStateMixin.__init__(self)
        self.id = name if name else str(uuid.uuid4())
        self.parent_rwlock = SmartRWLock()
        self._parent_context: Context | None = parent
        self._child_context_list: list[Context] = []
        self._child_context_num: int = 0
        self._runtime = runtime
        self.freezing: bool = False
        self.awake_time: dict[TaskBase, float] = {}
        if self._runtime:
            self._runtime.sync_counter.increase()
        if parent:
            # if parent._parent_context is not None:
            #     raise ValueError("parent context must be a root context")
            with parent.parent_rwlock.write():
                parent._child_context_num += 1
                parent._child_context_list.append(self)
        self._sync_context = None
        self._async_context = None

    def __repr__(self):
        if debug_mode:
            return (
                f"{self.__class__.__name__}("
                f"data={DataWhiteBoardMixin.__repr__(self)}, "
                f"state={TaskStateMixin.__repr__(self)}, "
                f"parent={self._parent_context}, "
                f"child_context_num={self._child_context_num}, "
                f"complete_tasks={self._completed_tasks}, "
                f"available_tasks={self.available_tasks})"
            )
        else:
            return f"{self.__class__.__name__}(id={self.id})"

    @property
    def sync_context(self) -> "SyncContext":
        """Lazy accessor for the sync context wrapper."""
        if self._sync_context is None:
            self._sync_context = SyncContext(self)
        return self._sync_context

    @property
    def async_context(self) -> "AsyncContext":
        """Lazy accessor for the async context wrapper."""
        if self._async_context is None:
            self._async_context = AsyncContext(self)
        return self._async_context

    def _awake_interval(self, time_interval: float | int, task: "TaskBase") -> None:
        """Schedule a task to re-awaken after a time interval.

        Args:
            time_interval: Seconds to wait before the task can re-awaken.
            task: The task to schedule.
        """
        self.awake_time[task] = time.time() + time_interval


class SyncContext(SyncDataWhiteBoard, SyncTaskState):
    """Synchronous wrapper around Context for thread-safe operations."""

    def __init__(self, context: Context):
        """Initialize with the underlying context.

        Args:
            context: The core context object.
        """
        SyncDataWhiteBoard.__init__(self, context)
        SyncTaskState.__init__(self, context)
        self.__context = context

    @property
    def id(self) -> str:
        """Unique identifier of the context."""
        return self.__context.id

    @property
    def context(self) -> Context:
        """Access the underlying core context."""
        return self.__context

    @property
    def async_context(self) -> "AsyncContext":
        """Access the async context wrapper for the same core context."""
        return self.__context.async_context

    def freeze(self):
        """Freeze the context to prevent it from being scheduled."""
        self.__context.freezing = True

    def thraw(self):
        """Unfreeze the context to allow scheduling again."""
        self.__context.freezing = False

    def destory(self, destory_parent: bool = False):
        """Destroy the context and release runtime resources.

        Args:
            destory_parent: If True, also destroy the parent when this is its last child.
        """
        if self.__context.is_destory():
            return

        self.__context.set_destory(True)
        if self.__context._runtime:
            self.__context._runtime.sync_counter.decrease()
        if self.__context._parent_context:
            with self.__context._parent_context.parent_rwlock.write():
                self.__context._parent_context._child_context_num -= 1
                if destory_parent and self.__context._parent_context._child_context_num == 0:
                        self.__context._parent_context.sync_context.destory()
        if self.__context._child_context_list:
            for child in self.__context._child_context_list:
                child.sync_context.destory()

    def create(
        self, copy_data: bool = False, deep_copy: bool = False, name: str = "", skip_complete: bool = False
    ) -> "SyncContext":
        """Create a new context, optionally copying data from this one.

        Args:
            copy_data: Whether to copy data to the new context.
            deep_copy: Whether to deep-copy data instead of sharing references.
            name: Optional name for the new context.
            skip_complete: If True, skip completion tracking for the new context.

        Returns:
            The sync wrapper of the new context.
        """
        new_context = Context(self.__context._runtime, name=name)
        if copy_data:
            self.copy(new_context, deep_copy)
        new_context._skip_complete = skip_complete
        return new_context.sync_context

    def child_context_num(self) -> int:
        """Get the number of child contexts."""
        with self.__context.parent_rwlock.read():
            return self.__context._child_context_num

    def iter_child_context(self):
        """Iterate over child contexts under a read lock."""
        with self.__context.parent_rwlock.read():
            yield from self.__context._child_context_list

    def parent_context(self) -> "Context | None":
        """Get the parent context."""
        with self.__context.parent_rwlock.read():
            return self.__context._parent_context

    def create_child(self, num: int = 0) -> "list[SyncContext] | SyncContext":
        """Create one or more child contexts.

        Args:
            num: Number of children to create. 0 creates a single child.

        Returns:
            A single SyncContext if num is 0, otherwise a list of SyncContexts.
        """
        if num:
            return [Context(self.__context._runtime, self.__context).sync_context for _ in range(num)]
        else:
            return Context(self.__context._runtime, self.__context).sync_context

    def complete(self, task: "TaskBase"):
        """Mark a task as completed and propagate to the parent context."""
        super()._complete(task)
        if self.__context._parent_context:
            self.__context._parent_context.sync_context.complete(task)


class AsyncContext(AsyncDataWhiteBoard, AsyncTaskState):
    """Asynchronous wrapper around Context for async-safe operations."""

    def __init__(self, context: Context):
        """Initialize with the underlying context.

        Args:
            context: The core context object.
        """
        AsyncDataWhiteBoard.__init__(self, context)
        AsyncTaskState.__init__(self, context)
        self.__context = context

    @property
    def id(self) -> str:
        """Unique identifier of the context."""
        return self.__context.id

    @property
    def context(self) -> Context:
        """Access the underlying core context."""
        return self.__context

    @property
    def sync_context(self) -> "SyncContext":
        """Access the sync context wrapper for the same core context."""
        return self.__context.sync_context

    def freeze(self):
        """Freeze the context to prevent it from being scheduled."""
        self.__context.freezing = True

    def thraw(self):
        """Unfreeze the context to allow scheduling again."""
        self.__context.freezing = False

    async def destory(self, destory_parent: bool = False):
        """Destroy the context and release runtime resources.

        Args:
            destory_parent: If True, also destroy the parent when this is its last child.
        """
        if self.__context.is_destory():
            return

        self.__context.set_destory(True)
        if self.__context._runtime:
            await self.__context._runtime.async_counter.decrease()
        if self.__context._parent_context:
            async with self.__context._parent_context.parent_rwlock.async_read():
                self.__context._parent_context._child_context_num -= 1
                if destory_parent and self.__context._parent_context._child_context_num == 0:
                    await self.__context._parent_context.async_context.destory()
        if self.__context._child_context_list:
            for child in self.__context._child_context_list:
                await child.async_context.destory()

    async def create(
        self, copy_data: bool = False, deep_copy: bool = False, name: str = "", skip_complete: bool = False
    ) -> "AsyncContext":
        """Create a new context, optionally copying data from this one.

        Args:
            copy_data: Whether to copy data to the new context.
            deep_copy: Whether to deep-copy data instead of sharing references.
            name: Optional name for the new context.
            skip_complete: If True, skip completion tracking for the new context.

        Returns:
            The async wrapper of the new context.
        """
        new_context = Context(self.__context._runtime, name=name)
        if copy_data:
            await self.copy(new_context, deep_copy)
        new_context._skip_complete = skip_complete
        return new_context.async_context

    async def child_context_num(self) -> int:
        """Get the number of child contexts."""
        async with self.__context.parent_rwlock.async_read():
            return self.__context._child_context_num

    async def iter_child_context(self) -> AsyncGenerator["AsyncContext"]:
        """Iterate over child contexts under an async read lock."""
        async with self.__context.parent_rwlock.async_read():
            for child in self.__context._child_context_list:
                yield child.async_context

    async def parent_context(self) -> "AsyncContext | None":
        """Get the parent context, or None if this is a root context."""
        async with self.__context.parent_rwlock.async_read():
            if self.__context._parent_context:
                return self.__context._parent_context.async_context
            else:
                return None

    @overload
    async def create_child(self, num: int = 0, name: str | None = None) -> "AsyncContext": ...

    @overload
    async def create_child(self, num: int, name: str | list[str] | None = None) -> list["AsyncContext"]: ...

    async def create_child(
        self, num: int = 0, name: str | list[str] | None = None
    ) -> "AsyncContext | list[AsyncContext]":
        """Create one or more child contexts.

        Args:
            num: Number of children to create. 0 creates a single child.
            name: Optional name(s) for the children.

        Returns:
            A single AsyncContext if num is 0, otherwise a list of AsyncContexts.
        """
        # if self.__context._parent_context is not None:
        #     raise ValueError("parent context must be a root context")
        return_context = num == 0
        num = num if num else 1
        if name is None:
            name = [""] * num
        elif isinstance(name, str):
            name = [name] * num
        elif isinstance(name, list) and len(name) != num:
            raise ValueError("name list length must be equal to num")
        context = []
        for idx in range(num):
            sub_context = Context(self.__context._runtime, name=name[idx])
            sub_context._parent_context = self.__context
            async with self.__context.parent_rwlock.async_write():
                self.__context._child_context_list.append(sub_context)
                self.__context._child_context_num += 1
            context.append(sub_context.async_context)

        if not return_context:
            return context
        else:
            return context[0]

    async def complete(self, task: "TaskBase"):
        """Mark a task as completed and propagate to the parent context."""
        await super()._complete(task)
        if self.__context._parent_context:
            await self.__context._parent_context.async_context.complete(task)


class LoopContext(Context):
    """Context used for loop tasks, pre-activating the loop task."""

    def __init__(self, runtime: "Runtime | None", task: "TaskBase", name: str = "LoopContext"):
        """Initialize a LoopContext with the loop task already available.

        Args:
            runtime: The runtime for tracking active context count.
            task: The loop task to make immediately available.
            name: Optional context name.
        """
        super().__init__(runtime, name=name)
        self._add_available_task(task)


class RateLimitContext(Context):
    """Context wrapper indicating a rate-limit throttle event."""

    def __init__(self, context: Context):
        """Initialize from an existing context.

        Args:
            context: The original context that was rate-limited.
        """
        self.context = context
        self.id = f"RateLimit#{self.context.id}"
        self.freezing = False


class StopContext(Context):
    """Signal context that instructs workers to stop processing."""

    def __init__(self, name: str = "StopContext"):
        """Initialize a stop signal context."""
        super().__init__(None, name=name)


class OutputContext(Context):
    """Terminal context that holds the final output of DAG execution."""

    def __init__(self, context: Context | None = None, name: str = "OutputContext"):
        """Initialize an OutputContext, optionally copying data from another context.

        Args:
            context: Optional source context to copy ID and data from.
            name: Optional context name.
        """
        super().__init__(None, name=name)
        if context:
            self.id = context.id
            context.sync_white_board.copy(self)

    async def acopy(self, context: Context):
        """Async copy ID and data from another context.

        Args:
            context: The source context to copy from.
        """
        self.id = context.id
        await context.async_white_board.copy(self)


    def asdit(self) -> dict:
        """Return the raw data dictionary."""
        return self._data
