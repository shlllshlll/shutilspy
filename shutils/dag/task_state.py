"""Task state tracking for DAG contexts, including completion and retry logic."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..rwlock import AsyncRWLock, RWLock

if TYPE_CHECKING:
    from .task import TaskBase

__all__ = [
    "AsyncTaskState",
    "ErrorInfo",
    "SyncTaskState",
    "TaskStateMixin",
]


@dataclass
class ErrorInfo:
    """Information about an error that occurred during task execution.

    Attributes:
        has_error: Whether an error occurred.
        exception: The exception instance, if any.
        error_node: The ID of the task that caused the error.
    """
    has_error: bool = False
    exception: Exception | None = None
    error_node: str | None = None


class TaskStateMixin:
    """Mixin providing task completion tracking and available-task discovery."""

    def __init__(self):
        """Initialize task state with empty completion and availability sets."""
        self._completed_tasks: set[TaskBase] = set()
        self._available_tasks: set[TaskBase] = set()
        self._task_lock: RWLock = RWLock()
        self._task_alock: AsyncRWLock = AsyncRWLock()

        self._retry_dict: dict[TaskBase, int] = {}
        self._retry_alock: asyncio.Lock = asyncio.Lock()

        self._destory: bool = False

        self._error_info: ErrorInfo = ErrorInfo()
        self._skip_complete: bool = False

    def __repr__(self):
        return f"TaskState(destory={self._destory}, error_info={self._error_info})"

    def is_destory(self) -> bool:
        """Check if this context has been destroyed."""
        return self._destory

    def set_destory(self, value: bool):
        """Set the destroyed flag."""
        self._destory = value

    @property
    def available_tasks(self) -> set["TaskBase"]:
        """Set of tasks whose upstream dependencies are all completed."""
        return self._available_tasks

    def _add_available_task(self, task: "TaskBase"):
        """Mark a task as available for execution."""
        self._available_tasks.add(task)

    @property
    def error_info(self) -> ErrorInfo:
        return self._error_info

    @error_info.setter
    def error_info(self, value: ErrorInfo):
        self._error_info = value

    @property
    def sync_task_state(self) -> "SyncTaskState":
        """Lazy accessor for the sync task state wrapper."""
        return SyncTaskState(self)

    @property
    def async_task_state(self) -> "AsyncTaskState":
        """Lazy accessor for the async task state wrapper."""
        return AsyncTaskState(self)

    def _complete(self, task: "TaskBase"):
        if self._skip_complete:
            self._skip_complete = False
            return
        if task not in self._completed_tasks:
            self._available_tasks.discard(task)
            self._completed_tasks.add(task)

            for down_task in task.downstream_tasks:
                if down_task in self._completed_tasks:
                    continue
                if all(up_task in self._completed_tasks for up_task in down_task.upstream_tasks):
                    self._available_tasks.add(down_task)


class SyncTaskState:
    """Synchronous thread-safe wrapper for task state operations."""

    def __init__(self, task_state: TaskStateMixin):
        """Initialize with the underlying task state mixin.

        Args:
            task_state: The mixin instance to wrap.
        """
        self.__task_state = task_state

    def _complete(self, task: "TaskBase"):
        with self.__task_state._task_lock.write():
            self.__task_state._complete(task)

    def avaliable_task(self):
        """Return a list of currently available tasks under a read lock."""
        with self.__task_state._task_lock.read():
            return list(self.__task_state._available_tasks)



class AsyncTaskState:
    """Asynchronous wrapper for task state operations."""

    def __init__(self, task_state: TaskStateMixin):
        """Initialize with the underlying task state mixin.

        Args:
            task_state: The mixin instance to wrap.
        """
        self.__task_state = task_state

    async def read_wrapper[T](self, func: Callable[..., T], *args, **kwargs):
        """Execute a function under an async read lock."""
        async with self.__task_state._task_alock.read():
            return func(*args, **kwargs)

    async def write_wrapper[T](self, func: Callable[..., T], *args, **kwargs):
        """Execute a function under an async write lock."""
        async with self.__task_state._task_alock.write():
            return func(*args, **kwargs)

    def _complete(self, task: "TaskBase"):
        return self.write_wrapper(self.__task_state.sync_task_state._complete, task)

    def avaliable_task(self):
        """Return available tasks via async read wrapper."""
        return self.read_wrapper(self.__task_state.sync_task_state.avaliable_task)

    async def retry(self, task: "TaskBase"):
        """Increment and return the retry count for a task.

        Args:
            task: The task being retried.

        Returns:
            The new retry count.
        """
        async with self.__task_state._retry_alock:
            if task in self.__task_state._retry_dict:
                self.__task_state._retry_dict[task] += 1
            else:
                self.__task_state._retry_dict[task] = 1
            return self.__task_state._retry_dict[task]
