#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: task_state.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""
import random
import asyncio
from dataclasses import dataclass
from typing import Dict, Generator, Set, TYPE_CHECKING, Callable, TypeVar
from ..rwlock import RWLock, AsyncRWLock

if TYPE_CHECKING:
    from .task import TaskBase


@dataclass
class ErrorInfo:
    has_error: bool = False
    exception: Exception | None = None
    error_node: str | None = None


class TaskStateMixin:

    def __init__(self):
        self._completed_tasks: Set["TaskBase"] = set()
        self._available_tasks: Set["TaskBase"] = set()
        self._task_lock: RWLock = RWLock()
        self._task_alock: AsyncRWLock = AsyncRWLock()

        self._retry_dict: Dict["TaskBase", int] = {}
        self._retry_alock: asyncio.Lock = asyncio.Lock()

        self._destory: bool = False

        self._error_info: ErrorInfo = ErrorInfo()
        self._skip_complete: bool = False

    def __repr__(self):
        return f"TaskState(destory={self._destory}, error_info={self._error_info})"

    def is_destory(self) -> bool:
        return self._destory

    def set_destory(self, value: bool):
        self._destory = value

    @property
    def available_tasks(self) -> Set["TaskBase"]:
        return self._available_tasks

    def _add_available_task(self, task: "TaskBase"):
        self._available_tasks.add(task)

    @property
    def error_info(self) -> ErrorInfo:
        return self._error_info

    @error_info.setter
    def error_info(self, value: ErrorInfo):
        self._error_info = value

    @property
    def sync_task_state(self) -> "SyncTaskState":
        return SyncTaskState(self)

    @property
    def async_task_state(self) -> "AsyncTaskState":
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
    def __init__(self, task_state: TaskStateMixin):
        self.__task_state = task_state

    def complete(self, task: "TaskBase"):
        with self.__task_state._task_lock.write():
            self.__task_state._complete(task)

    def avaliable_task(self):
        with self.__task_state._task_lock.read():
            return list(self.__task_state._available_tasks)



class AsyncTaskState:
    def __init__(self, task_state: TaskStateMixin):
        self.__task_state = task_state

    async def read_wrapper[T](self, func: Callable[..., T], *args, **kwargs):
        async with self.__task_state._task_alock.read():
            return func(*args, **kwargs)

    async def write_wrapper[T](self, func: Callable[..., T], *args, **kwargs):
        async with self.__task_state._task_alock.write():
            return func(*args, **kwargs)

    def complete(self, task: "TaskBase"):
        return self.write_wrapper(self.__task_state.sync_task_state.complete, task)

    def avaliable_task(self):
        return self.read_wrapper(self.__task_state.sync_task_state.avaliable_task)

    async def retry(self, task: "TaskBase"):
        async with self.__task_state._retry_alock:
            if task in self.__task_state._retry_dict:
                self.__task_state._retry_dict[task] += 1
            else:
                self.__task_state._retry_dict[task] = 1
            return self.__task_state._retry_dict[task]
