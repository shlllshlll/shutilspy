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
from typing import Dict, Set, TYPE_CHECKING
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
        if task not in self._completed_tasks:
            self._completed_tasks.add(task)
            self._available_tasks.discard(task)

            for down_task in task.downstream_tasks:
                if all(up_task in self._completed_tasks for up_task in down_task.upstream_tasks):
                    self._available_tasks.add(down_task)

class SyncTaskState:
    def __init__(self, task_state: TaskStateMixin):
        self.__task_state = task_state

    def complete(self, task: "TaskBase"):
        with self.__task_state._task_lock.write():
            self.__task_state._complete(task)
    
    def next(self) -> "TaskBase | None":
        with self.__task_state._task_lock.read():
            if self.__task_state._available_tasks:
                return random.choice(list(self.__task_state._available_tasks))
            return None

class AsyncTaskState:
    def __init__(self, task_state: TaskStateMixin):
        self.__task_state = task_state

    async def complete(self, task: "TaskBase"):
        async with self.__task_state._task_alock.write():
            with self.__task_state._task_lock.write():
                self.__task_state._complete(task)
    
    async def next(self) -> "TaskBase | None":
        async with self.__task_state._task_alock.read():
            with self.__task_state._task_lock.read():
                if self.__task_state._available_tasks:
                    return random.choice(list(self.__task_state._available_tasks))
                return None

    async def retry(self, task: "TaskBase"):
        async with self.__task_state._retry_alock:
            if task in self.__task_state._retry_dict:
                self.__task_state._retry_dict[task] += 1
            else:
                self.__task_state._retry_dict[task] = 1
            return self.__task_state._retry_dict[task]
