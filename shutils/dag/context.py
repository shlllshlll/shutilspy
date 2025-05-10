#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: context.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""
import time
import uuid
from typing import TYPE_CHECKING
from ..rwlock import RWLock, AsyncRWLock
from .task_state import TaskStateMixin, SyncTaskState, AsyncTaskState
from .data_white_board import (
    DataWhiteBoardMixin,
    SyncDataWhiteBoard,
    AsyncDataWhiteBoard,
)
from .global_data import debug_mode

if TYPE_CHECKING:
    from .task import TaskBase
    from .runtime import Runtime


class Context(DataWhiteBoardMixin, TaskStateMixin):
    """
    Context is a class that provides a context for the DAG.
    """

    def __init__(self, runtime: "Runtime | None", parent: "Context | None" = None, name: str = ""):
        DataWhiteBoardMixin.__init__(self)
        TaskStateMixin.__init__(self)
        self.id = name if name else str(uuid.uuid4())
        self.__complete_tasks: set["TaskBase"] = set()
        self.parent_rwlock = RWLock()
        self.parent_arwlock = AsyncRWLock()
        self._parent_context: Context | None = parent
        self._child_context_list: list["Context"] = []
        self.__child_context_num: int = 0
        self._runtime = runtime
        self.awake_time: dict["TaskBase", float] = {}
        if self._runtime:
            self._runtime.sync_counter.increase()
        if parent:
            if parent._parent_context is not None:
                raise ValueError("parent context must be a root context")
            with parent.parent_rwlock.write():
                parent.__child_context_num += 1
                parent._child_context_list.append(self)
        self._sync_context = None
        self._async_context = None

    def __repr__(self):
        if debug_mode:
            return f"{self.__class__.__name__}(data={DataWhiteBoardMixin.__repr__(self)}, state={TaskStateMixin.__repr__(self)}, parent={self._parent_context}, child_context_num={self.__child_context_num}, complete_tasks={self.__complete_tasks}, available_tasks={self.available_tasks})"
        else:
            return f"{self.__class__.__name__}(id={self.id})"


    @property
    def sync_context(self):
        if self._sync_context is None:
            self._sync_context = SyncContext(self)
        return self._sync_context

    @property
    def async_context(self):
        if self._async_context is None:
            self._async_context = AsyncContext(self)
        return self._async_context

    def _awake_interval(self, time_interval: float | int, task: "TaskBase") -> None:
        self.awake_time[task] = time.time() + time_interval


class SyncContext(SyncDataWhiteBoard, SyncTaskState):
    def __init__(self, context: Context):
        SyncDataWhiteBoard.__init__(self, context)
        SyncTaskState.__init__(self, context)
        self.__context = context

    def destory(self):
        if self.__context.is_destory():
            return

        self.__context.set_destory(True)
        if self.__context._runtime:
            self.__context._runtime.sync_counter.decrease()
        if self.__context._parent_context:
            with self.__context._parent_context.parent_rwlock.write():
                self.__context._parent_context.__child_context_num -= 1
        if self.__context._child_context_list:
            for child in self.__context._child_context_list:
                child.sync_context.destory()

    def create(self, copy_data: bool = False, deep_copy: bool = False, name: str = ""):
        new_context = Context(self.__context._runtime, name=name)
        if copy_data:
            self.copy(new_context, deep_copy)
        return new_context
    
    def child_context_num(self) -> int:
        """
        Get the number of child contexts.
        """
        with self.__context.parent_rwlock.read():
            return self.__context.__child_context_num
    
    def iter_child_context(self):
        with self.__context.parent_rwlock.read():
            for child in self.__context._child_context_list:
                yield child

    def parent_context(self) -> "Context | None":
        """
        Get the parent context.
        """
        with self.__context.parent_rwlock.read():
            return self.__context._parent_context

    def create_child(self, num: int = 0):
        """
        Create a child context.
        """
        if num:
            return [Context(self.__context._runtime, self.__context) for _ in range(num)]
        else:
            return Context(self.__context._runtime, self.__context)


class AsyncContext(AsyncDataWhiteBoard, AsyncTaskState):
    def __init__(self, context: Context):
        AsyncDataWhiteBoard.__init__(self, context)
        AsyncTaskState.__init__(self, context)
        self.__context = context

    async def destory(self):
        """
        Destory the context.
        """
        if self.__context.is_destory():
            return

        self.__context.set_destory(True)
        if self.__context._runtime:
            await self.__context._runtime.async_counter.decrease()
        if self.__context._parent_context:
            async with self.__context._parent_context.parent_arwlock.write():
                with self.__context._parent_context.parent_rwlock.write():
                    self.__context._parent_context.__child_context_num -= 1
        if self.__context._child_context_list:
            for child in self.__context._child_context_list:
                await child.async_context.destory()

    async def create(self, copy_data: bool = False, deep_copy: bool = False, name: str = ""):
        new_context = Context(self.__context._runtime, name=name)
        if copy_data:
            await self.copy(new_context, deep_copy)
        return new_context
    
    async def child_context_num(self) -> int:
        async with self.__context.parent_arwlock.read():
            with self.__context.parent_rwlock.read():
                return self.__context.__child_context_num

    async def iter_child_context(self):
        async with self.__context.parent_arwlock.read():
            with self.__context.parent_rwlock.read():
                for child in self.__context._child_context_list:
                    yield child
    
    async def partent_context(self):
        async with self.__context.parent_arwlock.read():
            with self.__context.parent_rwlock.read():
                return self.__context._parent_context
    
    async def create_child(self, num: int = 0):
        if self.__context._parent_context is not None:
            raise ValueError("parent context must be a root context")
        num = num if num else 1
        context = []
        for _ in range(num):
            sub_context = Context(self.__context._runtime)
            async with self.__context.parent_arwlock.write():
                with self.__context.parent_rwlock.write():
                    self.__context._child_context_list.append(sub_context)
                    self.__context.__child_context_num += 1
            context.append(sub_context)
        
        if num:
            return context
        else:
            return context[0]

class LoopContext(Context):
    def __init__(self, runtime: "Runtime | None", task: "TaskBase", name: str = "LoopContext"):
        super().__init__(runtime, name=name)
        self._add_available_task(task)

class RateLimitContext(Context):
    def __init__(self, context: Context):
        self.context = context

class StopContext(Context):
    def __init__(self, name: str = "StopContext"):
        super().__init__(None, name=name)


class OutputContext(Context):
    def __init__(self, context: Context | None = None, name: str = "OutputContext"):
        super().__init__(None, name=name)
        if context:
            context.sync_white_board.copy(self)

    async def acopy(self, context: Context):
        await context.async_white_board.copy(self)
