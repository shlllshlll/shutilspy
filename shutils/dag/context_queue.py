#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: context_queue.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

from enum import Enum
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import threading
import asyncio
from dataclasses import dataclass, field
import janus
from .context import Context

class ContextPriority(Enum):
    LIFO = 0
    FIFO_HIGH = 1
    FIFO_LOW = 2

@dataclass(order=True)
class PrioritizedItem:
    priority: int
    sequence: int
    item: Context = field(compare=False)


class ContextQueueMixin:
    def __init__(self):
        self._priority_queue: janus.PriorityQueue[PrioritizedItem] = janus.PriorityQueue()
        self._counter_dict: dict[ContextPriority, int] = {}
        self._sync_lock = threading.Lock()
        self._async_lock = asyncio.Lock()
        self._sync_queue = None
        self._async_queue = None
        for priority in ContextPriority:
            self._counter_dict[priority] = 0
    
    @property
    def sync_queue(self) -> "SyncContextQueue":
        if self._sync_queue is None:
            self._sync_queue = SyncContextQueue(self)
        return self._sync_queue
    
    @property
    def async_queue(self) -> "AsyncContextQueue":
        if self._async_queue is None:
            self._async_queue = AsyncContextQueue(self)
        return self._async_queue
    
class SyncContextQueue:
    def __init__(self, context_queue: ContextQueueMixin):
        self.__context_queue = context_queue
    
    def get(self) -> Context:
        return self.__context_queue._priority_queue.sync_q.get().item
    
    def put(self, context: Context, priority: ContextPriority = ContextPriority.FIFO_HIGH):
        with self.__context_queue._sync_lock:
            self.__context_queue._priority_queue.sync_q.put(PrioritizedItem(priority.value, self.__context_queue._counter_dict[priority], context))
            if priority == ContextPriority.LIFO:
                self.__context_queue._counter_dict[priority] -= 1
            else:
                self.__context_queue._counter_dict[priority] += 1

class AsyncContextQueue:
    def __init__(self, context_queue: ContextQueueMixin):
        self.__context_queue = context_queue
    
    async def get(self) -> Context:
        ret = await self.__context_queue._priority_queue.async_q.get()
        return ret.item
    
    @asynccontextmanager
    async def _get_with_context(self) -> AsyncGenerator[Context, None]:
        get_context = False
        try:
            yield await self.get()
            get_context = True
        finally:
            if get_context:
                self.__context_queue._priority_queue.async_q.task_done()
    
    async def put(self, context: Context, priority: ContextPriority = ContextPriority.FIFO_HIGH):
        async with self.__context_queue._async_lock:
            with self.__context_queue._sync_lock:
                item = PrioritizedItem(priority.value, self.__context_queue._counter_dict[priority], context)
                await self.__context_queue._priority_queue.async_q.put(item)
                if priority == ContextPriority.LIFO:
                    self.__context_queue._counter_dict[priority] -= 1
                else:
                    self.__context_queue._counter_dict[priority] += 1
    
    