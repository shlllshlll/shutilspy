#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: utils.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import asyncio
import threading
from collections import deque
from contextlib import asynccontextmanager
from typing import Callable, Coroutine, TypeVar, AsyncGenerator
import concurrent.futures
from .task import TaskBase
from .context import Context


class ResourcePool[T]:
    def __init__(
        self,
        default_size: int = 0,
        max_size: int = 0,
        create_func: Callable[[], T] | None = None,
        release_func: Callable[[T], None] | None = None,
        resources: list[T] | None = None,
    ):
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
        self._closed = True
        while not self._resource_queue.empty():
            resource = self._resource_queue.get_nowait()
            if self._release_func:
                self._release_func(resource)
        self._resource_queue.shutdown()


def get_loop_safe_runner(coro: Coroutine) -> asyncio.Future | concurrent.futures.Future:
    """根据当前线程决定如何运行协程"""
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
        raise RuntimeError("No running event loop - cannot run coroutine")


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
        if mask_self:
            if task not in task_set:
                task_set.add(task)
                task_queue.append(task)

    while task_queue:
        cur_task = task_queue.popleft()
        for up_or_downtask in getattr(cur_task, up_down):
            if up_or_downtask not in task_set:
                task_set.add(up_or_downtask)
                task_queue.append(up_or_downtask)
        yield cur_task


def mask_upstream_task_sync(context: Context, task_list: TaskBase | list[TaskBase], mask_self: bool = False):
    for task in __mask_common(task_list, mask_self, "upstream_tasks"):
        context.sync_context.complete(task)
    return context


async def mask_upstream_task_async(context: Context, task_list: TaskBase | list[TaskBase], mask_self: bool = False):
    for task in __mask_common(task_list, mask_self, "upstream_tasks"):
        await context.async_context.complete(task)
    return context


def mask_downstream_task_sync(context: Context, task_list: TaskBase | list[TaskBase], mask_self: bool = False):
    for task in __mask_common(task_list, mask_self, "downstream_tasks"):
        context.sync_context.complete(task)
    return context


async def mask_downstream_task_async(context: Context, task_list: TaskBase | list[TaskBase], mask_self: bool = False):
    for task in __mask_common(task_list, mask_self, "downstream_tasks"):
        await context.async_context.complete(task)
    return context
