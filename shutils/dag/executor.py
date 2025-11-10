#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: cache.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import time
from dataclasses import dataclass
import contextvars
import logging
from typing import Any
from typing import Coroutine, Iterable
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from .runtime import Runtime
from .dag import DAG
from .task import (
    TaskBase,
    ForgroundTask,
    AsyncTask,
    SyncTask,
    ShutdownTask,
    AShutdownTask,
    Environment,
)
from .context import Context, OutputContext, StopContext, LoopContext, RateLimitContext
from .task_state import ErrorInfo
from .context_queue import ContextPriority


logger = logging.getLogger(__name__)
_worker_context_var: contextvars.ContextVar[dict[Any, Any]] = contextvars.ContextVar("worker_context")

class WorkerLocalProxy:
    def __getattr__(self, name: str):
        try:
            # 获取当前上下文中的 worker 存储，然后从该存储中获取属性
            # Get the worker storage from the current context, then get the attribute from it
            return _worker_context_var.get()[name]
        except (LookupError, KeyError):
            # 如果 contextvar 未设置或属性不存在，则引发 AttributeError
            # This makes it behave like a normal object
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any):
        try:
            # 获取当前上下文中的 worker 存储，然后设置该存储的属性
            # Get the worker storage from the current context, then set an attribute on it
            _worker_context_var.get()[name] = value
        except LookupError:
            raise RuntimeError(
                "Cannot set attribute on worker_local outside of a running worker context."
            )

    def __delattr__(self, name: str):
        try:
            # 删除属性
            del _worker_context_var.get()[name]
        except (LookupError, KeyError):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __contains__(self, item: str) -> bool:
        try:
            return item in _worker_context_var.get()
        except LookupError:
            return False
worker_local = WorkerLocalProxy()

@dataclass
class ExecutorConfig:
    context_worker_num: int = 1
    task_worker_num: int = 1
    context_queue_timeout: float | None = 1
    thread_pool_worker_num: int | None = 0
    process_pool_worker_num: int | None = 0
    enable_context_gc: bool = True
    enbale_context_bypass: bool = True


class Executor:
    def __init__(
        self,
        dag: DAG,
        runtime: Runtime | None = None,
        config: ExecutorConfig = ExecutorConfig(),
    ):
        if runtime is None:
            self.runtime = Runtime()
        else:
            self.runtime = runtime
        self.dag = dag
        self._config = config
        if self._config.process_pool_worker_num != 0:
            self._process_pool = ProcessPoolExecutor(max_workers=self._config.process_pool_worker_num)
        else:
            self._process_pool = None
        if self._config.thread_pool_worker_num != 0:
            self.__thread_pool = ThreadPoolExecutor(max_workers=self._config.thread_pool_worker_num)
        else:
            self.__thread_pool = None

    async def run(self, input_context: Context | list[Context] | None = None) -> list[OutputContext]:
        if input_context is None:
            input_context = [Context(self.runtime)]
        elif isinstance(input_context, Context):
            input_context = [input_context]
        elif isinstance(input_context, list):
            pass
        else:
            raise ValueError("context must be a Context or a list of Context")
        logger.info(f"[Executor.run]: length: {len(input_context)}, input: {input_context}")

        for context in input_context:
            await context.async_task_state.complete(self.dag.in_task)
            await self.runtime.async_queue.put(context)
        logger.info(f"[Executor.run]: put input context to async queue done")

        env = Environment(self.runtime, self._process_pool, self.dag)
        worker_tasks = [asyncio.create_task(self.__worker_loop(idx, env)) for idx in range(self._config.context_worker_num)]
        output = await asyncio.gather(*worker_tasks)
        output_context = []
        for output_context_list in output:
            output_context.extend(output_context_list)

        for task in self.dag.tasks.values():
            if isinstance(task, ShutdownTask):
                task.shutdown()
            elif isinstance(task, AShutdownTask):
                await task.shutdown()
        return output_context

    async def _run_task(
        self, idx: int, sub_idx: int, task: TaskBase, in_context: Context, env: Environment
    ) -> list[Context]:
        if task in in_context.awake_time:
            if in_context.awake_time[task] > time.time():
                logger.debug(f"{in_context} cannot awake now")
                await self.runtime.async_queue.put(in_context)
                return []
            logger.debug(f"{in_context} can awake now")
            in_context.awake_time.pop(task)

        context_list = []
        try:
            logger.debug(f"[Worker{idx}-{sub_idx}]: {in_context} begin running {task}")
            if isinstance(task, ForgroundTask):
                if isinstance(task, SyncTask):
                    context_list = task(in_context, env)
                else:
                    raise ValueError(f"[Worker{idx}-{sub_idx}]: Unknown task type in forground mode: {type(task)}")
            else:
                if isinstance(task, AsyncTask):
                    context_list = await task(in_context, env)
                elif isinstance(task, SyncTask):
                    if self.__thread_pool:
                        loop = asyncio.get_running_loop()
                        context_list = await loop.run_in_executor(self.__thread_pool, task, in_context, env)
                    else:
                        context_list = await asyncio.to_thread(task, in_context, env)
                else:
                    raise ValueError(f"[Worker{idx}-{sub_idx}]: Unknown task type: {type(task)}")
            logger.debug(f"[Worker{idx}-{sub_idx}]: {in_context} running {task} done")
        except Exception as e:
            if task.config.retry_times > 0:
                if await in_context.async_task_state.retry(task) <= task.config.retry_times:
                    if task.config.retry_interval != 0:
                        if callable(task.config.retry_interval):
                            interval = task.config.retry_interval(in_context)
                        else:
                            interval = task.config.retry_interval
                        in_context._awake_interval(interval, task)
                    await self.runtime.async_queue.put(in_context)
                    return []
            logger.error(f"[Worker{idx}-{sub_idx}]: {in_context} running {task} failed, error: {type(e).__name__}: {e}")
            in_context.error_info = ErrorInfo(has_error=True, exception=e, error_node=task.id)
            await in_context.async_context.destory()

        for idx, out_context in enumerate(context_list):
            if isinstance(out_context, LoopContext) is False and isinstance(out_context, RateLimitContext) is False:
                await out_context.async_task_state.complete(task)
            if isinstance(out_context, RateLimitContext):
                context_list[idx] = out_context.context

        return context_list

    @staticmethod
    async def _async_limit(semaphore: asyncio.Semaphore, coro: Coroutine):
        async with semaphore:
            return await coro

    async def _worker_loop(self, idx: int, env: Environment) -> list[OutputContext]:
        output_context: list[OutputContext] = []
        worker_storage = {}
        while True:
            try:
                async with self.runtime.check_get_context(self._config.context_queue_timeout) as in_context:
                    logger.debug(f"[Worker{idx}]: get context[{in_context}] from async queue done")
                    if isinstance(in_context, StopContext):
                        logger.info(f"[Worker{idx}]: get StopContext, break")
                        break
                    if in_context.is_destory():
                        logger.error(f"[Worker{idx}]: Context {in_context} is destory, skip")
                        continue

                    avaliable_tasks = await in_context.async_task_state.avaliable_task()
                    if not avaliable_tasks:
                        logger.error(f"[Worker{idx}]: Context {in_context} do not have avaliable task, will destory")
                        await in_context.async_context.destory()
                        continue

                tasks = [
                    self._run_task(idx, sub_idx, task, in_context, env) for sub_idx, task in enumerate(avaliable_tasks)
                ]
                if self._config.task_worker_num > 0:
                    semaphore = asyncio.Semaphore(self._config.task_worker_num)
                    tasks = [self._async_limit(semaphore, task) for task in tasks]
                token = _worker_context_var.set(worker_storage)
                context_list_list = await asyncio.gather(*tasks)
                _worker_context_var.reset(token)
                context_list = [context for context_list in context_list_list for context in context_list]
                if len(context_list_list) > 1:
                    # need deduplicate
                    context_list = list(set(context_list))
                await self._context_postprocess(in_context, context_list, avaliable_tasks)
                for out_context in context_list:
                    if isinstance(out_context, OutputContext):
                        output_context.append(out_context)
                    else:
                        if out_context == in_context:
                            await self.runtime.async_queue.put(out_context, ContextPriority.LIFO)
                        elif isinstance(out_context, LoopContext):
                            await self.runtime.async_queue.put(out_context, ContextPriority.FIFO_LOW)
                        else:
                            await self.runtime.async_queue.put(out_context, ContextPriority.FIFO_HIGH)
            except asyncio.TimeoutError:
                logger.debug(f"[Worker{idx}]: context queue get timeout, skip")
                continue
        return output_context

    async def _context_postprocess(
        self, in_context: Iterable[Context] | Context, output_context: Iterable[Context], running_tasks: list[TaskBase]
    ):
        if not self._config.enable_context_gc and not self._config.enbale_context_bypass:
            return

        if isinstance(in_context, Context):
            in_context = [in_context]
        in_context_set = set([context for context in in_context if not context.is_destory()])
        out_context_set = set([context for context in output_context if not context.is_destory()])

        if self._config.enable_context_gc and in_context_set:
            # collect all output contexts
            for context in output_context:
                # collect parent contexts
                parent_context = await context.async_context.parent_context()
                if parent_context is not None and not parent_context.context.is_destory():
                    out_context_set.add(parent_context.context)
                # collect child contexts
                async for child_context in context.async_context.iter_child_context():
                    if not child_context.context.is_destory():
                        out_context_set.add(child_context.context)

            # contexts that are in input context but not in output context should be destoryed
            destory_context_set = in_context_set - out_context_set
            for context in destory_context_set:
                logger.debug(f"[ContextGC]: {context} is not in output context, destory")
                await context.async_context.destory()

        if self._config.enbale_context_bypass and out_context_set:
            # collect all input contexts
            for context in in_context:
                # collect parent contexts
                parent_context = await context.async_context.parent_context()
                if parent_context is not None and not parent_context.context.is_destory():
                    in_context_set.add(parent_context.context)
                # collect child contexts
                async for child_context in context.async_context.iter_child_context():
                    if not child_context.context.is_destory():
                        in_context_set.add(child_context.context)
            # contexts that are in output context but not in input context should mask it's bypass tasks
            new_context_set = out_context_set - in_context_set
            new_context_set = {item for item in new_context_set if not isinstance(item, LoopContext)}
            running_task_set = set(running_tasks)
            for context in new_context_set:
                current_running_tasks = context._completed_tasks & running_task_set
                bypass_tasks = self.dag._get_bypass_tasks(current_running_tasks)
                logger.debug(f"[ContextBypass]: {context} is not in input context, mask bypass tasks: {bypass_tasks}")
                for bypass_task in bypass_tasks:
                    await context.async_task_state.complete(bypass_task)
