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
import logging
from typing import Coroutine, Iterable
import asyncio
import traceback
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


@dataclass
class ExecutorConfig:
    worker_num: int = 1
    sub_worker_num: int = 1
    context_queue_timeout: float | None = 1
    thread_pool_worker_num: int | None = 0
    process_pool_worker_num: int | None = 0


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
        self.__config = config
        if self.__config.process_pool_worker_num != 0:
            self.__process_pool = ProcessPoolExecutor(max_workers=self.__config.process_pool_worker_num)
        else:
            self.__process_pool = None
        if self.__config.thread_pool_worker_num != 0:
            self.__thread_pool = ThreadPoolExecutor(max_workers=self.__config.thread_pool_worker_num)
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

        env = Environment(self.runtime, self.__process_pool)
        worker_tasks = [asyncio.create_task(self.__worker_loop(idx, env)) for idx in range(self.__config.worker_num)]
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

    async def __run_task(self, idx: int, sub_idx: int, task: TaskBase, in_context: Context, env: Environment) -> list[Context]:
        if task in in_context.awake_time:
            if in_context.awake_time[task] > time.time():
                logger.info(f"{in_context} cannot awake now")
                await self.runtime.async_queue.put(in_context)
                return []
            logger.info(f"{in_context} can awake now")
            in_context.awake_time.pop(task)

        context_list = []
        try:
            logger.info(f"[Worker{idx}-{sub_idx}]: {in_context} begin running {task}")
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
            logger.info(f"[Worker{idx}-{sub_idx}]: {in_context} running {task} done")
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
            traceback.print_exc()
            logger.error(f"[Worker{idx}-{sub_idx}]: {in_context} running {task} failed, error: {e}")
            in_context.error_info = ErrorInfo(has_error=True, exception=e, error_node=task.id)
            await in_context.async_context.destory()

        for idx, out_context in enumerate(context_list):
            if isinstance(out_context, LoopContext) is False and isinstance(out_context, RateLimitContext) is False:
                await out_context.async_task_state.complete(task)
            if isinstance(out_context, RateLimitContext):
                context_list[idx] = out_context.context
        return context_list

    @staticmethod
    async def __async_limit(semaphore: asyncio.Semaphore, coro: Coroutine):
        async with semaphore:
            return await coro

    async def __worker_loop(self, idx: int, env: Environment) -> list[OutputContext]:
        output_context: list[OutputContext] = []
        while True:
            try:
                async with self.runtime.check_get_context(self.__config.context_queue_timeout) as in_context:
                    logger.info(f"[Worker{idx}]: get context[{in_context}] from async queue done")
                    if isinstance(in_context, StopContext):
                        logger.info(f"[Worker{idx}]: get StopContext, break")
                        break
                    if in_context.is_destory():
                        logger.error(f"[Worker{idx}]: Context {in_context} is destory, skip")
                        continue

                    avaliable_tasks = await in_context.async_task_state.avaliable_task()
                    if not avaliable_tasks:
                        logger.error(f"[Worker{idx}]: no avaliable task")
                        await in_context.async_context.destory()
                        continue

                tasks = [self.__run_task(idx, sub_idx, task, in_context, env) for sub_idx, task in enumerate(avaliable_tasks)]
                if self.__config.sub_worker_num > 0:
                    semaphore = asyncio.Semaphore(self.__config.sub_worker_num)
                    tasks = [self.__async_limit(semaphore, task) for task in tasks]
                context_list_list = await asyncio.gather(*tasks)
                context_list = [context for context_list in context_list_list for context in context_list]
                if len(context_list_list) > 1:
                    # need deduplicate
                    context_list = list(set(context_list))
                await self.__context_gc(in_context, context_list)
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
                logger.info(f"[Worker{idx}]: context queue get timeout, skip")
                continue
        return output_context
    
    async def __context_gc(self, in_context: Iterable[Context] | Context, output_context: Iterable[Context]):
        if isinstance(in_context, Context):
            in_context = [in_context]
        in_context_set = set(in_context)
        out_context_set = set(output_context)
        inner_context_set = in_context_set & out_context_set
        in_context_set = in_context_set - inner_context_set

        for context in output_context:
            parent_context = await context.async_context.parent_context()
            if parent_context is not None:
                out_context_set.add(parent_context)
            async for child_context in context.async_context.iter_child_context():
                out_context_set.add(child_context)
            
        in_context_set = in_context_set - out_context_set
        for context in in_context_set:
            logger.info(f"[ContextGC]: {context} is not in output context, destory")
            await context.async_context.destory()
            

