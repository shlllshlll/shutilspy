#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: cache.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import time
import logging
import asyncio
from enum import Enum
from typing import Any, override
from .dag import DAG
from .runtime import Runtime
from .context import Context, OutputContext, StopContext, LoopContext, RateLimitContext
from .task import (
    TaskBase,
    ForgroundTask,
    AsyncTask,
    SyncTask,
    ShutdownTask,
    AShutdownTask,
    Environment,
)
from .executor import Executor, ExecutorConfig, _worker_context_var
from .context_queue import ContextPriority
from ..rwlock import AsyncRWLock


logger = logging.getLogger(__name__)


class ContextStatus(Enum):
    INIT = "INIT"
    RUNNING = "RUNNING"
    FINISH = "FINISH"

class ServeExecutor(Executor):
    @override
    def __init__(
        self,
        dag: DAG,
        runtime: Runtime | None = None,
        config: ExecutorConfig = ExecutorConfig(),
    ):
        super().__init__(dag, runtime, config)
        self.context_status_dict: dict[str, ContextStatus] = {}
        self.context_result_trackers: dict[str, asyncio.Future] = {}
        self.lock = AsyncRWLock()


    async def run(self) -> None:    # type: ignore[override]
        env = Environment(self.runtime, self._process_pool, self.dag)
        worker_tasks = [asyncio.create_task(self._worker_loop(idx, env)) for idx in range(self._config.context_worker_num)]
        await asyncio.gather(*worker_tasks)

        for task in self.dag.tasks.values():
            if isinstance(task, ShutdownTask):
                task.shutdown()
            elif isinstance(task, AShutdownTask):
                await task.shutdown()

    @override
    async def _worker_loop(self, idx: int, env: Environment) -> list[OutputContext]:
        worker_storage = {}
        while True:
            try:
                async with self.runtime.check_get_context(self._config.context_queue_timeout, False) as in_context:
                    logger.debug(f"[Worker{idx}]: get context[{in_context}] from async queue done")
                    if isinstance(in_context, StopContext):
                        logger.error(f"[Worker{idx}]: get unexpected StopContext, ServeExecutor will not stop, please check your code, skip")
                        continue
                    if in_context.is_destory():
                        logger.error(f"[Worker{idx}]: Context {in_context} is destory, skip")
                        continue

                    async with self.lock.write():
                        if in_context.id in self.context_status_dict:
                            self.context_status_dict[in_context.id] = ContextStatus.RUNNING

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
                        if out_context.id not in self.context_status_dict:
                            logger.error(f"[Worker{idx}]: OutputContext {out_context} id {out_context.id} not in context_status_dict, please check your code, skip")
                            continue

                        async with self.lock.write():
                            context_status= self.context_status_dict[out_context.id]
                            if context_status == ContextStatus.FINISH:
                                logger.error(f"[Worker{idx}]: OutputContext {out_context} id {out_context.id} already FINISH, please check your code and comfirm only one output for one input, skip")
                                continue
                            self.context_status_dict[out_context.id] = ContextStatus.FINISH
                            self.context_result_trackers[out_context.id].set_result(out_context.asdit())
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

        return []

    async def submit_task(self, context: Context) -> str:
        async with self.lock.write():
            if context.id in self.context_status_dict:
                raise ValueError(f"Context {context} already submitted.")
            self.context_status_dict[context.id] = ContextStatus.INIT
            future = asyncio.Future()
            self.context_result_trackers[context.id] = future

        await context.async_task_state.complete(self.dag.in_task)
        await self.runtime.async_queue.put(context, ContextPriority.FIFO_HIGH)
        return context.id

    async def get_task_status(self, task_id: str):
        async with self.lock.read():
            if task_id not in self.context_status_dict:
                raise ValueError(f"Task id {task_id} not found.")
            return self.context_status_dict[task_id]

    async def get_task_result(self, task_id: str) -> dict:
        async with self.lock.read():
            if task_id not in self.context_result_trackers:
                raise ValueError(f"Task id {task_id} not found.")
            future = self.context_result_trackers[task_id]

        result = await future
        async with self.lock.write():
            # clean up
            del self.context_status_dict[task_id]
            del self.context_result_trackers[task_id]

        return result