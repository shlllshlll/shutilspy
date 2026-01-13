#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: simplified_executor.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief: [WIP] Simplified single-level Executor for async DAG execution
"""

import time
import contextvars
import logging
from dataclasses import dataclass
import traceback
from typing import Any, AsyncGenerator
import asyncio
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from .runtime import Runtime
from .dag import DAG
from .task import (
    TaskBase,
    ForegroundTask,
    AsyncTask,
    SyncTask,
    ShutdownTask,
    AsyncShutdownTask,
    Environment,
)
from .executor import Executor, ExecutorConfig, _worker_context_var
from .context import Context, OutputContext, StopContext, LoopContext, RateLimitContext
from .task_state import ErrorInfo
from .task_queue import TaskPriorityQueue, TaskPriority, TaskItem
from .context_queue import ContextPriority


logger = logging.getLogger(__name__)


class TaskExecutor(Executor):
    """Single-level DAG executor with global task-based scheduling.

    Unlike the original two-level Executor, this implementation:
    - Uses a single global task queue instead of context queue
    - Controls concurrency with a single global semaphore
    - Executes one task at a time per worker
    - Provides priority-based scheduling at task level

    Key benefits:
    - Simpler architecture with single-level concurrency control
    - Better resource utilization through global scheduling
    - Easier configuration (just max_concurrent_tasks)
    """

    def __init__(
        self,
        dag: DAG,
        runtime: Runtime | None = None,
        config: ExecutorConfig = ExecutorConfig(),
    ):
        super().__init__(dag, runtime, config)

        # Task queue for global scheduling
        self._task_queue = TaskPriorityQueue()

        # Worker count defaults to max_concurrent_tasks
        self._num_workers = self._config.task_worker_num


    async def run(
        self,
        input_context: Context | list[Context] | None = None
    ) -> list[OutputContext]:
        """Execute the DAG with given input contexts.

        Args:
            input_context: Single context, list of contexts, or None for default

        Returns:
            List of output contexts
        """
        if input_context is None:
            input_context = [Context(self.runtime)]
        elif isinstance(input_context, Context):
            input_context = [input_context]
        elif isinstance(input_context, list):
            pass
        else:
            raise ValueError("context must be a Context or a list of Context")

        logger.info(f"[SimplifiedExecutor.run]: length: {len(input_context)}, input: {input_context}")

        # Initialize input contexts
        for context in input_context:
            await context.async_context.complete(self.dag.in_task)
            # Enqueue all available tasks with FIFO_HIGH priority
            await self._task_queue.async_put_context_tasks(
                context,
                TaskPriority.FIFO_HIGH
            )

        logger.info(f"[SimplifiedExecutor.run]: initial tasks enqueued")

        # Create environment
        env = Environment(self.runtime, self._process_pool, self.dag)

        # Start worker pool
        worker_tasks = [
            asyncio.create_task(self._worker_loop(idx, env))
            for idx in range(self._num_workers)
        ]

        # Wait for all workers to complete
        output = await asyncio.gather(*worker_tasks)

        # Collect all output contexts
        output_context = []
        for output_context_list in output:
            output_context.extend(output_context_list)

        # Shutdown tasks
        for task in self.dag.tasks.values():
            if isinstance(task, ShutdownTask):
                task.shutdown()
            elif isinstance(task, AsyncShutdownTask):
                await task.shutdown()

        logger.info(f"[SimplifiedExecutor.run]: execution complete, outputs: {len(output_context)}")
        return output_context

    @asynccontextmanager
    async def check_get_task(self, timeout: float | None = None, use_counter: bool = True) -> AsyncGenerator[TaskItem, None]:
        counter = self.runtime.counter
        if not use_counter or counter > 0:
            async with asyncio.timeout(timeout):
                task = await self._task_queue.async_get_task()
                yield task
        else:
            yield TaskItem(TaskPriority.FIFO_HIGH, 0, StopContext(), TaskBase(lambda ctx: None ))

    async def _worker_loop(
        self,
        worker_id: int,
        env: Environment
    ) -> list[OutputContext]:
        """Single worker that continuously fetches and executes tasks.

        Flow:
        1. Acquire global semaphore (limit total concurrent tasks)
        2. Get TaskItem from queue (with timeout)
        3. Execute the task
        4. Postprocess the context (GC, bypass)
        5. Check for newly available tasks in the same context
        6. If new tasks found, enqueue them with LIFO_HIGH priority
        7. Release semaphore
        8. Loop until stop condition
        """
        output_contexts: list[OutputContext] = []
        worker_storage = {}

        while True:
            try:
                # Get next task with timeout
                async with self.check_get_task(self._config.context_queue_timeout) as task_item:
                    in_context = task_item.context
                    task = task_item.task
                    logger.debug(f"[Worker{worker_id}]: get task[{task_item}] from context[{in_context}]")
                    if isinstance(in_context, StopContext):
                        logger.info(f"[Worker{worker_id}]: get StopContext, break")
                        break
                    if in_context.is_destory():
                        logger.error(f"[Worker{worker_id}]: Context {in_context} is destory, skip")
                        continue

                    avaliable_tasks = await in_context.async_task_state.avaliable_task()
                    if not avaliable_tasks:
                        logger.error(f"[Worker{worker_id}]: Context {in_context} do not have avaliable task, will destory")
                        await in_context.async_context.destory()
                        continue
            except asyncio.TimeoutError:
                logger.debug(f"[Worker{worker_id}]: Queue timeout, waiting...")
                continue


            # Set worker local storage
            token = _worker_context_var.set(worker_storage)
            try:
                # Execute single task
                logger.debug(f"[Worker{worker_id}]: Executing {task} for {in_context}")
                output_contexts_list = await self._run_task(
                    worker_id,
                    task,
                    in_context,
                    env
                )

                # Handle task output
                for output_context in output_contexts_list:
                    if isinstance(output_context, OutputContext):
                        output_contexts.append(output_context)
                    elif output_context == in_context:
                        # Same context: check for more tasks and put them back with LIFO_HIGH
                        # This ensures the same DAG path is prioritized
                        logger.debug(f"[Worker{worker_id}]: Context {in_context} continues, requeueing with LIFO_HIGH")
                        count = await self._task_queue.async_put_context_tasks(
                            in_context,
                            TaskPriority.LIFO_HIGH
                        )
                        if count == 0:
                            # No more tasks in this context, destroy it and stop tracking
                            logger.debug(f"[Worker{worker_id}]: Context {in_context} has no more tasks, destroying")
                            await in_context.async_context.destory()
                    elif isinstance(output_context, LoopContext):
                        # Loop context: use FIFO_LOW priority to avoid starving other contexts
                        logger.debug(f"[Worker{worker_id}]: LoopContext {output_context}, enqueuing with FIFO_LOW")
                        await self._task_queue.async_put_context_tasks(
                            output_context,
                            TaskPriority.FIFO_LOW
                        )
                    else:
                        # New context: enqueue its tasks with FIFO_HIGH priority
                        logger.debug(f"[Worker{worker_id}]: New context {output_context}, enqueuing with FIFO_HIGH")
                        await self._task_queue.async_put_context_tasks(
                            output_context,
                            TaskPriority.FIFO_HIGH
                        )

                # Postprocess context (GC, bypass)
                await self._context_postprocess(in_context, output_contexts_list, task)

            except Exception as e:
                logger.error(f"[Worker{worker_id}]: Error processing task {task}: {e}")
            finally:
                _worker_context_var.reset(token)

        return output_contexts

    async def _run_task(
        self, idx: int, task: TaskBase, in_context: Context, env: Environment
    ) -> list[Context]:
        if task in in_context.awake_time:
            if in_context.awake_time[task] > time.time():
                logger.debug(f"{in_context} cannot awake now")
                await self._task_queue.async_put_task(in_context, task)
                return []
            logger.debug(f"{in_context} can awake now")
            in_context.awake_time.pop(task)

        context_list = []
        try:
            logger.debug(f"[Worker{idx}]: {in_context} begin running {task}")
            if isinstance(task, ForegroundTask):
                if isinstance(task, SyncTask):
                    context_list = task(in_context, env)
                else:
                    raise ValueError(f"[Worker{idx}]: Unknown task type in forground mode: {type(task)}")
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
                    raise ValueError(f"[Worker{idx}]: Unknown task type: {type(task)}")
            logger.debug(f"[Worker{idx}]: {in_context} running {task} done")
        except Exception as e:
            if task.config.retry_times > 0:
                if await in_context.async_task_state.retry(task) <= task.config.retry_times:
                    if task.config.retry_interval != 0:
                        if callable(task.config.retry_interval):
                            interval = task.config.retry_interval(in_context)
                        else:
                            interval = task.config.retry_interval
                        in_context._awake_interval(interval, task)
                    await self._task_queue.async_put_task(in_context, task)
                    return []
            logger.error(f"[Worker{idx}]: {in_context} running {task} failed, error: {type(e).__name__}: {e}")
            traceback.print_exc()
            in_context.error_info = ErrorInfo(has_error=True, exception=e, error_node=task.id)
            await in_context.async_context.destory()

        for idx, out_context in enumerate(context_list):
            if isinstance(out_context, LoopContext) is False and isinstance(out_context, RateLimitContext) is False:
                await out_context.async_context.complete(task)
            if isinstance(out_context, RateLimitContext):
                context_list[idx] = out_context.context

        return context_list

    async def _context_postprocess(
        self,
        input_context: Context,
        output_contexts: list[Context],
        running_task: TaskBase
    ) -> None:
        """Postprocess context after task execution.

        Handles:
        1. Context GC (destroy contexts no longer referenced)
        2. Bypass logic (skip tasks in new contexts)

        Simplified compared to original because we now handle single
        task execution per call.

        Args:
            input_context: The context before task execution
            output_contexts: Contexts produced by the task
            running_task: The task that just completed
        """
        if not self._config.enable_context_gc and not self._config.enable_context_bypass:
            return

        # Build sets for comparison
        input_context_set = {input_context}
        output_context_set = {
            ctx for ctx in output_contexts
            if not ctx.is_destory()
        }

        # Context GC logic
        if self._config.enable_context_gc:
            await self._collect_referenced_contexts(output_context_set)

            # Destroy contexts in input but not in output
            for context in input_context_set:
                if context not in output_context_set and not context.is_destory():
                    logger.debug(f"[ContextGC]: {context} no longer referenced, destroying")
                    await context.async_context.destory()

        # Bypass logic (simplified)
        if self._config.enable_context_bypass:
            # For each new context (not in input), apply bypass
            new_contexts = output_context_set - input_context_set
            for new_context in new_contexts:
                if isinstance(new_context, LoopContext):
                    continue

                # Get tasks that were completed in the input path
                completed_in_input = input_context._completed_tasks
                if running_task in completed_in_input:
                    bypass_tasks = self.dag._get_bypass_tasks({running_task})
                else:
                    bypass_tasks = self.dag._get_bypass_tasks(completed_in_input)

                logger.debug(
                    f"[ContextBypass]: {new_context} skipping bypass tasks: {bypass_tasks}"
                )
                for bypass_task in bypass_tasks:
                    await new_context.async_context.complete(bypass_task)

    async def _collect_referenced_contexts(self, context_set: set[Context]) -> None:
        """Helper to collect all parent and child contexts into the set.

        Used for GC and bypass logic.

        Args:
            context_set: Set of contexts to collect from, will be modified in place
        """
        additional_contexts: list[Context] = []

        for context in context_set:
            # Skip OutputContext for GC/bypass - it's a terminal context
            if isinstance(context, OutputContext):
                continue

            # Get parent
            parent_result = await context.async_context.parent_context()
            if parent_result is not None:
                parent_context = parent_result.context
                if parent_context not in context_set and not parent_context.is_destory():
                    additional_contexts.append(parent_context)

            # Get children
            async for child_wrapper in context.async_context.iter_child_context():
                child_context = child_wrapper.context
                if child_context not in context_set and not child_context.is_destory():
                    additional_contexts.append(child_context)

        # Recursively collect newly found contexts
        if additional_contexts:
            for ctx in additional_contexts:
                context_set.add(ctx)
            await self._collect_referenced_contexts(context_set)
