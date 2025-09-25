#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: task.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import asyncio
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import threading
import logging
import uuid
from inspect import isgenerator, isasyncgen
from abc import ABC, abstractmethod
from typing import Any, Callable, Generator, Coroutine, AsyncGenerator, Iterable, Protocol, TYPE_CHECKING
import queue
from .context import Context, AsyncContext, SyncContext, StopContext, LoopContext, OutputContext, RateLimitContext
from .runtime import Runtime
from ..rate_limiter import RateLimiter

if TYPE_CHECKING:
    from .dag import DAG

logger = logging.getLogger(__name__)


class ShutdownCallableProtocol(Protocol):
    def __call__(self, context: SyncContext) -> list[SyncContext] | SyncContext | None: ...
    def shutdown(self) -> None: ...


class AsyncShutdownCallableProtocol(Protocol):
    async def __call__(self, context: AsyncContext) -> list[AsyncContext] | AsyncContext | None: ...
    async def shutdown(self) -> None: ...


@dataclass
class Environment:
    runtime: Runtime
    process_pool: ProcessPoolExecutor | None
    dag: "DAG"


@dataclass
class TaskConfig:
    retry_times: int = 0
    retry_interval: int | float | Callable[[Context], float | int] = 0
    parallel_num: int = 0
    calls: int = 0
    period: int = 1


class TaskBase(ABC):
    def __init__(self, func: Callable | None, config: TaskConfig = TaskConfig(), name: str = ""):
        self.id = str(uuid.uuid4()) if not name else name
        self.upstream_tasks: set[TaskBase] = set()
        self.downstream_tasks: set[TaskBase] = set()
        self.config = config
        self.running_task_num = 0
        if self.config.calls > 0 and self.config.period > 0:
            self.rate_limiter = RateLimiter(self.config.calls, self.config.period)
        else:
            self.rate_limiter = None

    def add_upstream(self, task: "TaskBase"):
        self.upstream_tasks.add(task)
        task.downstream_tasks.add(self)

    def call_before(self, context: Context) -> Context | None:
        # parallel control
        if self.config.parallel_num > 0 and self.running_task_num > self.config.parallel_num:
            return context
        self.running_task_num += 1

        # qps control
        if self.rate_limiter is not None and not self.rate_limiter.allow():
            return RateLimitContext(context)

        return None

    def call_after(self, context_list: list[Context]) -> None:
        self.running_task_num -= 1

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other: "TaskBase"):
        return self.id == other.id

    def __repr__(self):
        return f"id={self.id}, config={self.config}"


class ForgroundTask(ABC):
    pass


class LongrunTask(ABC):
    pass


class ShutdownTask(ABC):
    @abstractmethod
    def shutdown(self):
        pass


class AShutdownTask(ABC):
    @abstractmethod
    async def shutdown(self):
        pass


class SyncTask(TaskBase):
    @abstractmethod
    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        pass

    def __call__(self, context: Context, env: Environment) -> list[Context]:
        ret = self.call_before(context)
        if ret:
            return [ret]
        sync_ret = self.call(context.sync_context, env)
        ret = [ctx.context for ctx in sync_ret]
        self.call_after(ret)
        return ret


class AsyncTask(TaskBase):
    @abstractmethod
    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        pass

    async def __call__(self, context: Context, env: Environment) -> list[Context]:
        ret = self.call_before(context)
        if ret:
            return [ret]
        async_ret = await self.call(context.async_context, env)
        ret = [ctx.context for ctx in async_ret]
        self.call_after(ret)
        return ret


class SyncProcessTask(AsyncTask):
    def __init__(
        self,
        func: Callable[[SyncContext], list[SyncContext] | SyncContext],
        config: TaskConfig = TaskConfig(),
        name: str = "",
    ):
        super().__init__(func, config, name)
        self._func = func

    @staticmethod
    def _func_wrapper(func: Callable[[SyncContext], list[SyncContext] | SyncContext], data: dict):
        input_context = Context(None)
        input_context._data = data
        ret = func(input_context.sync_context)
        if isinstance(ret, SyncContext):
            return ret.context._data

        output_data_list = []
        for sync_ctx in ret:
            output_data_list.append(sync_ctx.context._data)
        return output_data_list

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        if env.process_pool is None:
            raise RuntimeError("process pool is not set")
        loop = asyncio.get_running_loop()
        async with async_ctx.wlock():
            ret = await loop.run_in_executor(env.process_pool, self._func_wrapper, self._func, async_ctx.context._data)

        if isinstance(ret, dict):
            async with async_ctx.wlock():
                async_ctx.context._data = ret
            return [async_ctx]

        output_context_list = []
        for data in ret:
            output_context = await async_ctx.create()
            output_context.context._data = data
            output_context_list.append(output_context)
        await async_ctx.destory()
        return output_context_list


class SyncGeneratorTask(SyncTask, ShutdownTask):
    def __init__(
        self,
        func: Callable[[], Generator[SyncContext | list[SyncContext] | None, SyncContext, None]],
        config: TaskConfig = TaskConfig(),
        name: str = "",
    ):
        super().__init__(func, config, name)
        self._generator = func()
        if not isgenerator(self._generator):
            raise ValueError("func must be a generator function")
        next(self._generator)

    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        try:
            ret = self._generator.send(sync_ctx)
            if ret is None:
                return []
            if isinstance(ret, SyncContext):
                return [ret]
            return ret
        except StopIteration:
            return []

    def __repr__(self):
        return f"{self.__class__.__name__}(func={self._generator}, {TaskBase.__repr__(self)})"

    def shutdown(self):
        try:
            self._generator.close()
        except GeneratorExit:
            pass


class SyncImmediateTask(SyncTask):
    def __init__(
        self,
        func: Callable[[SyncContext], list[SyncContext] | SyncContext | None],
        config: TaskConfig = TaskConfig(),
        name: str = "",
    ):
        super().__init__(func, config, name)
        self._func = func

    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        ret = self._func(sync_ctx)
        if isinstance(ret, SyncContext):
            return [ret]
        elif ret is None:
            return []
        return ret

    def __repr__(self):
        return f"SyncImmediateTask(func={self._func}, {TaskBase.__repr__(self)})"


class SyncImmediateShutdownTask(SyncImmediateTask, ShutdownTask):
    def __init__(self, shutdown_callable: ShutdownCallableProtocol, config: TaskConfig = TaskConfig(), name: str = ""):
        super().__init__(shutdown_callable, config, name)

    def shutdown(self):
        self._func.shutdown()


class SyncLoopTask(SyncGeneratorTask):
    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        need_create_loop_context = isinstance(sync_ctx.context, LoopContext) == False
        ret = super().call(sync_ctx, env)
        if ret:
            if need_create_loop_context:
                ret.append(LoopContext(sync_ctx.context._runtime, self).sync_context)
            else:
                ret.append(sync_ctx)
        elif need_create_loop_context is False:
            sync_ctx.destory()

        return ret


class SyncLongrunTask(SyncTask, LongrunTask, ShutdownTask):
    def __init__(
        self,
        func: Callable[[queue.Queue[tuple[Context, queue.Queue[list[Context] | Context | None]]]], None],
        config: TaskConfig = TaskConfig(),
        name: str = "",
    ):
        super().__init__(func, config, name)
        self.__input_queue = queue.Queue()
        self._func = func
        self.__thread = None

    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        if self.__thread is None:
            self.__thread = threading.Thread(target=self._func, args=(self.__input_queue,))
            self.__thread.start()
        if not self.__thread.is_alive():
            logger.error(f"[SyncLongrunTask]: thread exit unexpectedly")
            raise RuntimeError("thread is not alive")
        future = queue.Queue()
        self.__input_queue.put((sync_ctx, future))
        result = future.get()
        if isinstance(result, SyncContext):
            return [sync_ctx]
        elif isinstance(result, list):
            return result
        return []

    def __repr__(self):
        return f"{self.__class__.__name__}(func={self._func}, {TaskBase.__repr__(self)})"

    def shutdown(self):
        if self.__thread and self.__thread.is_alive():
            self.__input_queue.put((StopContext().sync_context, queue.Queue()))
            self.__thread.join()


class AsyncLongrunTask(AsyncTask, LongrunTask, AShutdownTask):
    def __init__(
        self,
        func: Callable[[asyncio.Queue[tuple[AsyncContext, asyncio.Future]]], Coroutine[Any, Any, None]],
        config: TaskConfig = TaskConfig(),
        name: str = "",
    ):
        super().__init__(func, config, name)
        self.__input_queue = asyncio.Queue()
        self.__task = None
        self._func = func

    def _task_down_callback(self, task: asyncio.Task):
        if self.__future.done():
            return

        try:
            result = task.result()
            self.__future.set_result(result)
        except Exception as e:
            logger.error(f"[AsyncLongrunTask]: task exit with exception: {e}")
            self.__future.set_exception(e)

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        if self.__task is None:
            self.__task = asyncio.create_task(self._func(self.__input_queue))
            self.__task.add_done_callback(self._task_down_callback)
        elif self.__task.done():
            logger.error(f"[AsyncLongrunTask]: task exit unexpectedly")
            raise RuntimeError("task is not alive")
        self.__future = asyncio.Future()
        await self.__input_queue.put((async_ctx, self.__future))
        result = await self.__future

        if isinstance(result, AsyncContext):
            return [result]
        elif isinstance(result, list):
            return result
        return []

    def __repr__(self):
        return f"AsyncLongrunTask(func={self._func}, {TaskBase.__repr__(self)})"

    async def shutdown(self):
        if self.__task and not self.__task.done():
            await self.__input_queue.put((StopContext().async_context, asyncio.Future()))
            await self.__task
            self.__task = None


class AsyncGeneratorTask(AsyncTask, AShutdownTask):
    def __init__(
        self,
        func: Callable[[], AsyncGenerator[AsyncContext | list[AsyncContext] | None, AsyncContext]],
        config: TaskConfig = TaskConfig(),
        name: str = "",
    ):
        super().__init__(func, config, name)
        self.__generator = func()
        if not isasyncgen(self.__generator):
            raise ValueError("func must be a async generator function")
        self.__activate_generator = False

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        if not self.__activate_generator:
            self.__activate_generator = True
            await anext(self.__generator)
        try:
            ret = await self.__generator.asend(async_ctx)
            if ret is None:
                return []
            if isinstance(ret, AsyncContext):
                return [ret]
            return ret
        except StopAsyncIteration:
            return []

    def __repr__(self):
        return f"{self.__class__.__name__}(func={self.__generator}, {TaskBase.__repr__(self)})"

    async def shutdown(self):
        try:
            await self.__generator.aclose()
        except GeneratorExit:
            pass


class AsyncLoopTask(AsyncGeneratorTask):
    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        need_create_loop_context = isinstance(async_ctx.context, LoopContext) == False
        ret = await super().call(async_ctx, env)
        if ret:
            if need_create_loop_context:
                ret.append(LoopContext(async_ctx.context._runtime, self).async_context)
            else:
                ret.append(async_ctx)
        elif need_create_loop_context is False:
            await async_ctx.destory()
        return ret


class AsyncImmediateTask(AsyncTask):
    def __init__(
        self,
        func: Callable[[AsyncContext], Coroutine[Any, Any, list[AsyncContext] | AsyncContext | None]],
        config: TaskConfig = TaskConfig(),
        name: str = "",
    ):
        super().__init__(func, config, name)
        self._func = func

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        ret = await self._func(async_ctx)
        if isinstance(ret, AsyncContext):
            return [ret]
        elif ret is None:
            return []
        return ret

    def __repr__(self):
        return f"AsyncImmediateTask(func={self._func}, {TaskBase.__repr__(self)})"


class AsyncRouteTask(AsyncTask):
    def __init__(
        self,
        func: Callable[[AsyncContext], Coroutine[Any, Any, str | TaskBase | list[str | TaskBase]]],
        config: TaskConfig = TaskConfig(),
        name: str = "",
    ):
        super().__init__(None, config, name)
        self._func = func
        self._mask_task_cache: dict[tuple[TaskBase, ...], set[TaskBase]] = {}

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        route_tasks = await self._func(async_ctx)
        if not isinstance(route_tasks, list):
            route_tasks = [route_tasks]

        real_route_tasks: list[TaskBase] = []
        for task_name in route_tasks:
            if type(task_name) is str:
                task = env.dag.tasks.get(task_name, None)
                if not task:
                    logger.error(f"task {task} not found in dag tasks")
                    await async_ctx.destory()
                    return []
            else:
                task = task_name
            if task not in self.downstream_tasks:
                logger.error(f"task {task} not found in downstream tasks")
                await async_ctx.destory()
                return []

            real_route_tasks.append(task)
        route_task_set = set(real_route_tasks)
        route_task_tuple = tuple(real_route_tasks)
        if route_task_tuple in self._mask_task_cache:
            mask_task_set = self._mask_task_cache[route_task_tuple]
        else:
            route_downstream_tasks = env.dag._get_all_downstream_tasks(route_task_set, True)
            noroute_downstream_tasks = env.dag._get_all_downstream_tasks(self.downstream_tasks - route_task_set, True)
            mask_task_set = noroute_downstream_tasks - route_downstream_tasks

        for task in mask_task_set:
            await async_ctx.complete(task)

        return [async_ctx]


class AsyncImmediateShutdownTask(AsyncImmediateTask, AShutdownTask):
    def __init__(
        self, shutdown_callable: AsyncShutdownCallableProtocol, config: TaskConfig = TaskConfig(), name: str = ""
    ):
        super().__init__(shutdown_callable, config, name)

    async def shutdown(self):
        await self._func.shutdown()


class ForSyncGeneratorTask(SyncGeneratorTask, ForgroundTask):
    pass


class ForSyncImmediateTask(SyncImmediateTask, ForgroundTask):
    pass


class ForSyncLoopTask(SyncLoopTask, ForgroundTask):
    pass


class InTask(AsyncTask):
    def __init__(self, name: str = "#InTask"):
        super().__init__(None, name=name)

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        return [async_ctx]

    async def __call__(self, context: Context, env: Environment) -> list[Context]:
        async_ret = await self.call(context.async_context, env)
        return [ctx.context for ctx in async_ret]


class OutTask(AsyncTask):
    def __init__(self, name: str = "#OutTask"):
        super().__init__(None, name=name)

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        if isinstance(async_ctx.context, OutputContext):
            return [async_ctx]
        else:
            output_context = OutputContext()
            await output_context.acopy(async_ctx.context)
            await async_ctx.destory()
            return [output_context.async_context]

    async def __call__(self, context: Context, env: Environment) -> list[Context]:
        asnc_ret = await self.call(context.async_context, env)
        return [ctx.context for ctx in asnc_ret]
