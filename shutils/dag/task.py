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
import uuid
from inspect import isgenerator, isasyncgen
from abc import ABC, abstractmethod
from typing import Any, Callable, Generator, Coroutine, AsyncGenerator
import queue
from .context import Context, StopContext, LoopContext, OutputContext
from .runtime import Runtime
from .context_queue import SyncContextQueue, AsyncContextQueue
from ..rate_limiter import RateLimiter


@dataclass
class Environment:
    runtime: Runtime
    process_pool: ProcessPoolExecutor | None


@dataclass
class TaskConfig:
    retry_times: int = 0
    retry_interval: int | float | Callable[[Context], float | int] = 0
    parallel_num: int = 0
    calls: int = 0
    period: int = 1


class TaskBase(ABC):
    def __init__(self, config: TaskConfig = TaskConfig()):
        self.id = str(uuid.uuid4())
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
            return context

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
    def call(self, context: Context, env: Environment) -> list[Context]:
        pass

    def __call__(self, context: Context, env: Environment) -> list[Context]:
        ret = self.call_before(context)
        if ret:
            return [ret]
        ret = self.call(context, env)
        self.call_after(ret)
        return ret


class AsyncTask(TaskBase):
    @abstractmethod
    async def call(self, context: Context, env: Environment) -> list[Context]:
        pass

    async def __call__(self, context: Context, env: Environment) -> list[Context]:
        ret = self.call_before(context)
        if ret:
            return [ret]
        ret = await self.call(context, env)
        self.call_after(ret)
        return ret


class SyncProcessTask(AsyncTask):
    def __init__(
        self,
        func: Callable[[Context], list[Context] | Context],
        config: TaskConfig = TaskConfig(),
    ):
        super().__init__(config)
        self._func = func

    @staticmethod
    def _func_wrapper(func: Callable[[Context], list[Context] | Context], data: dict):
        input_context = Context(None)
        input_context._data = data
        ret = func(input_context)
        if isinstance(ret, Context):
            return ret._data

        output_data_list = []
        for context in ret:
            output_data_list.append(context._data)
        return output_data_list

    async def call(self, context: Context, env: Environment) -> list[Context]:
        if env.process_pool is None:
            raise RuntimeError("process pool is not set")
        loop = asyncio.get_running_loop()
        async with context.async_context.wlock():
            ret = await loop.run_in_executor(env.process_pool, self._func_wrapper, self._func, context._data)

        if isinstance(ret, dict):
            async with context.async_context.wlock():
                context._data = ret
            return [context]

        output_context_list = []
        for data in ret:
            output_context = await context.async_context.create()
            output_context._data = data
            output_context_list.append(output_context)
        await context.async_context.destory()
        return output_context_list


class SyncGeneratorTask(SyncTask, ShutdownTask):
    def __init__(
        self,
        func: Callable[[], Generator[Context | list[Context] | None, Context, None]],
        config: TaskConfig = TaskConfig(),
    ):
        super().__init__(config)
        self._generator = func()
        if not isgenerator(self._generator):
            raise ValueError("func must be a generator function")
        next(self._generator)

    def call(self, context: Context, env: Environment) -> list[Context]:
        try:
            ret = self._generator.send(context)
            if ret is None:
                return []
            if isinstance(ret, Context):
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
        func: Callable[[Context], list[Context] | Context],
        config: TaskConfig = TaskConfig(),
    ):
        super().__init__(config)
        self._func = func

    def call(self, context: Context, env: Environment) -> list[Context]:
        ret: list[Context] | Context = self._func(context)
        if isinstance(ret, Context):
            return [ret]
        return ret

    def __repr__(self):
        return f"SyncImmediateTask(func={self._func}, {TaskBase.__repr__(self)})"


class SyncLoopTask(SyncGeneratorTask):
    def call(self, context: Context, env: Environment) -> list[Context]:
        need_create_loop_context = isinstance(context, LoopContext) == False
        ret = super().call(context, env)
        if ret:
            if need_create_loop_context:
                ret.append(LoopContext(context._runtime, self))
            else:
                ret.append(context)
        elif need_create_loop_context is False:
            context.sync_context.destory()

        return ret


class BackSyncLongrunTask(SyncTask, LongrunTask, ShutdownTask):
    def __init__(
        self,
        func: Callable[[queue.Queue[Context], SyncContextQueue], None],
        config: TaskConfig = TaskConfig(),
    ):
        super().__init__(config)
        self.__input_queue = queue.Queue()
        self._func = func
        self.__thread = None

    def call(self, context: Context, env: Environment) -> list[Context]:
        if self.__thread is None:
            self.__thread = threading.Thread(target=self._func, args=(self.__input_queue, env.runtime.sync_queue))
            self.__thread.start()
        if not self.__thread.is_alive():
            raise RuntimeError("thread is not alive")
        self.__input_queue.put(context)
        return []

    def __repr__(self):
        return f"{self.__class__.__name__}(func={self._func}, {TaskBase.__repr__(self)})"

    def shutdown(self):
        if self.__thread and self.__thread.is_alive():
            self.__input_queue.put(StopContext())
            self.__thread.join()


class AsyncLongrunTask(AsyncTask, LongrunTask, AShutdownTask):
    def __init__(
        self,
        func: Callable[[asyncio.Queue[Context], AsyncContextQueue], Coroutine[Any, Any, None]],
        config: TaskConfig = TaskConfig(),
    ):
        super().__init__(config)
        self.__input_queue = asyncio.Queue()
        self.__task = None
        self._func = func

    async def call(self, context: Context, env: Environment) -> list[Context]:
        if self.__task is None:
            self.__task = asyncio.create_task(self._func(self.__input_queue, env.runtime.async_queue))
        await self.__input_queue.put(context)
        return []

    def __repr__(self):
        return f"AsyncLongrunTask(func={self._func}, {TaskBase.__repr__(self)})"

    async def shutdown(self):
        if self.__task:
            await self.__input_queue.put(StopContext())
            await self.__task
            self.__task = None


class AsyncGeneratorTask(AsyncTask, AShutdownTask):
    def __init__(
        self,
        func: Callable[[], AsyncGenerator[Context | list[Context] | None, Context]],
        config: TaskConfig = TaskConfig(),
    ):
        super().__init__(config)
        self.__generator = func()
        if not isasyncgen(self.__generator):
            raise ValueError("func must be a async generator function")
        self.__activate_generator = False

    async def call(self, context: Context, env: Environment) -> list[Context]:
        if not self.__activate_generator:
            self.__activate_generator = True
            await anext(self.__generator)
        try:
            ret = await self.__generator.asend(context)
            if ret is None:
                return []
            if isinstance(ret, Context):
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
    async def call(self, context: Context, env: Environment) -> list[Context]:
        need_create_loop_context = isinstance(context, LoopContext) == False
        ret = await super().call(context, env)
        if ret:
            if need_create_loop_context:
                ret.append(LoopContext(context._runtime, self))
            else:
                ret.append(context)
        elif need_create_loop_context is False:
            await context.async_context.destory()
        return ret


class AsyncImmediateTask(AsyncTask):
    def __init__(
        self,
        func: Callable[[Context], Coroutine[Any, Any, list[Context] | Context]],
        config: TaskConfig = TaskConfig(),
    ):
        super().__init__(config)
        self._func = func

    async def call(self, context: Context, env: Environment) -> list[Context]:
        ret = await self._func(context)
        if isinstance(ret, Context):
            return [ret]
        return ret

    def __repr__(self):
        return f"AsyncImmediateTask(func={self._func}, {TaskBase.__repr__(self)})"


class ForSyncGeneratorTask(SyncGeneratorTask, ForgroundTask):
    pass


class ForSyncImmediateTask(SyncImmediateTask, ForgroundTask):
    pass


class ForSyncLoopTask(SyncLoopTask, ForgroundTask):
    pass


class SyncLongrunTask(BackSyncLongrunTask, ForgroundTask):
    pass


class InTask(SyncTask, ForgroundTask):
    def call(self, context: Context, env: Environment) -> list[Context]:
        return [context]

    def __call__(self, context: Context, env: Environment) -> list[Context]:
        return self.call(context, env)


class OutTask(AsyncTask):
    async def call(self, context: Context, env: Environment) -> list[Context]:
        if isinstance(context, OutputContext):
            return [context]
        else:
            output_context = OutputContext()
            await output_context.acopy(context)
            await context.async_context.destory()
            return [output_context]

    async def __call__(self, context: Context, env: Environment) -> list[Context]:
        return await self.call(context, env)
