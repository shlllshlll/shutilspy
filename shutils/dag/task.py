"""Task definitions for DAG execution, including sync, async, stream, and process tasks."""

import asyncio
import contextlib
import logging
import queue
import threading
import uuid
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from inspect import isasyncgen, isgenerator
from typing import TYPE_CHECKING, Any, Protocol

from .context import AsyncContext, Context, LoopContext, OutputContext, RateLimitContext, StopContext, SyncContext
from .lib.limiter import Limiter
from .runtime import Runtime

if TYPE_CHECKING:
    from .dag import DAG

logger = logging.getLogger(__name__)

__all__ = [
    "AsyncFunctionShutdownTask",
    "AsyncFunctionTask",
    "AsyncLoopTask",
    "AsyncRouterTask",
    "AsyncServiceTask",
    "AsyncShutdownCallableProtocol",
    "AsyncShutdownTask",
    "AsyncStreamTask",
    "AsyncTask",
    "Environment",
    "ForegroundSyncFunctionTask",
    "ForegroundSyncLoopTask",
    "ForegroundSyncStreamTask",
    "ForegroundTask",
    "LongRunningTask",
    "ProcessTask",
    "ShutdownCallableProtocol",
    "ShutdownTask",
    "SinkNode",
    "SourceNode",
    "SyncFunctionShutdownTask",
    "SyncFunctionTask",
    "SyncLoopTask",
    "SyncStreamTask",
    "SyncTask",
    "SyncThreadTask",
    "TaskBase",
    "TaskConfig",
]


class ShutdownCallableProtocol(Protocol):
    """Protocol for sync callables that support a shutdown hook."""

    def __call__(self, context: SyncContext) -> list[SyncContext] | SyncContext | None: ...
    def shutdown(self) -> None: ...


class AsyncShutdownCallableProtocol(Protocol):
    """Protocol for async callables that support a shutdown hook."""

    async def __call__(self, context: AsyncContext) -> list[AsyncContext] | AsyncContext | None: ...
    async def shutdown(self) -> None: ...


@dataclass
class Environment:
    """Runtime environment passed to tasks during execution.

    Attributes:
        runtime: The runtime counter for tracking active contexts.
        process_pool: Optional process pool for CPU-bound task execution.
        dag: The DAG this environment belongs to.
    """
    runtime: Runtime
    process_pool: ProcessPoolExecutor | None
    dag: "DAG"


@dataclass
class TaskConfig:
    """Configuration for task execution behavior.

    Attributes:
        retry_times: Maximum number of retry attempts on failure.
        retry_interval: Seconds between retries, or a callable returning the interval.
        parallel_num: Maximum concurrent executions (0 means unlimited).
        limiter: Optional rate limiter for QPS/concurrency control.
    """
    retry_times: int = 0
    retry_interval: int | float | Callable[[Context], float | int] = 0
    parallel_num: int = 0
    limiter: Limiter | None = None

class TaskBase(ABC):  # noqa: B024
    """Abstract base class for all DAG tasks."""

    def __init__(self, func: Callable | None, config: TaskConfig | None = None, name: str = ""):
        """Initialize a task with optional function, config, and name.

        Args:
            func: The callable this task wraps.
            config: Task execution configuration.
            name: Optional task name; defaults to a UUID.
        """
        if config is None:
            config = TaskConfig()
        self.id = name if name else str(uuid.uuid4())
        self.upstream_tasks: set[TaskBase] = set()
        self.downstream_tasks: set[TaskBase] = set()
        self.config = config
        self.running_task_num = 0
        self.rate_limiter = self.config.limiter

    def add_upstream(self, task: "TaskBase"):
        """Add an upstream dependency to this task.

        Args:
            task: The upstream task to depend on.
        """
        self.upstream_tasks.add(task)
        task.downstream_tasks.add(self)

    def call_before(self, context: Context) -> Context | None:
        """Pre-execution hook for parallel and rate-limit control.

        Returns a context if the task should be skipped, or None to proceed.
        """
        # parallel control
        if self.config.parallel_num > 0 and self.running_task_num > self.config.parallel_num:
            return context
        self.running_task_num += 1

        # qps control
        if self.rate_limiter is not None and not self.rate_limiter.try_acquire().success:
            logger.debug(f"[Task {self.id}]: rate limit exceeded, throttling...")
            self.running_task_num -= 1
            return RateLimitContext(context)

        return None

    def call_after(self, context_list: list[Context]) -> None:
        """Post-execution hook to release rate-limit tokens and update parallel count."""
        self.running_task_num -= 1
        if self.rate_limiter is not None:
            self.rate_limiter.release()

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other: object):
        if not isinstance(other, TaskBase):
            return False
        return self.id == other.id

    def __repr__(self):
        return f"id={self.id}, config={self.config}"


class ForegroundTask(ABC):
    """Marker class for tasks that must run on the main event loop thread."""

    @abstractmethod
    def _foreground_marker(self): ...


class LongRunningTask(ABC):
    """Marker class for long-running tasks that maintain their own thread or coroutine."""

    @abstractmethod
    def _long_running_marker(self): ...


class ShutdownTask(ABC):
    """Abstract base for tasks that require a sync shutdown hook."""

    @abstractmethod
    def shutdown(self):
        """Clean up resources when the executor stops."""


class AsyncShutdownTask(ABC):
    """Abstract base for tasks that require an async shutdown hook."""

    @abstractmethod
    async def shutdown(self):
        """Clean up resources asynchronously when the executor stops."""


class SyncTask(TaskBase):
    """Base class for synchronous tasks."""

    @abstractmethod
    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        """Execute the task synchronously.

        Args:
            sync_ctx: The sync context for this execution.
            env: The runtime environment.

        Returns:
            List of output sync contexts.
        """

    def __call__(self, context: Context, env: Environment) -> list[Context]:
        ret = self.call_before(context)
        if ret:
            return [ret]
        sync_ret = self.call(context.sync_context, env)
        ret = [ctx.context for ctx in sync_ret]
        self.call_after(ret)
        return ret


class AsyncTask(TaskBase):
    """Base class for asynchronous tasks."""

    @abstractmethod
    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        """Execute the task asynchronously.

        Args:
            async_ctx: The async context for this execution.
            env: The runtime environment.

        Returns:
            List of output async contexts.
        """

    async def __call__(self, context: Context, env: Environment) -> list[Context]:
        ret = self.call_before(context)
        if ret:
            return [ret]
        async_ret = await self.call(context.async_context, env)
        ret = [ctx.context for ctx in async_ret]
        self.call_after(ret)
        return ret


class ProcessTask(AsyncTask):
    """Async task that offloads execution to a process pool."""

    def __init__(
        self,
        func: Callable[[SyncContext], list[SyncContext] | SyncContext],
        config: TaskConfig | None = None,
        name: str = "",
    ):
        """Initialize a process task.

        Args:
            func: The sync function to run in a process pool.
            config: Task execution configuration.
            name: Optional task name.
        """
        if config is None:
            config = TaskConfig()
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
        """Execute the function in a process pool and return output contexts.

        Args:
            async_ctx: The async context for this execution.
            env: The runtime environment providing the process pool.

        Returns:
            List of output async contexts.

        Raises:
            RuntimeError: If no process pool is configured.
        """
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


class SyncStreamTask(SyncTask, ShutdownTask):
    """Sync task backed by a generator that streams multiple outputs."""

    def __init__(
        self,
        func: Callable[[], Generator[SyncContext | list[SyncContext] | None, SyncContext]],
        config: TaskConfig | None = None,
        name: str = "",
    ):
        """Initialize a sync stream task.

        Args:
            func: A generator function that yields output contexts.
            config: Task execution configuration.
            name: Optional task name.

        Raises:
            ValueError: If func does not return a generator.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(func, config, name)
        self._generator = func()
        if not isgenerator(self._generator):
            raise ValueError("func must be a generator function")
        next(self._generator)

    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        """Send the context to the generator and return yielded outputs."""
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
        """Close the underlying generator."""
        with contextlib.suppress(GeneratorExit):
            self._generator.close()


class SyncFunctionTask(SyncTask):
    """Sync task that wraps a simple function."""

    def __init__(
        self,
        func: Callable[[SyncContext], list[SyncContext] | SyncContext | None],
        config: TaskConfig | None = None,
        name: str = "",
    ):
        """Initialize a sync function task.

        Args:
            func: The sync function to execute.
            config: Task execution configuration.
            name: Optional task name.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(func, config, name)
        self._func = func

    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        """Execute the wrapped function and return output contexts."""
        ret = self._func(sync_ctx)
        if isinstance(ret, SyncContext):
            return [ret]
        elif ret is None:
            return []
        return ret
        if isinstance(ret, SyncContext):
            return [ret]
        elif ret is None:
            return []
        return ret

    def __repr__(self):
        return f"SyncFunctionTask(func={self._func}, {TaskBase.__repr__(self)})"


class SyncFunctionShutdownTask(SyncFunctionTask, ShutdownTask):
    """Sync function task with a shutdown hook."""

    def __init__(self, shutdown_callable: ShutdownCallableProtocol, config: TaskConfig | None = None, name: str = ""):
        """Initialize with a shutdown-callable function.

        Args:
            shutdown_callable: A callable that also has a shutdown() method.
            config: Task execution configuration.
            name: Optional task name.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(shutdown_callable, config, name)

    def shutdown(self):
        """Delegate shutdown to the wrapped callable."""
        self._func.shutdown()


class SyncLoopTask(SyncStreamTask):
    """Sync stream task that automatically re-enqueues until the generator exhausts."""

    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        """Execute one iteration and append a LoopContext to continue the loop."""
        need_create_loop_context = not isinstance(sync_ctx.context, LoopContext)
        ret = super().call(sync_ctx, env)
        if ret:
            if need_create_loop_context:
                ret.append(LoopContext(sync_ctx.context._runtime, self).sync_context)
            else:
                ret.append(sync_ctx)
        elif not need_create_loop_context:
            sync_ctx.destory()

        return ret


class SyncThreadTask(SyncTask, LongRunningTask, ShutdownTask):
    """Sync task that runs in a dedicated background thread."""

    def __init__(
        self,
        func: Callable[[queue.Queue[tuple[Context, queue.Queue[list[Context] | Context | None]]]], None],
        config: TaskConfig | None = None,
        name: str = "",
    ):
        """Initialize a sync thread task.

        Args:
            func: A function that consumes from a queue of (context, response_queue) pairs.
            config: Task execution configuration.
            name: Optional task name.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(func, config, name)
        self.__input_queue = queue.Queue()
        self._func = func
        self.__thread = None

    def call(self, sync_ctx: SyncContext, env: Environment) -> list[SyncContext]:
        """Send context to the background thread and wait for the result."""
        if self.__thread is None:
            self.__thread = threading.Thread(target=self._func, args=(self.__input_queue,))
            self.__thread.start()
        if not self.__thread.is_alive():
            logger.error("[SyncThreadTask]: thread exit unexpectedly")
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
        """Send a stop signal to the background thread and join it."""
        if self.__thread and self.__thread.is_alive():
            self.__input_queue.put((StopContext().sync_context, queue.Queue()))
            self.__thread.join()


class AsyncServiceTask(AsyncTask, LongRunningTask, AsyncShutdownTask):
    """Async task backed by a long-running coroutine service."""

    def __init__(
        self,
        func: Callable[[asyncio.Queue[tuple[AsyncContext, asyncio.Future]]], Coroutine[Any, Any, None]],
        config: TaskConfig | None = None,
        name: str = "",
    ):
        """Initialize an async service task.

        Args:
            func: An async function that consumes from a queue of (context, future) pairs.
            config: Task execution configuration.
            name: Optional task name.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(func, config, name)
        self.__input_queue = asyncio.Queue()
        self.__task = None
        self._func = func

    def _task_down_callback(self, task: asyncio.Task):
        """Callback when the service task finishes unexpectedly."""
        if self.__future.done():
            return

        try:
            result = task.result()
            self.__future.set_result(result)
        except Exception as e:
            logger.error(f"[AsyncServiceTask]: task exit with exception: {e}")
            self.__future.set_exception(e)

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        """Send context to the service coroutine and await the result."""
        if self.__task is None:
            self.__task = asyncio.create_task(self._func(self.__input_queue))
            self.__task.add_done_callback(self._task_down_callback)
        elif self.__task.done():
            logger.error("[AsyncServiceTask]: task exit unexpectedly")
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
        return f"AsyncServiceTask(func={self._func}, {TaskBase.__repr__(self)})"

    async def shutdown(self):
        """Send a stop signal to the service coroutine and await its completion."""
        if self.__task and not self.__task.done():
            await self.__input_queue.put((StopContext().async_context, asyncio.Future()))
            await self.__task
            self.__task = None


class AsyncStreamTask(AsyncTask, AsyncShutdownTask):
    """Async task backed by an async generator that streams multiple outputs."""

    def __init__(
        self,
        func: Callable[[], AsyncGenerator[AsyncContext | list[AsyncContext] | None, AsyncContext]],
        config: TaskConfig | None = None,
        name: str = "",
    ):
        """Initialize an async stream task.

        Args:
            func: An async generator function that yields output contexts.
            config: Task execution configuration.
            name: Optional task name.

        Raises:
            ValueError: If func does not return an async generator.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(func, config, name)
        self.__generator = func()
        if not isasyncgen(self.__generator):
            raise ValueError("func must be a async generator function")
        self.__activate_generator = False

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        """Send the context to the async generator and return yielded outputs."""
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
        """Close the underlying async generator."""
        with contextlib.suppress(GeneratorExit):
            await self.__generator.aclose()


class AsyncLoopTask(AsyncStreamTask):
    """Async stream task that automatically re-enqueues until the generator exhausts."""

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        """Execute one iteration and append a LoopContext to continue the loop."""
        need_create_loop_context = not isinstance(async_ctx.context, LoopContext)
        ret = await super().call(async_ctx, env)
        if ret:
            if need_create_loop_context:
                ret.append(LoopContext(async_ctx.context._runtime, self).async_context)
            else:
                ret.append(async_ctx)
        elif not need_create_loop_context:
            await async_ctx.destory()
        return ret


class AsyncFunctionTask(AsyncTask):
    """Async task that wraps a simple async function."""

    def __init__(
        self,
        func: Callable[[AsyncContext], Coroutine[Any, Any, list[AsyncContext] | AsyncContext | None]],
        config: TaskConfig | None = None,
        name: str = "",
    ):
        """Initialize an async function task.

        Args:
            func: The async function to execute.
            config: Task execution configuration.
            name: Optional task name.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(func, config, name)
        self._func = func

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        """Execute the wrapped async function and return output contexts."""
        ret = await self._func(async_ctx)
        if isinstance(ret, AsyncContext):
            return [ret]
        elif ret is None:
            return []
        return ret

    def __repr__(self):
        return f"AsyncFunctionTask(func={self._func}, {TaskBase.__repr__(self)})"


class AsyncRouterTask(AsyncTask):
    """Async task that routes contexts to specific downstream tasks."""

    def __init__(
        self,
        func: Callable[[AsyncContext], Coroutine[Any, Any, str | TaskBase | list[str | TaskBase]]],
        config: TaskConfig | None = None,
        name: str = "",
    ):
        """Initialize an async router task.

        Args:
            func: An async function returning task name(s) or task object(s) to route to.
            config: Task execution configuration.
            name: Optional task name.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(None, config, name)
        self._func = func
        self._mask_task_cache: dict[tuple[TaskBase, ...], set[TaskBase]] = {}

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        """Route the context to specified downstream tasks and mask bypass tasks."""
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


class AsyncFunctionShutdownTask(AsyncFunctionTask, AsyncShutdownTask):
    """Async function task with an async shutdown hook."""

    def __init__(
        self, shutdown_callable: AsyncShutdownCallableProtocol, config: TaskConfig | None = None, name: str = ""
    ):
        """Initialize with an async-shutdown-callable function.

        Args:
            shutdown_callable: An async callable that also has a shutdown() method.
            config: Task execution configuration.
            name: Optional task name.
        """
        if config is None:
            config = TaskConfig()
        super().__init__(shutdown_callable, config, name)

    async def shutdown(self):
        """Delegate shutdown to the wrapped async callable."""
        await self._func.shutdown()


class ForegroundSyncStreamTask(SyncStreamTask, ForegroundTask):
    """Foreground variant of SyncStreamTask."""

    def _foreground_marker(self): pass


class ForegroundSyncFunctionTask(SyncFunctionTask, ForegroundTask):
    """Foreground variant of SyncFunctionTask."""

    def _foreground_marker(self): pass


class ForegroundSyncLoopTask(SyncLoopTask, ForegroundTask):
    """Foreground variant of SyncLoopTask."""

    def _foreground_marker(self): ...


class SourceNode(AsyncTask):
    """Source node that passes through input contexts unchanged."""

    def __init__(self, name: str = "#SourceNode"):
        """Initialize the source node.

        Args:
            name: Node name, defaults to "#SourceNode".
        """
        super().__init__(None, name=name)

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        """Pass through the context unchanged."""
        return [async_ctx]

    async def __call__(self, context: Context, env: Environment) -> list[Context]:
        async_ret = await self.call(context.async_context, env)
        return [ctx.context for ctx in async_ret]


class SinkNode(AsyncTask):
    """Sink node that converts contexts to OutputContext for final results."""

    def __init__(self, name: str = "#SinkNode"):
        """Initialize the sink node.

        Args:
            name: Node name, defaults to "#SinkNode".
        """
        super().__init__(None, name=name)

    async def call(self, async_ctx: AsyncContext, env: Environment) -> list[AsyncContext]:
        """Convert the context to an OutputContext if not already one."""
        if isinstance(async_ctx.context, OutputContext):
            return [async_ctx]
        else:
            output_context = OutputContext()
            await output_context.acopy(async_ctx.context)
            await async_ctx.destory(destory_parent=True)
            return [output_context.async_context]

    async def __call__(self, context: Context, env: Environment) -> list[Context]:
        asnc_ret = await self.call(context.async_context, env)
        return [ctx.context for ctx in asnc_ret]
