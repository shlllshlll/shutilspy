# DAG Guide

## Context

Context is the data carrier that flows between tasks. It provides thread-safe key-value storage via DataWhiteBoard and supports both sync and async operations.

### AsyncContext

In async tasks, use `AsyncContext` for data operations:

```python
async def my_task(async_ctx):
    # Read data
    value = await async_ctx.get_item("key")
    # Read data with default (returns None if key missing)
    value = await async_ctx.get("key")
    # Read data with custom default
    value = await async_ctx.get("key", "default")

    # Write data
    await async_ctx.set_item("key", "value")
    # Write multiple key-value pairs at once
    await async_ctx.set_data(key1="value1", key2="value2")

    # Delete data
    await async_ctx.del_item("key")

    # Get number of items
    length = await async_ctx.len()

    # Iterate over keys
    async for key in async_ctx.keys():
        print(key)

    # Iterate over values
    async for value in async_ctx.values():
        print(value)

    # Iterate over key-value pairs
    async for key, value in async_ctx.items():
        print(key, value)

    # Check key existence
    exists = await async_ctx.contains("key")

    # Create a new context (for data-generating tasks)
    new_ctx = await async_ctx.create()

    # Destroy current context (when no longer needed)
    await async_ctx.destroy()

    # Lock for reading
    async with async_ctx.rlock():
        await do_something(async_ctx)

    # Lock for writing
    async with async_ctx.wlock():
        await do_something(async_ctx)

    return async_ctx
```

### SyncContext

In sync tasks, use `SyncContext` with dict-like syntax:

```python
def my_task(sync_ctx):
    # Read data
    value = sync_ctx["key"]
    # Read data with default (returns None if key missing)
    value = sync_ctx.get("key")
    # Read data with custom default
    value = sync_ctx.get("key", "default")

    # Write data
    sync_ctx["key"] = "value"
    # Write multiple key-value pairs at once
    sync_ctx.set_data(key1="value1", key2="value2")

    # Delete data
    del sync_ctx["key"]

    # Get number of items
    length = len(sync_ctx)

    # Iterate over keys
    for key in sync_ctx:
        print(key)
    for key in sync_ctx.keys():
        print(key)

    # Iterate over values
    for value in sync_ctx.values():
        print(value)

    # Iterate over key-value pairs
    for key, value in sync_ctx.items():
        print(key, value)

    # Check key existence
    if "key" in sync_ctx:
        pass

    # Create a new context (for data-generating tasks)
    new_ctx = sync_ctx.create()

    # Destroy current context (when no longer needed)
    sync_ctx.destroy()

    # Lock for reading
    with sync_ctx.rlock():
        do_something(sync_ctx)

    # Lock for writing
    with sync_ctx.wlock():
        do_something(sync_ctx)

    return sync_ctx
```

## Task Types

### AsyncFunctionTask

Simple async task that processes a context and returns it. Return values:

- `AsyncContext` — pass the context downstream
- `list[AsyncContext]` — pass multiple contexts downstream (e.g., one new + the original)
- `None` — discard the context

```python
async def process(async_ctx):
    data = await async_ctx.get("key", None)
    if data is None:
        return None  # Discard context

    if data == "create_new_context":
        # Create a new context and return both
        new_ctx = await async_ctx.create()
        return [new_ctx, async_ctx]

    result = await transform(data)
    await async_ctx.set("key", result)
    return async_ctx

task = AsyncFunctionTask(process, name="process")
```

### AsyncRouterTask

Routes context to specific downstream tasks based on data. By default (without a router), contexts are sent to all downstream tasks. Return values can be:

- `str` — task ID of the downstream task
- `TaskBase` — downstream task instance
- `list[str | TaskBase]` — route to multiple downstream tasks

```python
async def route(async_ctx):
    data = await async_ctx.get("type", None)
    if data == "type_a":
        return "task_a"  # Route by task ID
    elif data == "type_b":
        return task_b     # Route by task instance
    else:
        return ["task_a", "task_b"]  # Route to multiple tasks

task = AsyncRouterTask(route, name="router")
```

### AsyncStreamTask

Generator-based task that maintains state across invocations:

```python
async def stream_processor():
    ctx = yield
    while True:
        data = await ctx.get_item("data")
        result = await process(data)
        await ctx.set_item("result", result)
        ctx = yield ctx

task = AsyncStreamTask(stream_processor, name="stream")
```

### AsyncLoopTask

Continuous task that only receives an initial context, then produces new contexts in a loop without further input. This is ideal for source tasks (e.g., reading a file line by line), as it can push contexts into the DAG earlier than a one-shot `AsyncFunctionTask` that would need to read the entire file first. The DAG executor prioritizes contexts already in the pipeline, so `AsyncLoopTask` helps avoid context queuing bottlenecks.

```python
async def file_reader():
    start_ctx = yield
    async for line in read_file():
        new_ctx = await start_ctx.create()
        await new_ctx.set("line", line)
        yield new_ctx

task = AsyncLoopTask(file_reader, name="reader")
```

### AsyncServiceTask

Long-running task that processes contexts one at a time:

```python
async def writer_service(queue):
    async with aiofiles.open("output.txt", "w") as f:
        while True:
            async_ctx, future = await queue.get()
            if isinstance(async_ctx.context, StopContext):
                break
            data = await async_ctx.get_item("data")
            await f.write(data)
            future.set_result(async_ctx)

task = AsyncServiceTask(writer_service, name="writer")
```

### SyncFunctionTask

Simple synchronous task:

```python
def process(sync_ctx):
    data = sync_ctx["key"]
    sync_ctx["key"] = transform(data)
    return sync_ctx

task = SyncFunctionTask(process, name="process")
```

### Shutdown Tasks

Shutdown tasks run cleanup logic after the DAG finishes. The callable must implement a `shutdown()` method alongside `__call__()`.

**AsyncFunctionShutdownTask:**

```python
class FileUpload:
    async def __call__(self, context):
        return context

    async def shutdown(self):
        await cleanup()

task = AsyncFunctionShutdownTask(FileUpload(), name="file_upload")
```

**SyncFunctionShutdownTask:**

```python
class FileUpload:
    def __call__(self, context):
        return context

    def shutdown(self):
        cleanup()

task = SyncFunctionShutdownTask(FileUpload(), name="file_upload")
```

## Task Configuration

Each task accepts an optional `TaskConfig` for rate limiting:

```python
from shutils.dag import TaskConfig

config = TaskConfig(
    calls=100,   # Max calls within one period
    period=1,    # Period in seconds
)

task = AsyncFunctionTask(process, config=config, name="process")
```

## Building a DAG

```python
from shutils.dag import DAG, AsyncFunctionTask, Executor

dag = DAG()

# Add tasks with dependencies
dag.add_task(task_a)                  # No dependencies
dag.add_task(task_b, [task_a])        # Depends on task_a
dag.add_task(task_c, [task_a, task_b]) # Depends on both

# Build the DAG (adds SourceNode and SinkNode)
dag.build()
```

## Executor

### Offline Executor

Batch executor that processes all contexts until completion:

```python
executor = Executor(dag)
results = await executor.run()
```

### Online Executor (ServeExecutor)

Continuous executor for server scenarios:

```python
from shutils.dag import ServeExecutor, Context

executor = ServeExecutor(dag)
run_task = asyncio.create_task(executor.run())

# Submit a context
ctx = Context(executor.runtime)
task_id = await executor.submit_task(ctx)

# Check status
status = await executor.get_task_status(task_id)

# Get result
result = await executor.get_task_result(task_id)
```

### Configuration

```python
from shutils.dag import ExecutorConfig

config = ExecutorConfig(
    context_worker_num=2,    # Number of context workers
    task_worker_num=4,       # Max concurrent tasks per context
    context_queue_timeout=1.0,  # Queue timeout in seconds
    enable_context_gc=True,  # Enable context garbage collection
    enable_context_bypass=True,  # Enable bypass optimization
)
```

## Visualization

Start a Flask-based web visualization server:

```python
from shutils.dag import DAG

dag = DAG()
# ... build dag ...
dag.visualize(dag, host="0.0.0.0", port=8088)
```

Requires the `visualizer` extra: `pip install shutilspy[visualizer]`
