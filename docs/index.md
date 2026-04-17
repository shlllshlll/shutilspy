# shutilspy

Python utilities including DAG task framework, caching, rate limiting, RW locks, and parameter helpers.

## Features

- **DAG Framework**: Build and execute directed acyclic graph workflows with sync/async tasks
- **Caching**: TTL and LRU caches with persistent storage and auto-save
- **Rate Limiting**: Token-bucket, QPS, and concurrency limiters with sync/async support
- **RW Locks**: Read-write locks for both sync and async code
- **Parameter Helpers**: Dataclass utilities for serialization, deserialization, and hidden fields
- **Smart Lock**: Adaptive lock with automatic strategy selection based on operation duration
- **Image Size**: Get image dimensions from binary headers without decoding

## Quick Start

```python
from shutils.dag import DAG, AsyncFunctionTask, Executor

# Build a simple pipeline
dag = DAG()

async def process(ctx):
    ctx.context.sync_white_board["result"] = "done"
    return ctx

task = AsyncFunctionTask(process, name="process")
dag.add_task(task)
dag.build()

# Execute
executor = Executor(dag)
results = await executor.run()
```

## Installation

```bash
pip install shutilspy
```
