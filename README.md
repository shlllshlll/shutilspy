# shutilspy

Python utilities including DAG task framework, caching, rate limiting, RW locks, and parameter helpers.

## Features

- **DAG Framework** - Build and execute directed acyclic graph workflows with sync/async tasks, routing, streaming, and service tasks
- **Caching** - TTL and LRU caches with persistent storage (lzma+pickle), auto-save by step/interval, and decorator support
- **Rate Limiting** - Token-bucket, QPS, and concurrency limiters with sync/async support and decorator
- **RW Locks** - Read-write locks for both sync (`RWLock`) and async (`AsyncRWLock`) code
- **Smart Lock** - Adaptive lock (`SmartLock`, `SmartRWLock`) with automatic strategy selection based on operation duration
- **Parameter Helpers** - Dataclass utilities for serialization (`asdict`, `asjson`), deserialization (`dict_to_dataclass`), and hidden fields (`Hide`/`HIDE`)
- **Image Size** - Get image dimensions from binary headers without full decoding (GIF, PNG, JPEG, TIFF, SVG, WebP)

## Installation

### pip

```bash
# without DAG visualizer
pip install shutilspy

# with DAG visualizer
pip install shutilspy[visualizer]
```

### conda

```bash
# without DAG visualizer
conda install shlllshlll::shutilspy
# with DAG visualizer
conda install shlllshlll::shutilspy-visualizer
```

### Optional dependencies

```bash
pip install shutilspy[visualizer]  # Flask-based DAG visualization
```

## Quick Examples

### Singleton

```python
from shutils import singleton

@singleton
class MyClass:
    pass

assert MyClass() is MyClass()
```

### Caching

```python
from shutils import cached

@cached(backend="ttl", ttl=300)
def expensive(x):
    return x * 2
```

### DAG Pipeline

```python
from shutils.dag import DAG, AsyncFunctionTask, Executor

dag = DAG()

async def step_a(ctx):
    ctx.context.sync_white_board["a"] = "done"
    return ctx

async def step_b(ctx):
    val = ctx.context.sync_white_board.get("a")
    ctx.context.sync_white_board["b"] = f"{val}_processed"
    return ctx

dag.add_task(AsyncFunctionTask(step_a, name="A"))
dag.add_task(AsyncFunctionTask(step_b, name="B"), [task_a])
dag.build()

results = await Executor(dag).run()
```

### Rate Limiting

```python
from shutils import limiter

@limiter(calls=10, period=1)
def api_call():
    return "response"
```

## Documentation

Full documentation is available at the [docs](https://shlllshlll.github.io/shutilspy/).

- [Getting Started](https://shlllshlll.github.io/shutilspy/getting-started/)
- [DAG Framework Overview](https://shlllshlll.github.io/shutilspy/dag/)
- [DAG Guide](https://shlllshlll.github.io/shutilspy/dag/guide/)

## Development

```bash
pixi install
pixi run test      # Run tests with coverage
pixi run lint      # Run ruff linter
pixi run format    # Run ruff formatter
pixi run docs-serve  # Start docs server
```

## License

MIT
