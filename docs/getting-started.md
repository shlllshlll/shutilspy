# Getting Started

## Installation

### pip

```bash
pip install shutilspy
```

### conda

```bash
conda install -c conda-forge shutilspy
```

### pixi

```bash
pixi add shutilspy
```

## Basic Usage

### Singleton Pattern

```python
from shutils import singleton

@singleton
class MyClass:
    def __init__(self):
        self.value = 42

# Always returns the same instance
a = MyClass()
b = MyClass()
assert a is b
```

### Caching

```python
from shutils import cached

@cached(backend="ttl", ttl=300)
def expensive_computation(x):
    return x * 2

result = expensive_computation(5)  # computed
result = expensive_computation(5)  # cached
```

### Rate Limiting

```python
from shutils import limiter

@limiter(calls=10, period=1)
def api_call():
    return "response"
```

### DAG Workflow

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

task_a = AsyncFunctionTask(step_a, name="A")
task_b = AsyncFunctionTask(step_b, name="B")
dag.add_task(task_a)
dag.add_task(task_b, [task_a])
dag.build()

executor = Executor(dag)
results = await executor.run()
```
