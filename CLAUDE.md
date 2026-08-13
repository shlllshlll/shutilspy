# CLAUDE.md

## Project Overview
**shutilspy** (imports as `shutils`) — Python utilities: DAG async task framework, caching (TTL/LRU), rate limiting, RW locks, and dataclass parameter helpers.

## Environment
- **Python**: >=3.12 (uses PEP 695 type aliases; `asyncio.Queue.shutdown` is guarded for Python 3.13+)
- **Package manager**: pixi (conda + pypi)
- **Install deps**: `pixi install`

## Commands
- `pixi run test` — pytest with coverage (fail_under 80)
- `pixi run lint` — ruff check
- `pixi run format` — ruff format
- `pixi run docs-serve` — mkdocs live preview
- `pixi run docs-build` — build static docs

## Code Style
- **Formatter/Linter**: ruff (target py313, line-length 120)
- **Lint rules**: E, W, F, I, N, UP, B, SIM, RUF
- **Docstrings**: Google-style, in English
- **Type hints**: Required on public APIs

## Project Structure
```
shutils/
  __init__.py          # Re-exports from utils, rwlock, cache, param, rate_limiter, imagesize, dag
  utils.py             # singleton, SingletonMeta, static_vars, get_callable_info, calculate_md5
  rwlock.py            # RWLock, AsyncRWLock
  cache.py             # TTLCache, LRUCache, PresistentMixin, cached decorator
  param.py             # asdict, dict_to_dataclass, Hide/HIDE, ParamMixin
  rate_limiter.py      # RateLimiter, RateLimitException, RateLimiterDecorator
  imagesize.py         # get(), getDPI()
  dag/                 # Async DAG task execution framework
    dag.py             # DAG graph definition
    task.py            # Task types (SyncTask, AsyncTask, FunctionTask, etc.)
    context.py         # Context (SyncContext, AsyncContext)
    executor.py        # Executor + ExecutorConfig
    serve_executor.py  # ServeExecutor (long-running)
    task_executor.py   # TaskExecutor
    runtime.py         # Runtime (top-level orchestrator)
    data_white_board.py # DataWhiteBoard (inter-task data sharing)
    task_state.py      # TaskStateMixin, ErrorInfo
    context_queue.py   # ContextQueue, ContextPriority
    task_queue.py      # TaskPriorityQueue, TaskItem
    lib/               # Internal async primitives
      aio_queue.py     # Queue, PriorityQueue (async wrappers)
      smart_lock.py    # SmartLock, SmartRWLock
      limiter.py       # Limiter, LimiterType
```

## Architecture Notes
- DAG framework: async-first; Python 3.13 queue shutdown APIs are guarded for older interpreters
- Context/DataWhiteBoard: data flows between tasks via key-value stores
- SmartLock: adaptive strategy selection (read-preferring vs write-preferring)
- Cache: persistent via lzma+pickle, auto-save by step/interval, optimistic locking
- PresistentMixin registers signal handlers — use monkeypatch in tests
