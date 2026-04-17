# PROGRESS.md

## Phase 1: Environment & Configuration Setup
- [x] Update `pyproject.toml` (requires-python, optional-deps, dev deps, ruff/pytest/coverage, pixi tasks)
- [x] Update `.gitignore` (coverage, ruff_cache, site/)
- [x] Run `pixi install`

## Phase 4.5: Developer Documentation
- [x] Create `CLAUDE.md`
- [x] Create `PROGRESS.md`

## Phase 2: Test Suite (265 tests, all passing)
- [x] `tests/conftest.py` — shared fixtures
- [x] `tests/test_utils.py` — singleton, SingletonMeta, static_vars, etc.
- [x] `tests/test_rwlock.py` — RWLock, AsyncRWLock
- [x] `tests/test_cache.py` — TTLCache, LRUCache, cached decorator, PresistentMixin
- [x] `tests/test_param.py` — asdict, dict_to_dataclass, Hide/HIDE, ParamMixin
- [x] `tests/test_rate_limiter.py` — RateLimiter, RateLimitException, RateLimiterDecorator
- [x] `tests/test_imagesize.py` — get(), getDPI()
- [x] `tests/dag/conftest.py` — DAG fixtures
- [x] `tests/dag/test_runtime.py`
- [x] `tests/dag/test_task_state.py`
- [x] `tests/dag/test_data_white_board.py`
- [x] `tests/dag/test_context.py`
- [x] `tests/dag/test_context_queue.py`
- [x] `tests/dag/test_task_queue.py`
- [x] `tests/dag/test_dag.py`
- [x] `tests/dag/test_task.py`
- [x] `tests/dag/test_executor.py`
- [x] `tests/dag/test_serve_executor.py`
- [x] `tests/dag/test_task_executor.py`
- [x] `tests/dag/test_helper.py`
- [x] `tests/dag/test_utils.py`
- [x] `tests/dag/lib/test_limiter.py`
- [x] `tests/dag/lib/test_aio_queue.py`
- [x] `tests/dag/lib/test_smart_lock.py`

## Phase 3: Docstring Standardization
- [x] Replace `Brief:` headers with proper English module docstrings (utils, rwlock, cache, rate_limiter, param, dag)
- [x] Add Google-style docstrings to Tier 1 APIs (utils, rwlock, cache, rate_limiter, param)
- [x] Add Google-style docstrings to Tier 2 APIs (dag modules — added by docstring agent)
- [ ] Add Google-style docstrings to Tier 3 APIs (lib/, helper, utils, global_data, visualizer, imagesize)
- [x] Add `__all__` to each module

## Phase 4.1-4.4: Documentation Setup
- [x] Create `mkdocs.yml`
- [x] Create `docs/index.md`
- [x] Create `docs/getting-started.md`
- [x] Create API doc pages (`docs/api/*.md`)
- [x] Migrate `docs/dag.md` to English (`docs/dag/index.md` + `docs/dag/guide.md`)
- [x] Update `README.md`

## Phase 5: CI Workflow
- [x] Create `.github/workflows/test.yml`

## Verification
- [x] `pixi run test` — all tests pass (265/265), coverage >= 70%
- [x] `pixi run docs-build` — docs build successfully (griffe warnings are non-critical)
- [ ] `pixi run lint` — remaining warnings are pre-existing style issues (RUF003 Chinese comments, E721 type comparisons, UP007 old Union syntax)

## Bugs Found & Fixed
- [x] `shutils/dag/helper.py` — syntax error: `dict[str, Any]]` extra `]`
- [x] `shutils/cache.py:267` — `now` reference instead of `time.time()` in TTLCache.cleanup
- [x] `shutils/cache.py:346` — missing `while` loop in LRUCache.cleanup (docstring agent regression)
- [x] `shutils/utils.py` — docstring agent rewrote singleton decorator incorrectly; restored and properly enhanced
