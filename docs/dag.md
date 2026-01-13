# DAG (Directed Acyclic Graph) 功能说明

shutilspy 的 DAG 模块提供了一个灵活的任务编排框架，支持构建和执行有向无环图（DAG）形式的工作流。该框架支持同步和异步任务，并提供了丰富的任务类型和配置选项。

## 核心组件

### Context上下文

Context 是DAG任务执行中的数据载体，用于在任务之间传递数据。Context 对象提供了同步和异步两类操作接口，分别用于同步任务和异步任务中。Context包括两类接口：

#### AsyncContext

在异步任务中，上下文对象类型是AsyncContext，有如下的操作接口：

**数据读写接口：**

```python
# 读取数据
await async_ctx.get_item("key")
# 写入数据
await async_ctx.set_item("key", "value")
# 删除数据
await async_ctx.del_item("key")
# 获取数据长度
await async_ctx.len()
# 迭代获取key
for key in await async_ctx:
    print(key)
# 迭代获取key，方法2
for key in await async_ctx.keys():
    print(key)
# 迭代获取value
for value in await async_ctx.values():
    print(value)
# 迭代获取key和value
for key, value in await async_ctx.items():
    print(key, value)
# 判断key是否存在
await async_ctx.contains("key")
# 同时写入多个key-value
await async_ctx.set_data(key1="value1", key2="value2")
# 获取数据，key不存在时，返回None
await async_ctx.get("key")
# 获取数据，key不存在时，返回默认值
await async_ctx.get("key", "default")
# 获取读锁
async with async_ctx.rlock():
    await do_something(async_ctx)
# 获取写锁
async with async_ctx.wlock():
    await do_something(async_ctx)
```

**上下文控制接口：**

```python
# 创建新context，用于数据生成任务中
new_context = await async_ctx.create()
# 销毁当前context，当任务中不在需要当前context
await async_ctx.destory()
```

#### SyncContext

在同步任务中，上下文对象类型是SyncContext，有如下的操作接口：

**数据读写接口：**

```python
# 读取数据
sync_ctx["key"]
# 写入数据
sync_ctx["key"] = "value"
# 删除数据
del sync_ctx["key"]
# 获取数据长度
len(sync_ctx)
# 迭代获取key
for key in sync_ctx:
    print(key)
# 迭代获取key，方法2
for key in sync_ctx.keys():
    print(key)
# 迭代获取value
for value in sync_ctx.values():
    print(value)
# 迭代获取key和value
for key, value in sync_ctx.items():
    print(key, value)
# 判断key是否存在
"key" in sync_ctx
# 同时写入多个key-value
sync_ctx.set_data(key1="value1", key2="value2")
# 获取数据，key不存在时，返回None
sync_ctx.get("key")
# 获取数据，key不存在时，返回默认值
sync_ctx.get("key", "default")
# 获取读锁
with sync_ctx.rlock():
    do_something(sync_ctx)
# 获取写锁
with sync_ctx.wlock():
    do_something(sync_ctx)
```

**上下文控制接口：**

```python
# 创建新context，用于数据生成任务中
new_context = sync_ctx.create()
# 销毁当前context，当任务中不在需要当前context
sync_ctx.destroy()
```

### Task任务

框架提供了多种类型的异步和同步任务，由于 shutilspy 的 dag 使用了基于异步的执行器，因此更推荐使用异步任务，执行效率更高；同时如果同步任务使用不当，还存在事件循环卡死的风险。

#### 异步任务

- `AsyncFunctionTask`: 简单异步任务，即传入context，对context进行一些处理，随后返回处理后的context

  ```python
  async def process_data(async_ctx: AsyncContext) -> AsyncContext | list[AsyncContext] | None:
    data = await async_ctx.get("key", None)
    if data is None:
        logger.error(f"data is None, context: {async_ctx}")
        # context不符合预期，返回None，丢弃当前context
        return None
    if data == "create_new_context":
        # 创建新的context
        new_context = await async_ctx.create()
        # 同时返回新的context和输入的context
        return [new_context, async_ctx]
    new_data = await process_data(data)
    await async_ctx.set("key", new_data)
    # 处理完成返回当前context
    return async_ctx

  task = dag.AsyncFunctionTask(process_data)
  ```

- `AsyncRouterTask`: 路由任务，根据输入context的某个字段值，决定将context发送到哪些下游任务处理，默认情况下，路由任务会将context发送到所有下游任务处理。

  ```python
  async def route_task(async_ctx: AsyncContext) -> str | TaskBase | list[str | TaskBase]:
    data = await async_ctx.get("key", None)
    if data == "route_to_task1":
        return "task1"  # 发送到task1处理，可以指定下游task的id名，也可以直接返回下游task的实例；可以返回一个list，表示发送到多个下游任务
    elif data == "route_to_task2":
        return "task2" # 发送到task2处理
    else:
        return ["task1", "task2"]  # 发送到task1和task2处理

  task = dag.AsyncRouterTask(route_task)
  ```

- `AsyncStreamTask`: 异步生成器任务，即通过yield实现context的传入传出，用于需要在一个循环中处理context的场景

  ```python
  async def process_data_generator():
    # 接收第一个传入的context
    next_context = yield
    while True:
        cur_context = next_context
        data = await cur_context.get("key", None)
        # 异步处理数据
        result = await process_data(data)
        await cur_context.set("key", result)
        # 传出当前context并接收下一个传入的context
        next_context = yield cur_context

  task = dag.AsyncStreamTask(process_data_generator)
  ```

- `AsyncLoopTask`: 循环执行的异步任务，同样基于yield实现，不同的是，syncLoopTask仅在开始时接收一个输入context，在进入循环后，只进行context输出，不进行context输入。此类任务常用于dag中的起始的读取文件并生产context的任务，相比使用AsyncFunctionTask一次性读取文件生产所有的Context再送入dag框架，AsyncLoopTask可以更早的将context送入dag框架，同时dag的执行器被设计为优先处理已经在dag中运行的Context，因此AsyncLoopTask可以避免在dag中出现大量的context排队的问题。

  ```python
  async def loop_processor():
    start_context = yield
    while True:
        async with aiofile.async_open("file.txt", "r") as f:
            async for line in f:
                new_context = await start_context.create()
                await new_context.set("line", line)
                # 传出当前context并接收下一个传入的context
                yield new_context

  task = dag.AsyncLoopTask(loop_processor)
  ```

- `AsyncServiceTask`: 长时间运行的异步任务，常用于诸如文件写入的场景，处理完一个context后，需要保持文件打开，等待下一个context的到来，直到所有context处理完成再关闭。

  ```python
  async def long_running_task(queue):
    async with aiofile.async_open("file.txt", "w") as f:
        while True:
          async_ctx, future = await queue.get()
          # dag执行结束后，dag框架会通过queue传入一个StopContext，用于通知任务结束，此时需要退出任务
          if isinstance(async_ctx.context, dag.StopContext):
              break
          data = await async_ctx.get("data", None)
          await f.write(data)
          # 处理完成，通过asyncio.Future将context返回dag框架
          future.set_result(async_ctx)

  task = dag.AsyncServiceTask(long_running_task)
  ```

- `AsyncFunctionShutdownTask`: 异步关闭任务，用于需要在dag执行结束后执行的异步操作。

  ```python
  class FileUpload():
    async def __call__(self, context):
        # do nothing
        return context

    async def shutdown(self):
        await do_something()

  async_shutdown_task = dag.AsyncFunctionShutdownTask(FileUpload())
  ```


#### 同步任务

- `SyncFunctionTask`: 立即执行的同步任务

  ```python
  def process_data(context):
      # 同步处理数据
      data = context["key"]
      new_data = do_something(data)
      context["key"] = new_data
      return context

  task = dag.SyncFunctionTask(process_data)
  ```

- `SyncFunctionShutdownTask`: 同步关闭任务，用于需要在dag执行结束后执行的同步操作。

  ```python
  class FileUpload():
    def __call__(self, context):
        # do nothing
        return context

    def shutdown(self):
        do_something()

  sync_shutdown_task = dag.SyncFunctionShutdownTask(FileUpload())
  ```


#### 任务配置

每个任务的创建接口都有一个第二个隐藏参数可传入 `TaskConfig` 进行配置：

```python
from shutils.dag import TaskConfig

config = TaskConfig(
    # 用于控制统一个task并行执行的context数量，用于qps控制
    calls=100,             # 一个时间周期内调用次数限制
    period=1               # 时间周期（秒）
)
```

### DAG

DAG 类管理task以及task之间的依赖关系。

主要方法：

- `add_task(task, dependencies: None | TaskBase | list[TaskBase] = None)`: 添加任务及其依赖关系
- `build()`: 在添加task完成后调用，用于完成一些后置流程，比如：为DAG添加统一的输入和输出task，保证输入输出task的唯一性。

```python
from shutils.dag import DAG

dag_graph = DAG()
dag_graph.add_task(task1)
dag_graph.add_task(task2, dependencies=[task1])
dag_graph.build()
```

## Executor 执行器

Executor 是 DAG 的任务执行器，在 dag 构建结束后，就可以通过 Executor 来执行任务了。

### 执行器参数

Executor 的构造函数中第二个参数可以传入 `ExecutorConfig` 进行配置：

```python
from shutils.dag import ExecutorConfig

config = ExecutorConfig(
    # 执行器数量，决定了Executor能够同时处理Context的数量
    context_worker_num=1,
    # 子执行器数量，决定了一个Context能够同时执行的Task的数量；设置为小于1的值时，表示不限制子执行器数量
    task_worker_num=1
)
```

### 离线执行器

默认的Exector是一个批量执行器，即一次性将所有的Context送入DAG中进行处理或由其中的Task生成Context，直到所有Context处理完成后，Executor结束执行并返回结果。这种Executor非常适合离线DAG任务的处理。

```python
from shutils.dag import Executor

executor = Executor(dag_graph)
ret = await executor.run()
```

### 在线执行器

在诸如服务器等场景下，我们需要一个在线执行器，即DAG执行器能够持续接收Context进行处理，而不是一次性将所有Context送入DAG中进行处理。shutilspy 提供了一个 ServeExecutor 用于在线DAG任务的处理，通过`提交任务-等待任务-查询任务`的方式实现任务的处理。

```python
import asyncio
from shutils.dag import ServeExecutor, Context

executor = ServeExecutor(dag_graph)
executor_task = asyncio.create_task(executor.run())

# 提交任务
context = Context(executor.runtime)
context.sync_context['a'] = 3
context.sync_context['b'] = 5
task_id = await executor.submit_task(context)

# 查询任务状态
task_status = await executor.get_task_status(task_id)
print(task_status)

# 等待任务完成并获取结果
result = await executor.get_task_result(task_id)
```

## 可视化

shutilspy 的 dag 模块提供了一个基于flask的dag可视化服务，可以通过如下的方式启动：

```bash
from shutils import dag

# 1. build dag graph
dag_graph = dag.DAG()
dag_graph.add_task(task1)
dag_graph.add_task(task2, dependencies=[task1])
dag_graph.build()

# 2. start dag server
dag_graph.visualize(dag_graph, host="0.0.0.0", port=8088)
```

## TODO

- [x] 统一Context中各种同步、异步接口设计，取消需要先显示获取async_context和sync_context以及white_board的设计，统一通过context.xxx的方式访问，根据执行环境自动切换同步异步方式
- [x] 通用的worker级别全局变量管理和全局资源管理器
- [x] 新增支持服务模型的Executor，可以将dag中的task作为一个长期运行的服务，持续接收context进行处理
- [x] 完善限流控制功能
- [x] 完善错误重试处理机制
- [x] 优化bypass功能
- [x] 优化gc功能
- [x] 放开父子Context的两级限制
- [ ] 去除Executor中两级任务执行的设计，仅使用一级，简化执行逻辑
- [ ] 将partial函数功能融入接口，简化功能实现
- [ ] 优化锁实现方案
- [ ] 支持子DAG嵌套
- [ ] 调试和可视化功能增强，通过网页端可以查看context、task的任务执行情况，可以在执行task的时候，同步启动一个web服务，实时查看dag的执行情况
- [ ] 强化潜在错误探测机制：

  - [ ] 探测诸如：如在不动点和非不动点探测修改返回值的问题、父子context处理问题等
