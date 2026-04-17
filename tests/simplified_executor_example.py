#!/usr/bin/env python3
"""
Example: Comparing Executor vs SimplifiedExecutor

This example demonstrates the difference between the original two-level
Executor and the new single-level SimplifiedExecutor.
"""

import asyncio
import logging

from shutils.dag import (
    DAG,
    AsyncFunctionTask,
    Context,
    Executor,
    ExecutorConfig,
    TaskConfig,
)
from shutils.dag.lib import limiter

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

call_times = 0

# Define some sample tasks
async def task_a(context):
    """First task in the chain."""
    print("[TaskA] Executing, setting a=10")
    await context.set_item("a", 10)
    return [context]


async def task_b(context):
    """Second task, depends on A."""
    a_value = await context.get_item("a")
    result = a_value * 2
    print(f"[TaskB] Executing, a={a_value}, result={result}")
    await context.set_item("b", result)
    return [context]


async def task_c(context):
    """Third task, depends on A."""
    global call_times
    call_times += 1
    if call_times <= 3:
        raise Exception("Simulated transient error in TaskC")
    a_value = await context.get_item("a")
    result = a_value + 5
    print(f"[TaskC] Executing, a={a_value}, result={result}")
    await context.set_item("c", result)
    return [context]


async def task_d(context):
    """Fourth task, depends on B and C."""
    b_value = await context.get_item("b")
    c_value = await context.get_item("c")
    result = b_value + c_value
    print(f"[TaskD] Executing, b={b_value}, c={c_value}, result={result}")
    await context.set_item("d", result)
    # For the final output, we just return the context
    # The framework will handle OutputContext creation if needed
    return [context]


async def main():
    """Main entry point."""
    # buid dag
    dag = DAG()

    task_a_node = AsyncFunctionTask(task_a, name="A")
    task_b_node = AsyncFunctionTask(task_b, name="B")
    task_c_node = AsyncFunctionTask(
        task_c, name="C",
        config=TaskConfig(retry_times=3, limiter=limiter.Limiter(limiter.LimiterType.QPS, rate=1))
    )
    task_d_node = AsyncFunctionTask(task_d, name="D")

    dag.add_task(task_a_node)  # Task A has no dependencies, so it's a start task
    dag.add_task(task_b_node, [task_a_node])
    dag.add_task(task_c_node, [task_a_node])
    dag.add_task(task_d_node, [task_b_node, task_c_node])
    dag.build()

    # build executor
    config = ExecutorConfig(
        context_worker_num=2,
        task_worker_num=-1,
        context_queue_timeout=1.0,
        enable_context_bypass=True
    )
    executor = Executor(dag, config=config)


    # Create multiple input contexts
    contexts = [
        Context(executor.runtime)
        for i in range(2)
    ]

    # run
    outputs = await executor.run(contexts)
    print(f"\nProcessed {len(outputs)} contexts")
    for oc in outputs:
        print(f"  Output: {oc}")


    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
