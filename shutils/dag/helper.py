#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: helper.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import inspect
import importlib
from typing import Any, Callable
from functools import partial
from .dag import DAG
from ..utils import get_class
from .task import TaskBase, TaskConfig
from .runtime import Runtime
from .executor import Executor


def get_callable_func(func_str: str) -> Callable[..., Any]:
    """get callable function from string

    Args:
        func_str (str): function string

    Raises:
        ValueError: function string is not valid
        ValueError: function is not callable

    Returns:
        Callable[..., Any]: callable function
    """
    parts = func_str.split(".")
    # 1. 尝试locals
    caller_frame = inspect.currentframe().f_back
    caller_locals = caller_frame.f_locals if caller_frame else {}
    if parts[0] in caller_locals:
        processor_func = caller_locals.get(parts[0])
        if len(parts) == 2:
            processor_func = getattr(processor_func, parts[1])
    # 2. 尝试globals
    elif parts[0] in globals():
        processor_func = globals().get(parts[0])
        if len(parts) == 2:
            processor_func = getattr(processor_func, parts[1])
    # 3. 尝试importlib
    else:
        parts = func_str.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(f"processor is not valid: {func_str}")
        module = importlib.import_module(parts[0])
        processor_func = getattr(module, parts[1])
    # 检查processor_func是否为可调用对象
    if not callable(processor_func):
        raise ValueError(f"processor is not callable: {func_str}")
    return processor_func


def get_params(params: dict) -> dict[str, Any]]:
    """get params from dict

    Args:
        params (dict): params

    Raises:
        Exception: invalid params type

    Returns:
        dict[str, Any]: kwargs
    """
    kwargs = {}
    if type(params) == dict:
        for key, value in params.items():
            if isinstance(value, str) and value.startswith("eval(") and value.endswith(")"):
                kwargs[key] = eval(value[5:-1])
            else:
                kwargs[key] = value
    else:
        raise Exception(f"Invalid params type: {type(params)}")

    return kwargs

def build_dag(dag_conf: dict[str, Any]) -> Executor:
    worker_num = dag_conf.get("worker_num", 1)
    dag = DAG()
    task_dict: dict[str, tuple[TaskBase, dict[str, Any]]] = {}
    for task_conf in dag_conf["tasks"]:
        task_type = task_conf.get["type"]
        task_cls = get_class(TaskBase, task_type)
        if task_cls is None:
            raise Exception(f"Task type {task_type} not found")
        task_config = TaskConfig(**task_conf.get("config", {}))
        func = get_callable_func(task_conf["func"])
        task_func = partial(func, **get_params(task_conf.get("params", {})))
        task_params = task_conf.get("task_params", {})
        task_name = task_conf["name"]
        task_instance = task_cls(task_func, task_config, task_name, **task_params)
        task_dict[task_name] = (task_instance, task_conf)

    for task_instance, task_conf in task_dict.values():
        dependencies = []
        for upstream_task_name in task_conf.get("upstream", []):
            upstream_task_instance, _ = task_dict[upstream_task_name]
            dependencies.append(upstream_task_instance)
        dag.add_task(task_instance, dependencies)
    dag.build()

    executor = Executor(dag, worker_num)
    return executor
