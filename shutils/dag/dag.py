#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: dag.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

from typing import Iterable
from .task import TaskBase, InTask, OutTask


class DAG:
    def __init__(self):
        self.tasks: dict[str, TaskBase] = {}
        self.in_task: InTask = InTask()
        self.out_task: OutTask = OutTask()
        self.end_tasks: set[TaskBase] = set()
        self.start_tasks: set[TaskBase] = set()

    def add_task(self, task: TaskBase, dependencies: Iterable[TaskBase] | TaskBase = []):
        self.tasks[task.id] = task
        if isinstance(dependencies, TaskBase):
            dependencies = [dependencies]
        for dependency in dependencies:
            task.add_upstream(dependency)
        if not dependencies:
            self.start_tasks.add(task)

    def build(self):
        if len(self.start_tasks) == 0:
            raise ValueError("No start task")

        # 添加输入任务
        for task in self.start_tasks:
            self.add_task(task, [self.in_task])

        # 添加输出任务
        for task in list(self.tasks.values()):
            if not task.downstream_tasks:
                self.end_tasks.add(task)
                self.add_task(self.out_task, [task])
        
        # 最后将输入和输出任务添加到tasks列表中
        self.tasks[self.in_task.id] = self.in_task
        self.tasks[self.out_task.id] = self.out_task
