"""DAG graph definition and task dependency management."""

from collections.abc import Iterable

from .task import SinkNode, SourceNode, TaskBase

__all__ = ["DAG"]


class DAG:
    """Directed Acyclic Graph for managing task dependencies and execution order."""

    def __init__(self):
        """Initialize an empty DAG with source and sink nodes."""
        self.tasks: dict[str, TaskBase] = {}
        self.in_task: SourceNode = SourceNode()
        self.out_task: SinkNode = SinkNode()
        self.end_tasks: set[TaskBase] = set()
        self.start_tasks: set[TaskBase] = set()
        self._task_set: set[TaskBase] = set()
        self._downstream_tasks: dict[TaskBase, set[TaskBase]] = {}
        self._upstream_tasks: dict[TaskBase, set[TaskBase]] = {}

    def add_task(self, task: TaskBase, dependencies: Iterable[TaskBase] | TaskBase = None):
        """Add a task to the DAG with optional upstream dependencies.

        Args:
            task: The task to add.
            dependencies: Upstream tasks that must complete before this task.
        """
        if dependencies is None:
            dependencies = []
        self.tasks[task.id] = task
        if isinstance(dependencies, TaskBase):
            dependencies = [dependencies]
        for dependency in dependencies:
            task.add_upstream(dependency)
        if not dependencies:
            self.start_tasks.add(task)

    def build(self):
        """Build the DAG by connecting source/sink nodes and initializing task sets.

        Raises:
            ValueError: If no start tasks are defined.
        """
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

        # initialize task set
        self._task_set = set(self.tasks.values())

    def __collect_downstream_tasks(self):
        """collect all downstream tasks for each task"""

        # begin from end task to start task
        def collect(cur_task: TaskBase):
            for down_task in cur_task.downstream_tasks:
                if down_task not in self._downstream_tasks:
                    # not all downstream tasks are collected
                    return
            downstream_tasks = (
                set()
                .union(*[self._downstream_tasks[down_task] for down_task in cur_task.downstream_tasks])
                .union(cur_task.downstream_tasks)
            )
            self._downstream_tasks[cur_task] = downstream_tasks
            for up_task in cur_task.upstream_tasks:
                collect(up_task)

        collect(self.out_task)

    def __collect_upstream_tasks(self):
        """collect all upstream tasks for each task"""

        # begin from start task to end task
        def collect(cur_task: TaskBase):
            for up_task in cur_task.upstream_tasks:
                if up_task not in self._upstream_tasks:
                    # not all upstream tasks are collected
                    return
            upstream_tasks = (
                set()
                .union(*[self._upstream_tasks[up_task] for up_task in cur_task.upstream_tasks])
                .union(cur_task.upstream_tasks)
            )
            self._upstream_tasks[cur_task] = upstream_tasks
            for down_task in cur_task.downstream_tasks:
                collect(down_task)

        collect(self.in_task)

    def _get_all_downstream_tasks(self, task: TaskBase | Iterable[TaskBase], add_self: bool = False) -> set[TaskBase]:
        """Get all transitive downstream tasks for a task or set of tasks.

        Args:
            task: A single task or iterable of tasks.
            add_self: Whether to include the input tasks in the result.

        Returns:
            Set of all downstream tasks.
        """
        if not self._downstream_tasks:
            self.__collect_downstream_tasks()

        if isinstance(task, TaskBase):
            task = [task]
        result = set().union(*[self._downstream_tasks[t] for t in task])
        if add_self:
            result = result.union(task)
        return result

    def _get_all_upstream_tasks(self, task: TaskBase | Iterable[TaskBase], add_self: bool = False) -> set[TaskBase]:
        """Get all transitive upstream tasks for a task or set of tasks.

        Args:
            task: A single task or iterable of tasks.
            add_self: Whether to include the input tasks in the result.

        Returns:
            Set of all upstream tasks.
        """
        if not self._upstream_tasks:
            self.__collect_upstream_tasks()

        if isinstance(task, TaskBase):
            task = [task]
        result = set().union(*[self._upstream_tasks[t] for t in task])
        if add_self:
            result = result.union(task)
        return result

    def _get_bypass_tasks(self, task_list: TaskBase | Iterable[TaskBase]) -> set[TaskBase]:
        """Get tasks that are neither upstream nor downstream of the given tasks.

        Args:
            task_list: A single task or iterable of tasks.

        Returns:
            Set of bypass tasks.
        """
        downstream_tasks = self._get_all_downstream_tasks(task_list)
        task_set = set(task_list) if isinstance(task_list, Iterable) else {task_list}
        bypass_tasks = (
            self._task_set - downstream_tasks - self._get_all_upstream_tasks(task_list) - task_set
        )
        return bypass_tasks
