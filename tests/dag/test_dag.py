import pytest

from shutils.dag.dag import DAG
from shutils.dag.task import (
    AsyncFunctionTask,
    SinkNode,
    SourceNode,
)


class TestDAG:
    def test_init(self):
        dag = DAG()
        assert len(dag.tasks) == 0
        assert isinstance(dag.in_task, SourceNode)
        assert isinstance(dag.out_task, SinkNode)

    def test_add_task_no_deps(self):
        dag = DAG()
        task = AsyncFunctionTask(None, name="task1")
        dag.add_task(task)
        assert "task1" in dag.tasks
        assert task in dag.start_tasks

    def test_add_task_with_deps(self):
        dag = DAG()
        task1 = AsyncFunctionTask(None, name="task1")
        task2 = AsyncFunctionTask(None, name="task2")
        dag.add_task(task1)
        dag.add_task(task2, [task1])
        assert task1 in task2.upstream_tasks
        assert task2 in task1.downstream_tasks
        assert task2 not in dag.start_tasks

    def test_add_task_single_dep(self):
        dag = DAG()
        task1 = AsyncFunctionTask(None, name="task1")
        task2 = AsyncFunctionTask(None, name="task2")
        dag.add_task(task1)
        dag.add_task(task2, task1)  # Single TaskBase, not list
        assert task1 in task2.upstream_tasks

    def test_build_simple(self):
        dag = DAG()
        task = AsyncFunctionTask(None, name="task1")
        dag.add_task(task)
        dag.build()
        # After build, in_task and out_task are added
        assert dag.in_task.id in dag.tasks
        assert dag.out_task.id in dag.tasks
        # task should have in_task as upstream
        assert dag.in_task in task.upstream_tasks
        # out_task should have task as upstream
        assert task in dag.out_task.upstream_tasks

    def test_build_no_start_task_raises(self):
        dag = DAG()
        with pytest.raises(ValueError, match="No start task"):
            dag.build()

    def test_build_diamond(self):
        """Test diamond DAG: in -> A -> C -> out
                                        B ->
        """
        dag = DAG()
        task_a = AsyncFunctionTask(None, name="A")
        task_b = AsyncFunctionTask(None, name="B")
        task_c = AsyncFunctionTask(None, name="C")
        dag.add_task(task_a)
        dag.add_task(task_b)
        dag.add_task(task_c, [task_a, task_b])
        dag.build()
        assert task_c in task_a.downstream_tasks
        assert task_c in task_b.downstream_tasks

    def test_get_all_downstream_tasks(self):
        dag = DAG()
        task_a = AsyncFunctionTask(None, name="A")
        task_b = AsyncFunctionTask(None, name="B")
        task_c = AsyncFunctionTask(None, name="C")
        dag.add_task(task_a)
        dag.add_task(task_b, [task_a])
        dag.add_task(task_c, [task_b])
        dag.build()

        downstream = dag._get_all_downstream_tasks(task_a)
        assert task_b in downstream
        assert task_c in downstream

    def test_get_all_upstream_tasks(self):
        dag = DAG()
        task_a = AsyncFunctionTask(None, name="A")
        task_b = AsyncFunctionTask(None, name="B")
        task_c = AsyncFunctionTask(None, name="C")
        dag.add_task(task_a)
        dag.add_task(task_b, [task_a])
        dag.add_task(task_c, [task_b])
        dag.build()

        upstream = dag._get_all_upstream_tasks(task_c)
        assert task_a in upstream
        assert task_b in upstream

    def test_get_bypass_tasks(self):
        dag = DAG()
        task_a = AsyncFunctionTask(None, name="A")
        task_b = AsyncFunctionTask(None, name="B")
        task_c = AsyncFunctionTask(None, name="C")
        dag.add_task(task_a)
        dag.add_task(task_b)
        dag.add_task(task_c, [task_a])
        dag.build()

        # B is bypass of A's path
        bypass = dag._get_bypass_tasks(task_a)
        # B should be in bypass since it's neither upstream nor downstream of A
        assert task_b in bypass

    def test_add_self_flag(self):
        dag = DAG()
        task_a = AsyncFunctionTask(None, name="A")
        task_b = AsyncFunctionTask(None, name="B")
        dag.add_task(task_a)
        dag.add_task(task_b, [task_a])
        dag.build()

        downstream = dag._get_all_downstream_tasks(task_a, add_self=True)
        assert task_a in downstream
