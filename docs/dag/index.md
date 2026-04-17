# DAG Framework

The DAG (Directed Acyclic Graph) module provides a flexible task orchestration framework for building and executing workflow pipelines.

## Key Concepts

- **DAG**: Defines task dependencies and execution order
- **Task**: Units of work (sync or async) that process data
- **Context**: Data carrier that flows between tasks, with built-in key-value storage
- **Executor**: Runs the DAG with configurable workers and concurrency
- **DataWhiteBoard**: Thread-safe data sharing between tasks via the Context

## Architecture

```
Input → SourceNode → Task A → Task C → SinkNode → Output
                    → Task B →
```

Each task receives a Context, processes it, and returns one or more Contexts. The framework handles:
- Task scheduling based on dependencies
- Context routing between tasks
- Concurrent task execution
- Error handling and retry
- Rate limiting
- Graceful shutdown
