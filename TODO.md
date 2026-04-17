# TODO

## DAG Framework

- [ ] Remove the two-level task execution design in Executor, use single-level only to simplify execution logic
- [ ] Integrate partial function functionality into the API to simplify implementation
- [ ] Optimize lock implementation
- [ ] Support nested sub-DAGs
- [ ] Enhanced debugging and visualization: view context/task execution status via web UI, optionally start a web server alongside DAG execution for real-time monitoring
- [ ] Strengthen potential error detection:
  - [ ] Detect issues like modifying return values in fixed-point vs non-fixed-point tasks, parent-child context handling, etc.
