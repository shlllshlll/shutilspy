#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: visualizer.py
Author: shlll(shlll7347@gmail.com)
Brief: Static DAG visualization service using Flask and D3.js
"""

import threading
from typing import Optional, Union
try:
    from flask import Flask, render_template, jsonify
except ImportError:
    raise ImportError("Flask is required for the visualizer. Please install shutilspy with 'pip install shutilspy[visualizer]'.")
from .dag import DAG

app = Flask(__name__)
_current_dag: Optional[DAG] = None

def create_dag_graph(dag: DAG):
    """Convert DAG to a graph structure for visualization"""
    nodes = []
    links = []

    # Add nodes
    for task_id, task in dag.tasks.items():
        nodes.append({
            "id": task_id,
            "type": task.__class__.__name__
        })

    # Add links
    for task_id, task in dag.tasks.items():
        for upstream in task.upstream_tasks:
            links.append({
                "source": upstream.id,
                "target": task_id
            })

    return {"nodes": nodes, "links": links}

@app.route('/')
def index():
    return render_template('dag_visualizer.html')

@app.route('/api/dag')
def get_dag():
    if _current_dag is None:
        return jsonify({"error": "No DAG is currently being visualized"}), 400
    return jsonify(create_dag_graph(_current_dag))

def start_visualizer(dag: DAG, host='localhost', port=5000, background: bool = False) -> Union[threading.Thread, None]:
    """Start the visualization server

    Args:
        dag: The DAG instance to visualize
        host: Host to run the server on
        port: Port to run the server on
        background: If True, run the server in a background thread. If False, run in the foreground.

    Returns:
        If background=True, returns the server thread. If background=False, returns None.
    """
    global _current_dag
    _current_dag = dag

    def run_server():
        app.run(host=host, port=port, debug=False, use_reloader=False)

    if background:
        # Start Flask server in a separate thread
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        return server_thread
    else:
        # Run in the foreground
        run_server()
        return None