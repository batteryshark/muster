"""muster: a ledgered investigation runtime.

A durable, coverage-gated, resumable work-graph runtime with bounded adaptive model
steps. muster owns the spine: run identity and on-disk layout, the SQLite ledger, the
graph scaffolding with its work-queue drain, resume, and the durable-question channel.
An application registers its own domain (its tables, work handlers, and coverage
predicate) by composition, so muster never needs to know anything about that domain.
"""

from __future__ import annotations

from muster.graph import (
    AsyncWorkDispatcher,
    GraphDeps,
    GraphState,
    WorkDispatcher,
    ask,
    atomic_write,
    enter,
)
from muster.ledger import (
    ACTIONABLE_STATES,
    COMPLETE,
    CONTINUE,
    DEADLOCKED,
    Ledger,
    MAX_ITERATIONS,
    SCHEMA_VERSION,
    TERMINAL_STATES,
    Termination,
    new_id,
    stable_key,
    utcnow,
)
from muster.paths import (
    RunPaths,
    compute_project_id,
    compute_run_id,
    new_run_paths,
    resolve_run_dir,
)

__all__ = [
    "Ledger", "SCHEMA_VERSION", "new_id", "stable_key", "utcnow",
    "Termination", "TERMINAL_STATES", "ACTIONABLE_STATES",
    "CONTINUE", "COMPLETE", "DEADLOCKED", "MAX_ITERATIONS",
    "GraphState", "GraphDeps", "WorkDispatcher", "AsyncWorkDispatcher",
    "atomic_write", "enter", "ask",
    "RunPaths", "compute_project_id", "compute_run_id", "new_run_paths", "resolve_run_dir",
]
