"""Graph scaffolding: the generic phase-graph spine shared by every muster consumer.

muster does not own the graph *nodes* (pydantic-graph nodes are concrete-typed and
edge-inferred from their return annotations, so they necessarily reference a
consumer's own phases). What muster owns is the *mechanism* those nodes run on:

- ``GraphState`` / ``GraphDeps``: the small transient run context and the heavy-object
  bag a consumer extends (both ``kw_only`` so a subclass can add required fields without
  the "non-default follows default" dataclass trap).
- ``atomic_write``: crash-safe file replace, for reports and run.json.
- ``enter``: phase-node entry that bumps the iteration counter and records an ``enter``
  event.
- ``ask``: the durable-question pattern. A node that can't decide records a
  content-addressed question and keeps going; the run finishes ``needs_input`` and
  whoever is driving it answers and resumes. Never a blocking wait.
- ``WorkDispatcher``: the work-queue drain mechanism, an ``operation -> handler``
  registry plus the lease/dispatch step. A consumer's queue-drain node self-loops
  calling it; new operations plug in as handlers without touching the graph.

All the helpers are duck-typed on a pydantic-graph ``GraphRunContext`` whose ``state``
subclasses ``GraphState`` and ``deps`` subclasses ``GraphDeps``.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from muster.ledger import Ledger, stable_key
from muster.paths import RunPaths


@dataclass(kw_only=True)
class GraphState:
    """Small, transient run context (identity + counters). Consumers subclass to add
    their own transient fields; the heavy objects live in ``GraphDeps``, not here."""
    run_id: str
    project_id: str
    run_dir: Path
    db_path: Path
    target_path: Path
    iteration: int = 0
    max_iterations: int = 50


@dataclass(kw_only=True)
class GraphDeps:
    """Heavy per-run objects (the ledger, the run-dir paths) plus generic run flags.
    Consumers subclass to add config, a toolchain, model handles, and a ``model_for``
    role resolver. ``scratch`` is inter-node scratch space; ``resume`` is True when the
    run is being re-driven from its ledger."""
    ledger: Ledger
    paths: RunPaths
    resume: bool = False
    scratch: dict[str, Any] = field(default_factory=dict)


def atomic_write(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` via a temp file + ``os.replace`` so a crash mid-write
    never leaves a half-written report."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def enter(ctx, name: str) -> None:
    """Phase-node entry: bump the iteration counter and record an ``enter`` event so the
    ledger's ``graph_events`` is a queryable trace of which phases ran, in order."""
    ctx.state.iteration += 1
    ctx.deps.ledger.event(ctx.state.run_id, name, "enter")


def ask(ctx, *, node: str, kind: str, prompt: str, options=None) -> str | None:
    """Ask a durable question. Never a blocking wait. Returns the answer if one was
    injected (a prior resume), else records the question as pending and returns None; the
    caller handles None by deferring that path. The run finishes ``needs_input`` while any
    question is unanswered, and whoever is driving it answers and resumes. Idempotent: the
    id is content-addressed, so the same question maps to the same answer across re-drives."""
    qid = stable_key(prompt, kind, node)
    answer = ctx.deps.ledger.get_answer(ctx.state.run_id, qid)
    if answer is not None:
        return answer
    ctx.deps.ledger.ask_question(ctx.state.run_id, qid=qid, node=node, kind=kind,
                                 prompt=prompt, options=options)
    return None


WorkHandler = Callable[[Any, dict], None]


class WorkDispatcher:
    """The generic work-queue drain mechanism: a registry of ``operation -> handler``
    plus the lease/dispatch step. A consumer's queue-drain node self-loops calling
    ``run_one`` (or leases itself and calls ``dispatch``); each handler drives its leased
    item to a terminal status and may enqueue follow-ups. An unknown operation is deferred
    with a note (never left leased, which would stall the loop), and a handler that raises
    fails its item rather than the run. New operations plug in as handlers without touching
    the graph."""

    def __init__(self, handlers: dict[str, WorkHandler] | None = None, *,
                 node_label: str = "WorkDispatcher"):
        self._handlers: dict[str, WorkHandler] = dict(handlers or {})
        # graph_events node name used when a handler raises. Defaults to a domain-neutral
        # label; a consumer passes its own drain-node name so the trace attributes the
        # failure to the node that was actually running (not a hardcoded consumer name).
        self._node_label = node_label

    def register(self, operation: str, handler: WorkHandler) -> "WorkDispatcher":
        self._handlers[operation] = handler
        return self

    def dispatch(self, ctx, item: dict) -> None:
        """Route one leased work item to its handler."""
        op = item.get("operation")
        handler = self._handlers.get(op)
        try:
            if handler is not None:
                handler(ctx, item)
            else:
                ctx.deps.ledger.set_work_status(item["id"], "deferred",
                    error=f"No queue handler for operation {op!r}.")
        except Exception as exc:  # a handler bug must not stall the loop
            ctx.deps.ledger.set_work_status(item["id"], "failed", error=f"{type(exc).__name__}: {exc}")
            ctx.deps.ledger.event(ctx.state.run_id, self._node_label, "error",
                                  {"operation": op, "error": repr(exc)})

    def run_one(self, ctx) -> dict | None:
        """Lease the next actionable item and dispatch it. Returns the processed item, or
        None if the queue is drained."""
        item = ctx.deps.ledger.lease_next_actionable(ctx.state.run_id)
        if item is None:
            return None
        item = dict(item)
        self.dispatch(ctx, item)
        return item


AsyncWorkHandler = Callable[[Any, dict], Awaitable[None]]


class AsyncWorkDispatcher:
    """Async sibling of :class:`WorkDispatcher`, for agentic / IO-bound handlers.

    Handlers are coroutines (``async def handle(ctx, item)``), so they can ``await
    agent.run(...)`` directly on the graph's event loop — the natural way to make a bounded
    model step a first-class work-queue operation. ``drain`` works the queue with bounded
    concurrency: several items in flight at once, which is what a batch of model calls wants.

    Concurrency model — deliberately single-threaded asyncio, not OS threads. Leasing is
    synchronous with no ``await`` inside, so two coroutines never claim the same item; and
    each ledger write completes before control yields, so muster's one (thread-bound) SQLite
    connection stays safe with no locks, no pool, no per-thread connections. The only rule
    for a handler: don't hold a ledger transaction open across an ``await`` — the spine's
    methods each commit, so using them is always fine. (Parallelizing CPU-bound *sync*
    handlers across OS threads is a different model — one Ledger per thread plus an
    atomic-claim lease — that IO-bound agentic work does not need, and this class does not
    attempt.)"""

    def __init__(self, handlers: dict[str, AsyncWorkHandler] | None = None, *,
                 node_label: str = "AsyncWorkDispatcher"):
        self._handlers: dict[str, AsyncWorkHandler] = dict(handlers or {})
        self._node_label = node_label

    def register(self, operation: str, handler: AsyncWorkHandler) -> "AsyncWorkDispatcher":
        self._handlers[operation] = handler
        return self

    async def dispatch(self, ctx, item: dict) -> None:
        """Route one leased work item to its async handler. Mirrors WorkDispatcher.dispatch:
        an unknown operation is deferred (never left leased), and a handler that raises fails
        its item rather than the whole drain."""
        op = item.get("operation")
        handler = self._handlers.get(op)
        try:
            if handler is not None:
                await handler(ctx, item)
            else:
                ctx.deps.ledger.set_work_status(item["id"], "deferred",
                    error=f"No queue handler for operation {op!r}.")
        except Exception as exc:  # a handler bug must not stall the loop
            ctx.deps.ledger.set_work_status(item["id"], "failed", error=f"{type(exc).__name__}: {exc}")
            ctx.deps.ledger.event(ctx.state.run_id, self._node_label, "error",
                                  {"operation": op, "error": repr(exc)})

    async def run_one(self, ctx) -> dict | None:
        """Lease the next actionable item and dispatch it. Returns the processed item, or
        None if nothing is actionable right now."""
        item = ctx.deps.ledger.lease_next_actionable(ctx.state.run_id)
        if item is None:
            return None
        item = dict(item)
        await self.dispatch(ctx, item)
        return item

    async def drain(self, ctx, *, concurrency: int = 1, max_items: int | None = None) -> int:
        """Drain the actionable queue keeping up to ``concurrency`` handlers in flight,
        leasing a fresh item whenever a slot frees. Because it re-leases as slots free, work
        unblocked mid-drain is picked up in the same call — a dependency going terminal
        unblocks its dependents, and a ``needs_evidence`` item bounces back to be retried.
        Returns the number of items processed; stops when nothing is actionable (or after
        ``max_items`` leases, a runaway backstop)."""
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        processed = 0
        in_flight: set[asyncio.Future] = set()
        while True:
            while (len(in_flight) < concurrency
                   and (max_items is None or processed + len(in_flight) < max_items)):
                item = ctx.deps.ledger.lease_next_actionable(ctx.state.run_id)
                if item is None:
                    break  # nothing actionable right now; drain what's in flight, then re-check
                in_flight.add(asyncio.ensure_future(self.dispatch(ctx, dict(item))))
            if not in_flight:
                break
            done, in_flight = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()  # dispatch swallows handler errors; this surfaces only infra bugs
                processed += 1
        return processed
