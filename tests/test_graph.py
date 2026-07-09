"""muster.graph: the generic scaffolding and patterns, tested standalone."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from muster import (
    AsyncWorkDispatcher, GraphDeps, GraphState, Ledger, RunPaths, WorkDispatcher,
    ask, atomic_write, enter, stable_key,
)


@pytest.fixture
def ctx(tmp_path):
    """A minimal duck-typed GraphRunContext: the helpers only touch state.run_id/
    iteration and deps.ledger."""
    lg = Ledger(tmp_path / "run.db")
    lg.create_run(run_id="r1", project_id="p1", target_path="/t", target_root="/t",
                  storage_root=str(tmp_path), run_dir=str(tmp_path), config_json="{}")
    c = SimpleNamespace(state=SimpleNamespace(run_id="r1", iteration=0),
                        deps=SimpleNamespace(ledger=lg))
    yield c
    lg.close()


def test_atomic_write_leaves_no_temp(tmp_path):
    p = tmp_path / "out.txt"
    atomic_write(p, "hello")
    assert p.read_text() == "hello"
    assert not (tmp_path / "out.txt.tmp").exists()          # temp renamed away


def test_enter_bumps_iteration_and_records_event(ctx):
    enter(ctx, "Phase1")
    enter(ctx, "Phase2")
    assert ctx.state.iteration == 2
    events = ctx.deps.ledger.conn.execute(
        "select node, event from graph_events where run_id='r1' order by created_at").fetchall()
    assert [e["node"] for e in events] == ["Phase1", "Phase2"]
    assert all(e["event"] == "enter" for e in events)


def test_ask_is_durable_not_blocking(ctx):
    # unanswered → None (caller defers), and the question is now pending
    assert ask(ctx, node="N", kind="consent", prompt="go?", options=["y", "n"]) is None
    assert ctx.deps.ledger.count_pending_questions("r1") == 1
    # inject the answer → the same content-addressed question resolves on re-ask
    ctx.deps.ledger.record_answer("r1", stable_key("go?", "consent", "N"), "y")
    assert ask(ctx, node="N", kind="consent", prompt="go?") == "y"


def _handler_done(seen):
    def h(c, item):
        seen.append(item["target"])
        c.deps.ledger.set_work_status(item["id"], "done")
    return h


def test_dispatcher_run_one_leases_and_dispatches(ctx):
    seen: list[str] = []
    disp = WorkDispatcher().register("probe", _handler_done(seen))
    ctx.deps.ledger.enqueue(run_id="r1", key=stable_key("x"), target="x",
                            operation="probe", category="c", title="t")
    item = disp.run_one(ctx)
    assert item["target"] == "x" and seen == ["x"]
    assert disp.run_one(ctx) is None                        # queue drained


def test_dispatcher_unknown_op_is_deferred_never_stalls(ctx):
    ctx.deps.ledger.enqueue(run_id="r1", key=stable_key("y"), target="y",
                            operation="mystery", category="c", title="t")
    WorkDispatcher().run_one(ctx)                           # no handler registered
    assert ctx.deps.ledger.count_work_items("r1", operation="mystery", status="deferred") == 1


def test_dispatcher_handler_exception_fails_item_not_loop(ctx):
    def boom(c, item):
        raise RuntimeError("kaboom")
    ctx.deps.ledger.enqueue(run_id="r1", key=stable_key("z"), target="z",
                            operation="boom", category="c", title="t")
    WorkDispatcher({"boom": boom}).run_one(ctx)             # exception isolated to the item
    assert ctx.deps.ledger.count_work_items("r1", operation="boom", status="failed") == 1


def test_dispatcher_node_label_attributes_the_error_event(ctx):
    """A consumer passes its drain-node name so a handler failure is attributed to the node
    that actually ran, not a hardcoded consumer name leaked into generic muster."""
    def boom(c, item):
        raise RuntimeError("x")
    ctx.deps.ledger.enqueue(run_id="r1", key=stable_key("z"), target="z",
                            operation="boom", category="c", title="t")
    WorkDispatcher({"boom": boom}, node_label="MyDrainNode").run_one(ctx)
    ev = ctx.deps.ledger.conn.execute(
        "select node from graph_events where run_id='r1' and event='error'").fetchone()
    assert ev["node"] == "MyDrainNode"


def _enqueue(lg, target, *, op="op", deps=None):
    return lg.enqueue(run_id="r1", key=stable_key(target), target=target, operation=op,
                      category="c", title=target, depends_on=deps)


def test_async_dispatcher_run_one_leases_and_dispatches(ctx):
    seen: list[str] = []
    async def h(c, item):
        seen.append(item["target"])
        c.deps.ledger.set_work_status(item["id"], "done")
    disp = AsyncWorkDispatcher().register("op", h)
    _enqueue(ctx.deps.ledger, "x")
    item = asyncio.run(disp.run_one(ctx))
    assert item["target"] == "x" and seen == ["x"]
    assert asyncio.run(disp.run_one(ctx)) is None           # queue drained


def test_async_dispatcher_unknown_op_is_deferred(ctx):
    _enqueue(ctx.deps.ledger, "y", op="mystery")
    asyncio.run(AsyncWorkDispatcher().run_one(ctx))          # no handler registered
    assert ctx.deps.ledger.count_work_items("r1", operation="mystery", status="deferred") == 1


def test_async_dispatcher_handler_exception_fails_item_not_loop(ctx):
    async def boom(c, item):
        raise RuntimeError("kaboom")
    _enqueue(ctx.deps.ledger, "z", op="boom")
    asyncio.run(AsyncWorkDispatcher({"boom": boom}).run_one(ctx))
    assert ctx.deps.ledger.count_work_items("r1", operation="boom", status="failed") == 1


def test_async_dispatcher_drain_runs_concurrently_and_respects_dependencies(ctx):
    """A and B have no deps; C depends on both. With concurrency=2, A and B run at the same
    time (peak in-flight == 2), and C is only leased after both are terminal (the DAG)."""
    lg = ctx.deps.ledger
    order: list[str] = []
    inflight = peak = 0

    async def h(c, item):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        await asyncio.sleep(0.01)                           # a real yield so items overlap
        order.append(item["target"])
        c.deps.ledger.set_work_status(item["id"], "done")
        inflight -= 1

    a = _enqueue(lg, "A")
    b = _enqueue(lg, "B")
    _enqueue(lg, "C", deps=[a, b])
    processed = asyncio.run(AsyncWorkDispatcher({"op": h}).drain(ctx, concurrency=2))
    assert processed == 3
    assert peak == 2                                        # A and B genuinely overlapped
    assert order[-1] == "C" and set(order[:2]) == {"A", "B"}   # C only after its deps
    assert lg.coverage("r1")["pending"] == 0


def test_async_dispatcher_drain_retries_needs_evidence(ctx):
    """A needs_evidence item is non-terminal and re-leases, so one drain() call picks it up
    again; the handler resolves it on the second attempt."""
    lg = ctx.deps.ledger

    async def h(c, item):
        if item["attempts"] < 2:
            c.deps.ledger.need_evidence(item["id"], note="more")
        else:
            c.deps.ledger.set_work_status(item["id"], "done")
    wid = _enqueue(lg, "A")
    processed = asyncio.run(AsyncWorkDispatcher({"op": h}).drain(ctx, concurrency=1))
    assert processed == 2                                   # leased twice (bounce + resolve)
    assert lg.get_work_item(wid)["status"] == "done" and lg.coverage("r1")["pending"] == 0


def test_async_dispatcher_drain_no_double_lease_under_concurrency(ctx):
    lg = ctx.deps.ledger
    leased: list[str] = []

    async def h(c, item):
        leased.append(item["id"])
        await asyncio.sleep(0)
        c.deps.ledger.set_work_status(item["id"], "done")
    for i in range(12):
        _enqueue(lg, f"w{i}")
    asyncio.run(AsyncWorkDispatcher({"op": h}).drain(ctx, concurrency=4))
    assert len(leased) == len(set(leased)) == 12            # each item processed exactly once


def test_bases_are_kw_only_so_a_subclass_can_add_required_fields(tmp_path):
    """GraphState/GraphDeps are kw_only, so a consumer subclass can add a REQUIRED field
    without the 'non-default argument follows default argument' dataclass trap."""
    @dataclass(kw_only=True)
    class MyDeps(GraphDeps):
        config: str                                         # required, after base defaults

    lg = Ledger(tmp_path / "r.db")
    paths = RunPaths(storage_root=tmp_path, project_id="p", run_id="r", run_dir=tmp_path)
    d = MyDeps(ledger=lg, paths=paths, config="strict")
    assert d.config == "strict" and d.resume is False and d.scratch == {}
    lg.close()

    @dataclass(kw_only=True)
    class MyState(GraphState):
        depth: int

    s = MyState(run_id="r", project_id="p", run_dir=tmp_path, db_path=tmp_path / "x",
                target_path=tmp_path, depth=3)
    assert s.depth == 3 and s.iteration == 0
