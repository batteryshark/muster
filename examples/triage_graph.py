"""A minimal, runnable muster graph with a pydantic-ai (v2) agent step.

This wires the whole spine together on a toy domain — triaging a batch of security
alerts — so you can see how the pieces fit:

    Seed ──▶ Drain (AsyncWorkDispatcher, concurrent) ──▶ Summarize ──▶ End

- **The ledger is the source of truth.** `Seed` enqueues one work item per alert plus a
  `correlate` item that ``depends_on`` all of them. The dependency DAG means `correlate`
  is not actionable until every alert is terminal — muster enforces that at lease time.
- **Agentic handlers are first-class + concurrent.** `Drain` hands the queue to an
  ``AsyncWorkDispatcher`` that keeps several handlers in flight; the ``triage`` handler
  ``await``s the model, so alerts triage in parallel. One ``drain()`` call clears the run —
  re-leasing as slots free — then ``ledger.assess()`` confirms completion (the oracle, not
  the model: COMPLETE when nothing is pending, else DEADLOCKED / MAX_ITERATIONS).
- **The agent is a bounded, typed step.** One alert in, one validated ``Triage`` verdict
  out. Its verdict maps onto ledger states, and its domain reading is stamped as a
  ``state_label`` — orthogonal to the loop-driver ``status``.
- **needs_evidence is a real retry.** When the agent wants more, the item goes back to
  ``needs_evidence`` (non-terminal, re-leasable); it bounces back in the same ``drain()``
  call, ``attempts`` climbs, and the handler enriches the prompt so the agent can decide.

Run it with no API key — the ``__main__`` block injects a deterministic ``FunctionModel``:

    uv run --extra examples python examples/triage_graph.py

To point it at a real model instead, build the agent with a model string and drop the
override, e.g. ``Agent("anthropic:claude-sonnet-4-5", output_type=Triage, ...)``.

Note on async: the agent step uses the async API (``await agent.run(...)``), so it runs on
the graph's own event loop without a thread and without blocking it; the ledger writes stay
on that same single thread, which is what muster's thread-bound SQLite connection wants.
(``agent.run_sync`` is only for synchronous callers — using it here would fail on the running
loop.) ``AsyncWorkDispatcher`` runs these async handlers with bounded concurrency on that one
event-loop thread; its synchronous sibling ``WorkDispatcher`` is for non-async handlers.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from pydantic_graph import BaseNode, End, GraphBuilder

from muster import (
    AsyncWorkDispatcher, GraphDeps, GraphState, Ledger, RunPaths, enter, stable_key,
)

# ── The bounded model step: one alert in, one typed verdict out ──────────────────

class Triage(BaseModel):
    """The agent's structured output. `verdict` drives the ledger status; `label` is the
    domain's own reading, recorded as a state_label."""
    verdict: Literal["benign", "malicious", "needs_more"]
    label: str
    reason: str


AGENT_INSTRUCTIONS = (
    "You are a security alert triager. You are given ONE alert. Decide, from the alert "
    "text alone, whether it is `benign`, `malicious`, or you `needs_more` evidence to say. "
    "Set `label` to a short disposition (e.g. false-positive, confirmed, enriching) and "
    "give a one-line `reason`. Judge only this alert; do not invent evidence."
)


def build_agent(model=None):
    """An Agent that emits a validated ``Triage``. Pass a pydantic-ai model (a real one, or
    a TestModel/FunctionModel for offline runs); default resolves one from the environment."""
    from pydantic_ai import Agent

    return Agent(model, output_type=Triage, instructions=AGENT_INSTRUCTIONS, retries=2)


# ── Graph state + deps (muster's bases, extended) ───────────────────────────────

@dataclass(kw_only=True)
class TriageState(GraphState):
    """Transient run context — muster's identity/counter fields, unchanged."""


@dataclass(kw_only=True)
class TriageDeps(GraphDeps):
    """Heavy per-run objects. Adds the pydantic-ai agent and the alerts to triage."""
    agent: object = None
    alerts: tuple[str, ...] = ()


# ── Nodes ───────────────────────────────────────────────────────────────────────

@dataclass
class Seed(BaseNode[TriageState, TriageDeps, dict]):
    """Populate the ledger: one `triage` item per alert, plus a `correlate` item that
    depends on all of them (so it only becomes actionable once every alert is terminal)."""

    async def run(self, ctx) -> "Drain":
        d, s = ctx.deps, ctx.state
        enter(ctx, "Seed")
        triage_ids = [
            d.ledger.enqueue(
                run_id=s.run_id, key=stable_key("alert", str(i)), target=f"alert-{i}",
                operation="triage", category="alert", title=alert, payload={"alert": alert})
            for i, alert in enumerate(d.alerts)
        ]
        d.ledger.enqueue(
            run_id=s.run_id, key=stable_key("correlate"), target="batch",
            operation="correlate", category="summary", title="Correlate triaged alerts",
            depends_on=triage_ids)  # the DAG: blocked until every triage item is terminal
        return Drain()


@dataclass
class Drain(BaseNode[TriageState, TriageDeps, dict]):
    """Work the queue with the AsyncWorkDispatcher — up to `concurrency` handlers in flight,
    re-leasing as slots free. One drain() call clears the whole run: alerts triage in
    parallel, needs_evidence items bounce back for a retry, and the dependency-blocked
    `correlate` step runs once the alerts are all terminal. assess() then confirms it."""

    async def run(self, ctx) -> "Summarize":
        d, s = ctx.deps, ctx.state
        enter(ctx, "Drain")
        processed = await _DISPATCHER.drain(ctx, concurrency=4)
        verdict = d.ledger.assess(s.run_id)  # the oracle — not the model — decides completion
        d.ledger.event(s.run_id, "Drain", "note", {"processed": processed,
                       "verdict": verdict.verdict, "message": verdict.message})
        return Summarize()


@dataclass
class Summarize(BaseNode[TriageState, TriageDeps, dict]):
    """Coverage-gated finish: the run is complete only if nothing is pending."""

    async def run(self, ctx) -> End[dict]:
        d, s = ctx.deps, ctx.state
        enter(ctx, "Summarize")
        cov = d.ledger.coverage(s.run_id)
        labels = d.ledger.work_label_counts(s.run_id)
        status = "completed" if cov["pending"] == 0 else "partial"
        d.ledger.finish_run(s.run_id, status, coverage=cov, summary={"labels": labels})
        return End({"status": status, "coverage": cov, "labels": labels})


# ── Handlers ─────────────────────────────────────────────────────────────────────

async def _triage_item(ctx, item: dict) -> None:
    """Run the bounded agent step on one alert and map its verdict onto the ledger."""
    d = ctx.deps
    alert = json.loads(item["payload_json"])["alert"]
    enriched = item["attempts"] >= 2  # a second lease means we already went back for more
    prompt = f"Alert: {alert}"
    if enriched:
        prompt += "\n[enriched] decoded/expanded context has been attached; decide now."
    # The async agent API runs on the graph's own event loop — no thread, no blocking — and
    # the ledger writes below stay on this same (single) thread, which is what muster's
    # thread-bound SQLite connection wants. (Use agent.run_sync only from synchronous code.)
    out: Triage = (await d.agent.run(prompt)).output

    if out.verdict == "needs_more" and not enriched:
        # send it back to be re-leased with more evidence (non-terminal; attempts will climb)
        d.ledger.need_evidence(item["id"], note="insufficient signal; enrich and retry",
                               state_label="enriching")
    elif out.verdict == "malicious":
        d.ledger.resolve(item["id"], result=out.model_dump(), state_label="confirmed")
    elif out.verdict == "benign":
        d.ledger.resolve(item["id"], result=out.model_dump(), state_label="false-positive")
    else:  # still unsure after enrichment → a deliberate deferral (a reason is required)
        d.ledger.defer(item["id"], reason=f"inconclusive after enrichment: {out.reason}",
                       state_label="inconclusive")


async def _correlate_item(ctx, item: dict) -> None:
    """Pure ledger work (no model call) — only reachable once every triage item is terminal
    (the DAG). Async so it plugs into the same dispatcher as the agentic handler."""
    d, s = ctx.deps, ctx.state
    d.ledger.resolve(item["id"],
                     result={"labelBreakdown": d.ledger.work_label_counts(s.run_id)},
                     state_label="reported")


# The work-queue registry: one operation → one async handler. Agentic and synchronous
# handlers coexist here; the dispatcher just awaits each one.
_DISPATCHER = AsyncWorkDispatcher({"triage": _triage_item, "correlate": _correlate_item},
                                  node_label="Drain")


# ── Assembly + run ───────────────────────────────────────────────────────────────

def build_graph():
    """Assemble the phase graph. Edges are inferred from each node's return annotation."""
    g = GraphBuilder(name="triage", state_type=TriageState, deps_type=TriageDeps,
                     input_type=Seed, output_type=dict)
    g.add(
        g.edge_from(g.start_node).to(Seed),
        g.node(Seed),
        g.node(Drain),
        g.node(Summarize),
    )
    return g.build()


def run_triage(alerts, *, agent, run_dir: Path) -> dict:
    """Create a run, drive the graph, and return its End result."""
    paths = RunPaths(storage_root=run_dir.parent, project_id="triage-example",
                     run_id="triage-demo", run_dir=run_dir)
    ledger = Ledger(paths.db_path)
    ledger.create_run(run_id=paths.run_id, project_id=paths.project_id,
                      target_path=str(run_dir), target_root=str(run_dir),
                      storage_root=str(run_dir.parent), run_dir=str(run_dir),
                      config_json="{}", max_iterations=50)
    state = TriageState(run_id=paths.run_id, project_id=paths.project_id, run_dir=run_dir,
                        db_path=paths.db_path, target_path=run_dir, max_iterations=50)
    deps = TriageDeps(ledger=ledger, paths=paths, agent=agent, alerts=tuple(alerts))
    try:
        # pydantic-graph's run_sync returns the End(...) payload directly
        return build_graph().run_sync(inputs=Seed(), state=state, deps=deps)
    finally:
        ledger.close()


# ── Offline demo (deterministic FunctionModel — no API key needed) ───────────────

_ALERTS = (
    "Successful login from a new IP address in the same country",
    "powershell -enc <blob> invoking Net.WebClient.DownloadString",
    "Script embeds a large base64 blob assigned to a variable",
    "Periodic beacon-like outbound connection to an unknown host",
)


def _demo_model():
    """A FunctionModel that classifies from keywords, so the example runs deterministically
    with no network. Real use: swap this for a model string in build_agent()."""
    from pydantic_ai.messages import ModelResponse, ToolCallPart, UserPromptPart
    from pydantic_ai.models.function import FunctionModel

    def classify(messages, info):
        # pull the latest user prompt text
        text = ""
        for m in reversed(messages):
            parts = getattr(m, "parts", [])
            for p in parts:
                if isinstance(p, UserPromptPart):
                    text = p.content if isinstance(p.content, str) else str(p.content)
                    break
            if text:
                break
        t = text.lower()
        if "[enriched]" in t:
            if "base64" in t:
                v, label = "malicious", "confirmed"      # decoded blob turned out to be code
            elif "beacon" in t:
                v, label = "needs_more", "inconclusive"  # netflow still can't decide → deferred
            else:
                v, label = "benign", "false-positive"
        elif "powershell" in t:
            v, label = "malicious", "confirmed"
        elif "base64" in t or "beacon" in t:
            v, label = "needs_more", "enriching"         # ask for more, then retry enriched
        else:
            v, label = "benign", "false-positive"
        args = {"verdict": v, "label": label, "reason": f"demo heuristic for: {text[:48]}"}
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, args)])

    return FunctionModel(classify)


def main() -> None:
    agent = build_agent(_demo_model())  # real use: build_agent("anthropic:claude-sonnet-4-5")
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "run"
        run_dir.mkdir()
        result = run_triage(_ALERTS, agent=agent, run_dir=run_dir)

        # re-open to print the per-item outcome (status + domain label)
        ledger = Ledger(run_dir / "run.db")
        rows = ledger.conn.execute(
            "select title, operation, status, state_label, attempts from work_items "
            "where run_id='triage-demo' order by created_at").fetchall()
        print(f"\nrun status: {result['status']}")
        print(f"coverage  : {result['coverage']['terminal']}/{result['coverage']['workItemsTotal']} "
              f"terminal, {result['coverage']['pending']} pending")
        print(f"labels    : {result['labels']}\n")
        print(f"{'STATUS':<14}{'LABEL':<16}{'TRIES':<6}ITEM")
        for r in rows:
            print(f"{r['status']:<14}{(r['state_label'] or '-'):<16}{r['attempts']:<6}{r['title']}")
        ledger.close()


if __name__ == "__main__":
    main()
