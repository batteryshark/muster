"""A minimal, runnable muster graph with a pydantic-ai (v2) agent step.

This wires the whole spine together on a toy domain — triaging a batch of security
alerts — so you can see how the pieces fit:

    Seed ──▶ Triage (self-loops, draining the queue) ──▶ Summarize ──▶ End

- **The ledger is the source of truth.** `Seed` enqueues one work item per alert plus a
  `correlate` item that ``depends_on`` all of them. The dependency DAG means `correlate`
  is not actionable until every alert is terminal — muster enforces that at lease time.
- **The oracle decides completion, not the model.** Each `Triage` pass calls
  ``ledger.assess()``; it returns CONTINUE while actionable work remains and COMPLETE when
  the queue is drained (or DEADLOCKED / MAX_ITERATIONS otherwise). The graph never guesses.
- **The agent is a bounded, typed step.** One alert in, one validated ``Triage`` verdict
  out. Its verdict maps onto ledger states, and its domain reading is stamped as a
  ``state_label`` — orthogonal to the loop-driver ``status``.
- **needs_evidence is a real retry.** When the agent wants more, the item goes back to
  ``needs_evidence`` (non-terminal, re-leasable); ``attempts`` climbs, and on the second
  pass the handler enriches the prompt and the agent decides.

Run it with no API key — the ``__main__`` block injects a deterministic ``FunctionModel``:

    uv run --extra examples python examples/triage_graph.py

To point it at a real model instead, build the agent with a model string and drop the
override, e.g. ``Agent("anthropic:claude-sonnet-4-5", output_type=Triage, ...)``.

Note on threading: the model call runs via ``asyncio.to_thread`` because ``run_sync``
cannot execute on the graph's own event loop, and because the ledger's SQLite connection
is thread-bound, every ledger write stays on this (the main) thread. This is the same
split unmask uses for its agentic phases. muster's ``WorkDispatcher`` is the tool for a
registry of purely *synchronous* handlers; here one operation is agentic, so the drain
node dispatches by operation inline.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from pydantic_graph import BaseNode, End, GraphBuilder

from muster import CONTINUE, GraphDeps, GraphState, Ledger, RunPaths, enter, stable_key

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
    """Drain the queue one actionable item per pass, gated by the ledger's oracle. Agentic
    items call the model off the event loop; the sync `correlate` item is pure ledger work."""

    async def run(self, ctx) -> "Drain | Summarize":
        d, s = ctx.deps, ctx.state
        enter(ctx, "Drain")
        verdict = d.ledger.assess(s.run_id)  # the oracle — not the model — decides to stop
        if verdict.verdict != CONTINUE:
            d.ledger.event(s.run_id, "Drain", "note",
                           {"stop": verdict.verdict, "message": verdict.message})
            return Summarize()
        item = d.ledger.lease_next_actionable(s.run_id)  # respects the dependency DAG
        if item is None:
            return Summarize()
        if item["operation"] == "triage":
            await _triage_item(ctx, item)
        else:
            _correlate_item(ctx, item)
        return Drain()


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
    # Model call OFF the graph's event loop; every ledger write stays on this thread.
    out: Triage = await asyncio.to_thread(lambda: d.agent.run_sync(prompt).output)

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


def _correlate_item(ctx, item: dict) -> None:
    """Pure ledger work — only reachable once every triage item is terminal (the DAG)."""
    d, s = ctx.deps, ctx.state
    d.ledger.resolve(item["id"],
                     result={"labelBreakdown": d.ledger.work_label_counts(s.run_id)},
                     state_label="reported")


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
