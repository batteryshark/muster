# examples

Runnable, self-contained examples of wiring an application on top of muster.

## `triage_graph.py` — a graph with a pydantic-ai agent step

A toy investigation — triaging a batch of security alerts — that shows the whole spine
fit together in ~200 lines:

```
Seed ──▶ Drain (self-loops, draining the queue) ──▶ Summarize ──▶ End
```

- A **pydantic-graph** phase graph built on muster's `GraphState` / `GraphDeps`.
- A bounded, typed **pydantic-ai (v2)** agent step — one alert in, one validated `Triage`
  verdict out.
- The **ledger** as the source of truth: `assess()` decides completion (not the model), the
  **dependency DAG** keeps the `correlate` step blocked until every alert is terminal, and
  `needs_evidence` is a real re-leasable retry (watch `attempts` climb).
- Domain verdicts recorded as **`state_label`**, orthogonal to the loop-driver status.

Run it with no API key — a deterministic `FunctionModel` is injected for the demo:

```bash
uv run --extra examples python examples/triage_graph.py
# or:  pip install -e ".[examples]" && python examples/triage_graph.py
```

Expected output:

```
run status: completed
coverage  : 5/5 terminal, 0 pending
labels    : {'confirmed': 2, 'false-positive': 1, 'inconclusive': 1, 'reported': 1}

STATUS        LABEL           TRIES ITEM
done          false-positive  1     Successful login from a new IP address in the same country
done          confirmed       1     powershell -enc <blob> invoking Net.WebClient.DownloadString
done          confirmed       2     Script embeds a large base64 blob assigned to a variable
deferred      inconclusive    2     Periodic beacon-like outbound connection to an unknown host
done          reported        1     Correlate triaged alerts
```

To point it at a real model, build the agent with a model string and drop the demo
override — e.g. `build_agent("anthropic:claude-sonnet-4-5")`.

### Notes

- The agent step uses the async API (`await agent.run(...)`), so it runs on the graph's own
  event loop with no thread and no blocking, and the ledger writes stay on that same single
  thread — which is what muster's thread-bound SQLite connection wants. (`agent.run_sync` is
  for synchronous callers only; it would fail on the running loop.) muster's connection is
  thread-bound by design, which only matters if you offload ledger-touching work to a worker
  thread or want to process items *concurrently* — then use a per-thread connection over the
  (already-enabled) WAL, not a shared one.
- muster's `WorkDispatcher` is the tool for a registry of *synchronous* handlers; here one
  operation is agentic and needs `await`, so the drain node dispatches by operation inline.
- Real applications subclass `Ledger` to register their own domain tables (see the top-level
  README); this example uses the bare spine to stay focused on the wiring.
