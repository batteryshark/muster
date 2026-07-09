"""Ledger: the per-run SQLite spine, muster's durable coverage and resume oracle.

Every graph node reads ledger state on entry and records events and rows on exit;
completion is gated on ledger coverage, not on model output. This class owns the
generic spine (runs, artifacts, the work queue and its dependency DAG, per-item notes,
graph events, reports, and the durable question/answer channel) plus the termination
oracle (`assess`). An application registers its own tables and the tables to wipe on
resume through the constructor:

    class MyStore(Ledger):
        def __init__(self, db_path):
            super().__init__(db_path,
                             extra_schema=MY_SCHEMA_SQL,
                             reset_tables=("facts", "findings"))
            ...  # add record and count methods for those tables

muster never imports the application; the application subclasses it, so all of its
code works through one ledger object.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = "0.1.0"

# Sentinel distinguishing "leave this field untouched" from "set it to NULL". Used by
# set_work_status(state_label=...) so a status change can leave the domain label alone
# by default, yet still clear it (state_label=None) when a caller explicitly asks.
_UNSET: object = object()

# The canonical work-item state sets, single-sourced so every query agrees on them.
# Terminal states end an item's life; the loop is done when nothing non-terminal remains.
TERMINAL_STATES: tuple[str, ...] = ("done", "failed", "needs_review", "deferred", "blocked")
# Leasable states: an item the drain loop may claim, dependencies permitting. needs_evidence
# is deliberately here — it is a non-terminal "come back with more" state, so it re-leases
# (and each re-lease bumps attempts, which is how thrash surfaces).
ACTIONABLE_STATES: tuple[str, ...] = ("queued", "needs_evidence")

# Reasons are mandatory for these deliberate terminal decisions (the design contract:
# you may defer or block, but only with a recorded rationale).
_REASON_REQUIRED: tuple[str, ...] = ("deferred", "blocked")

# Termination-oracle verdicts (Ledger.assess).
CONTINUE = "continue"
COMPLETE = "complete"
DEADLOCKED = "deadlocked"
MAX_ITERATIONS = "max_iterations"

# SQL list fragments built from the constants above (never user input, safe to inline).
_TERMINAL_SQL = ", ".join(f"'{s}'" for s in TERMINAL_STATES)
_ACTIONABLE_SQL = ", ".join(f"'{s}'" for s in ACTIONABLE_STATES)

_CORE_SCHEMA_PATH = Path(__file__).with_name("schema.sql")
# Read once at import: the spine schema is a small data file, but a Ledger is constructed
# per run (and once per run by a project-wide rollup), so don't re-read it on every open.
_CORE_SCHEMA = _CORE_SCHEMA_PATH.read_text(encoding="utf-8")

# Spine tables a re-drive regenerates. The work queue and its notes are NOT here: they are
# the durable record of what work exists, deduped by stable_key, so a re-drive re-enqueues
# idempotently (finished items stay finished) and externally-added items survive to be
# picked up. Answers survive too (injected before reset so a re-asked question finds them).
_CORE_RESET_TABLES = ("artifacts", "graph_events", "reports", "questions")


def utcnow() -> str:
    """UTC timestamp in ISO-8601, the ledger's created_at/updated_at format. Public so a
    Ledger subclass can timestamp its own rows consistently with the spine (part of the
    subclassing contract, not a private reach-in)."""
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def stable_key(*parts: str) -> str:
    """Content-derived work-item key; never derived from list order."""
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class Termination:
    """The verdict from one loop pass (see Ledger.assess). `verdict` is one of CONTINUE,
    COMPLETE, DEADLOCKED, MAX_ITERATIONS; the counts and message are for logging/UI."""
    verdict: str
    iteration: int
    total: int
    terminal: int
    pending: int
    actionable: int
    idle_rounds: int
    message: str


class Ledger:
    def __init__(self, db_path: str | Path, *, extra_schema: str = "",
                 reset_tables: Iterable[str] = ()):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        # A consumer's domain schema (applied after the core spine) and the domain
        # tables to wipe on resume, on top of the spine's own derived tables.
        self._extra_schema = extra_schema
        self._domain_reset_tables = tuple(reset_tables)
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(_CORE_SCHEMA)
        if self._extra_schema:
            self.conn.executescript(self._extra_schema)
        self.conn.execute(
            "insert or ignore into meta(key, value) values('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # --- runs -------------------------------------------------------------
    def create_run(self, *, run_id, project_id, target_path, target_root,
                   storage_root, run_dir, config_json, max_iterations: int = 50) -> None:
        """Create the run, or mark an existing one running again (resume). On resume the
        loop-oracle state (iteration/idle counters, the ceiling) is deliberately left
        untouched so a re-driven run keeps counting toward its ceiling."""
        now = utcnow()
        self.conn.execute(
            """insert into runs
               (id, project_id, target_path, target_root, storage_root, run_dir,
                status, created_at, updated_at, config_json, max_iterations)
               values (?,?,?,?,?,?,?,?,?,?,?)
               on conflict(id) do update set
                 status='running', updated_at=excluded.updated_at,
                 config_json=excluded.config_json""",
            (run_id, project_id, str(target_path), str(target_root),
             str(storage_root), str(run_dir), "running", now, now, config_json,
             max_iterations),
        )
        self.conn.commit()

    def finish_run(self, run_id: str, status: str, *, coverage: dict | None = None,
                   summary: dict | None = None, error: str | None = None) -> None:
        now = utcnow()
        self.conn.execute(
            """update runs set status=?, updated_at=?, completed_at=?,
                              coverage_json=?, summary_json=?, error=? where id=?""",
            (status, now, now,
             json.dumps(coverage) if coverage is not None else None,
             json.dumps(summary) if summary is not None else None,
             error, run_id),
        )
        self.conn.commit()

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        cur = self.conn.execute("select * from runs where id=?", (run_id,))
        return cur.fetchone()

    # --- artifacts --------------------------------------------------------
    def add_artifact(self, *, run_id, kind, path, logical_path, origin,
                     sha256=None, size_bytes=None, language=None,
                     media_type=None, metadata: dict | None = None) -> str:
        aid = new_id("art")
        self.conn.execute(
            """insert into artifacts
               (id, run_id, kind, path, logical_path, sha256, size_bytes,
                media_type, language, origin, metadata_json, created_at)
               values (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (aid, run_id, kind, str(path), logical_path, sha256, size_bytes,
             media_type, language, origin, json.dumps(metadata or {}), utcnow()),
        )
        self.conn.commit()
        return aid

    def count_artifacts(self, run_id: str, kind: str | None = None) -> int:
        if kind is None:
            cur = self.conn.execute("select count(*) c from artifacts where run_id=?", (run_id,))
        else:
            cur = self.conn.execute(
                "select count(*) c from artifacts where run_id=? and kind=?", (run_id, kind))
        return cur.fetchone()["c"]

    # --- work items -------------------------------------------------------
    def enqueue(self, *, run_id, key, target, operation, category, title,
                priority=100, depends_on=None, payload=None, state_label=None,
                evidence=None) -> str:
        """Add a work item (idempotent by stable_key: a re-enqueue of the same key returns
        the existing item unchanged). Every id in depends_on must already exist for this run
        — enqueuing children after their parents means the DAG can never contain a forward
        reference, and therefore never a cycle."""
        deps = list(depends_on or [])
        for dep_id in deps:
            if self.conn.execute("select 1 from work_items where id=? and run_id=?",
                                 (dep_id, run_id)).fetchone() is None:
                raise ValueError(
                    f"depends_on references unknown work item {dep_id!r} in run {run_id!r}.")
        now = utcnow()
        wid = new_id("wi")
        cur = self.conn.execute(
            """insert or ignore into work_items
               (id, run_id, stable_key, target, operation, category, title,
                status, state_label, evidence, priority, depends_on_json, payload_json,
                created_at, updated_at)
               values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (wid, run_id, key, target, operation, category, title, "queued",
             state_label, evidence, priority, json.dumps(deps),
             json.dumps(payload or {}), now, now),
        )
        self.conn.commit()
        if cur.rowcount == 0:  # already present (stable key dedup)
            row = self.conn.execute(
                "select id from work_items where run_id=? and stable_key=?",
                (run_id, key)).fetchone()
            return row["id"]
        return wid

    def get_work_item(self, wid: str) -> sqlite3.Row | None:
        return self.conn.execute("select * from work_items where id=?", (wid,)).fetchone()

    def set_work_status(self, wid: str, status: str, *, result: dict | None = None,
                        error: str | None = None, evidence: str | None = None,
                        state_label: object = _UNSET) -> None:
        """Move an item to a new status. deferred/blocked require a reason (pass error=...)
        — they are deliberate decisions, not silent drops. state_label is touched only when
        the caller passes it (pass None to clear), so a plain status change leaves the
        orthogonal domain label alone. Convenience verbs: resolve/defer/block/need_evidence."""
        if status in _REASON_REQUIRED and not error:
            raise ValueError(f"status {status!r} requires a reason (pass error=...).")
        now = utcnow()
        terminal = status in TERMINAL_STATES
        # Build the SET clause from internal constants only (never user input).
        sets = ["status=?", "updated_at=?", "terminal_at=?", "result_json=?", "error=?"]
        params: list = [status, now, now if terminal else None,
                        json.dumps(result) if result is not None else None, error]
        if evidence is not None:
            sets.append("evidence=?")
            params.append(evidence)
        if state_label is not _UNSET:
            sets.append("state_label=?")
            params.append(state_label)
        params.append(wid)
        self.conn.execute(f"update work_items set {', '.join(sets)} where id=?", params)
        self.conn.commit()

    def resolve(self, wid: str, *, result: dict | None = None, evidence: str | None = None,
                state_label: object = _UNSET) -> None:
        """Terminal success: mark done with an optional result and supporting evidence."""
        self.set_work_status(wid, "done", result=result, evidence=evidence,
                             state_label=state_label)

    def defer(self, wid: str, reason: str, *, result: dict | None = None,
              state_label: object = _UNSET) -> None:
        """Terminal deliberate deferral — requires a rationale."""
        self.set_work_status(wid, "deferred", result=result, error=reason,
                             state_label=state_label)

    def block(self, wid: str, reason: str, *, result: dict | None = None,
              state_label: object = _UNSET) -> None:
        """Terminal block (needs external input) — requires a reason."""
        self.set_work_status(wid, "blocked", result=result, error=reason,
                             state_label=state_label)

    def need_evidence(self, wid: str, *, note: str | None = None,
                      state_label: object = _UNSET) -> None:
        """Non-terminal: send an item back to be re-leased with more evidence. The optional
        note explains what is missing (kept in result_json for the next handler)."""
        self.set_work_status(wid, "needs_evidence",
                             result=({"note": note} if note else None),
                             state_label=state_label)

    def set_label(self, wid: str, label: str | None) -> None:
        """Set (or clear, with ``None``) an item's domain ``state_label``. Deliberately
        orthogonal to ``status``: this never changes the loop-driver state or
        ``terminal_at`` — it records the domain's own reading of the item (e.g.
        ``evaluating`` → ``reviewed`` → ``triaged`` → ``confirmed``), which the drain loop
        ignores. Use this when the label moves independently of the work status; pass it to
        ``enqueue``/``set_work_status`` instead when it rides along with a status change."""
        self.conn.execute(
            "update work_items set state_label=?, updated_at=? where id=?",
            (label, utcnow(), wid))
        self.conn.commit()

    # --- per-item notes (append-only audit) -------------------------------
    def add_note(self, run_id: str, wid: str, text: str, *, iteration: int = 0) -> str:
        """Append an audit note to a work item without mutating its result."""
        nid = new_id("note")
        self.conn.execute(
            "insert into work_notes (id, run_id, work_item_id, iteration, text, created_at) "
            "values (?,?,?,?,?,?)", (nid, run_id, wid, iteration, text, utcnow()))
        self.conn.commit()
        return nid

    def get_notes(self, wid: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "select * from work_notes where work_item_id=? order by created_at",
            (wid,)).fetchall()

    # --- counts / coverage ------------------------------------------------
    def work_status_counts(self, run_id: str) -> dict[str, int]:
        cur = self.conn.execute(
            "select status, count(*) c from work_items where run_id=? group by status", (run_id,))
        return {r["status"]: r["c"] for r in cur.fetchall()}

    def work_label_counts(self, run_id: str) -> dict[str, int]:
        """Count work items grouped by domain ``state_label`` (unlabelled items excluded).
        The label-axis parallel to ``work_status_counts``: lets a report or UI break a run
        down by the domain's own reading (e.g. how many items are ``triaged`` vs
        ``confirmed``) independently of the loop-driver status."""
        cur = self.conn.execute(
            "select state_label, count(*) c from work_items "
            "where run_id=? and state_label is not null group by state_label", (run_id,))
        return {r["state_label"]: r["c"] for r in cur.fetchall()}

    def count_work_items(self, run_id: str, *, operation: str | None = None,
                         status: str | None = None, state_label: str | None = None) -> int:
        """Count work items, optionally filtered by operation, status, and/or
        ``state_label``. Lets a report count one operation's blind spots without
        conflating them with other items that share a status (e.g. binaries vs
        network-blocked fetches, both 'blocked'), or slice by the orthogonal domain
        label (e.g. how many 'confirmed' items regardless of status)."""
        sql = "select count(*) c from work_items where run_id=?"
        params: list = [run_id]
        if operation is not None:
            sql += " and operation=?"
            params.append(operation)
        if status is not None:
            sql += " and status=?"
            params.append(status)
        if state_label is not None:
            sql += " and state_label=?"
            params.append(state_label)
        return self.conn.execute(sql, params).fetchone()["c"]

    def pending_count(self, run_id: str) -> int:
        """Non-terminal items — everything the loop still owes a decision on (includes
        leased and dependency-blocked items, unlike actionable_count)."""
        return self.conn.execute(
            f"select count(*) c from work_items where run_id=? and status not in ({_TERMINAL_SQL})",
            (run_id,)).fetchone()["c"]

    def pending_items(self, run_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            f"select * from work_items where run_id=? and status not in ({_TERMINAL_SQL}) "
            "order by priority desc, created_at asc", (run_id,)).fetchall()

    # --- dependency-aware queue -------------------------------------------
    def _terminal_ids(self, run_id: str) -> set[str]:
        return {r["id"] for r in self.conn.execute(
            f"select id from work_items where run_id=? and status in ({_TERMINAL_SQL})",
            (run_id,))}

    def _actionable_rows(self, run_id: str) -> list[sqlite3.Row]:
        """Leasable items whose every dependency is terminal, in lease order (highest
        priority, then oldest). Dependency satisfaction is resolved in Python against a
        terminal-id set rather than a correlated json_each subquery — robust across SQLite
        builds and easy to reason about; run scale is modest."""
        terminal = self._terminal_ids(run_id)
        rows = self.conn.execute(
            f"select * from work_items where run_id=? and status in ({_ACTIONABLE_SQL}) "
            "order by priority desc, created_at asc", (run_id,)).fetchall()
        return [r for r in rows if all(d in terminal for d in json.loads(r["depends_on_json"]))]

    def actionable_count(self, run_id: str) -> int:
        """How many items could be leased right now — non-terminal, in a leasable state, and
        with all dependencies terminal. Zero-with-pending is the deadlock signal."""
        return len(self._actionable_rows(run_id))

    def next_actionable(self, run_id: str) -> sqlite3.Row | None:
        """The item lease_next_actionable would claim next, without claiming it."""
        rows = self._actionable_rows(run_id)
        return rows[0] if rows else None

    def lease_next_actionable(self, run_id: str):
        """Claim the next actionable work item (mark it `leased`, bump attempts) and return
        its row, or None if nothing is actionable. Respects the dependency DAG: an item whose
        dependencies are not all terminal is never leased. This is the drain loop's lease
        step; a handler must then drive the item to a terminal status."""
        rows = self._actionable_rows(run_id)
        if not rows:
            return None
        row = rows[0]
        ts = utcnow()
        self.conn.execute(
            "update work_items set status='leased', attempts=attempts+1, updated_at=? where id=?",
            (ts, row["id"]))
        self.conn.commit()
        # Return the post-lease view built in memory from the row already fetched (no second
        # query). The sole caller re-wraps this in a dict.
        leased = dict(row)
        leased["status"] = "leased"
        leased["attempts"] = row["attempts"] + 1
        leased["updated_at"] = ts
        return leased

    def requeue_stale_leases(self, run_id: str) -> int:
        """Flip any `leased` items back to `queued` (crash recovery). A drive that died
        mid-dispatch leaves items leased; since the durable queue now survives resume, the
        resuming drive must reclaim them or they would strand. Returns the count reclaimed."""
        cur = self.conn.execute(
            "update work_items set status='queued', updated_at=? "
            "where run_id=? and status='leased'", (utcnow(), run_id))
        self.conn.commit()
        return cur.rowcount

    # --- termination oracle ----------------------------------------------
    def assess(self, run_id: str, *, idle_limit: int = 3) -> Termination:
        """The termination oracle — the loop driver calls this once per pass to decide
        whether to continue, and the model never does. Bumps the persistent loop-pass
        counter, runs idle detection, and returns a verdict:

          COMPLETE        no non-terminal work remains — full coverage.
          MAX_ITERATIONS  the loop-pass ceiling was reached — safety net.
          DEADLOCKED      work remains but nothing is actionable (dependencies unsatisfied
                          or only stale leases remain), or no progress for idle_limit passes
                          — a human should look, this is not completion.
          CONTINUE        actionable work remains and progress is being made.
        """
        run = self.get_run(run_id)
        if run is None:
            raise ValueError(f"no run {run_id!r}")
        iteration = run["iteration"] + 1
        max_it = run["max_iterations"]
        counts = self.work_status_counts(run_id)
        total = sum(counts.values())
        terminal = sum(counts.get(s, 0) for s in TERMINAL_STATES)
        pending = total - terminal
        actionable = self.actionable_count(run_id)
        made_progress = (total != run["items_at_last_check"]
                         or terminal != run["terminal_at_last_check"])
        idle = 0 if made_progress else run["idle_rounds"] + 1
        self.conn.execute(
            "update runs set iteration=?, idle_rounds=?, items_at_last_check=?, "
            "terminal_at_last_check=?, updated_at=? where id=?",
            (iteration, idle, total, terminal, utcnow(), run_id))
        self.conn.commit()

        if pending == 0:
            return Termination(COMPLETE, iteration, total, terminal, pending, actionable, idle,
                               f"COMPLETE: {terminal}/{total} terminal, no pending work.")
        if iteration > max_it:
            return Termination(MAX_ITERATIONS, iteration, total, terminal, pending, actionable,
                               idle, f"MAX_ITERATIONS: pass {iteration} exceeds ceiling "
                                     f"{max_it}; {terminal}/{total} terminal, {pending} pending.")
        if actionable == 0:
            return Termination(DEADLOCKED, iteration, total, terminal, pending, actionable, idle,
                               f"DEADLOCKED: {pending} pending but none actionable "
                               f"(dependencies unsatisfied or only stale leases remain).")
        if idle >= idle_limit:
            return Termination(DEADLOCKED, iteration, total, terminal, pending, actionable, idle,
                               f"DEADLOCKED: no progress for {idle} passes; {pending} pending.")
        return Termination(CONTINUE, iteration, total, terminal, pending, actionable, idle,
                           f"CONTINUE: {terminal}/{total} terminal, {actionable} actionable, "
                           f"{pending} pending (pass {iteration}).")

    # --- resume (derived-state reset) ------------------------------------
    def delete_run_rows(self, run_id: str, *tables: str) -> None:
        """Delete this run's rows from the named tables. Public so a consumer subclass can
        do targeted domain resets (e.g. re-record observations over a recomposed union).
        Table names are internal constants, never user input."""
        for table in tables:
            self.conn.execute(f"delete from {table} where run_id=?", (run_id,))
        self.conn.commit()

    def reset_run_derived(self, run_id: str) -> None:
        """Clear everything a re-drive regenerates, keeping the run row, the durable work
        queue, and injected answers. Resume starts derived tables from a clean slate so nodes
        re-record without duplicating; anything worth reusing (the work queue — deduped by
        stable_key so re-enqueue is idempotent — fetched bytes on disk, and answers) is not
        touched. Wipes the spine's derived tables plus the consumer's registered domain
        tables. Pair with requeue_stale_leases to reclaim items a crash left leased."""
        self.delete_run_rows(run_id, *_CORE_RESET_TABLES, *self._domain_reset_tables)

    # --- questions / answers (durable human-in-the-loop) -----------------
    def ask_question(self, run_id: str, *, qid: str, node: str, kind: str, prompt: str,
                     options: list | None = None) -> str:
        """Record a pending question (idempotent by content-addressed id)."""
        self.conn.execute(
            "insert or ignore into questions (id, run_id, node, kind, prompt, options_json, created_at) "
            "values (?,?,?,?,?,?,?)",
            (qid, run_id, node, kind, prompt, json.dumps(options or []), utcnow()))
        self.conn.commit()
        return qid

    def record_answer(self, run_id: str, qid: str, answer: str) -> None:
        """Persist an answer (survives resume's reset so the re-asked question finds it)."""
        self.conn.execute(
            "insert or replace into answers (id, run_id, answer, answered_at) values (?,?,?,?)",
            (qid, run_id, str(answer), utcnow()))
        self.conn.commit()

    def get_answer(self, run_id: str, qid: str) -> str | None:
        row = self.conn.execute("select answer from answers where id=?", (qid,)).fetchone()
        return row["answer"] if row else None

    def pending_questions(self, run_id: str) -> list[dict]:
        """Questions asked this run that have no answer yet: what the caller must
        resolve before the run can complete."""
        rows = self.conn.execute(
            "select q.id, q.node, q.kind, q.prompt, q.options_json from questions q "
            "left join answers a on a.id=q.id where q.run_id=? and a.id is null "
            "order by q.created_at", (run_id,)).fetchall()
        return [{"id": r["id"], "node": r["node"], "kind": r["kind"], "prompt": r["prompt"],
                 "options": json.loads(r["options_json"])} for r in rows]

    def count_pending_questions(self, run_id: str) -> int:
        return self.conn.execute(
            "select count(*) c from questions q left join answers a on a.id=q.id "
            "where q.run_id=? and a.id is null", (run_id,)).fetchone()["c"]

    # --- events / reports -------------------------------------------------
    def event(self, run_id: str, node: str, event: str, payload: dict | None = None) -> None:
        self.conn.execute(
            """insert into graph_events (id, run_id, node, event, payload_json, created_at)
               values (?,?,?,?,?,?)""",
            (new_id("evt"), run_id, node, event, json.dumps(payload or {}), utcnow()),
        )
        self.conn.commit()

    def add_report(self, run_id: str, fmt: str, path: str | Path) -> None:
        p = Path(path)
        digest = hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None
        self.conn.execute(
            """insert into reports (id, run_id, format, path, sha256, created_at)
               values (?,?,?,?,?,?)""",
            (new_id("rep"), run_id, fmt, str(path), digest, utcnow()),
        )
        self.conn.commit()

    def coverage(self, run_id: str) -> dict:
        counts = self.work_status_counts(run_id)
        total = sum(counts.values())
        terminal = sum(counts.get(s, 0) for s in TERMINAL_STATES)
        return {
            "workItemsTotal": total,
            "done": counts.get("done", 0),
            "failed": counts.get("failed", 0),
            "blocked": counts.get("blocked", 0),
            "needsReview": counts.get("needs_review", 0),
            "needsEvidence": counts.get("needs_evidence", 0),
            "deferred": counts.get("deferred", 0),
            "queued": counts.get("queued", 0),
            "leased": counts.get("leased", 0),
            "terminal": terminal,
            "pending": total - terminal,
            "actionable": self.actionable_count(run_id),
        }
