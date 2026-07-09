"""muster.Ledger: the generic spine, tested standalone with no domain layered on."""

from __future__ import annotations

import pytest

from muster import Ledger, SCHEMA_VERSION, new_id, stable_key


@pytest.fixture
def ledger(tmp_path):
    lg = Ledger(tmp_path / "run.db")
    lg.create_run(run_id="r1", project_id="p1", target_path="/t", target_root="/t",
                  storage_root=str(tmp_path), run_dir=str(tmp_path), config_json="{}")
    yield lg
    lg.close()


def test_new_id_and_stable_key():
    assert new_id("wi").startswith("wi-")
    assert new_id("wi") != new_id("wi")                     # unique per call
    k = stable_key("a", "b")
    assert k == stable_key("a", "b")                        # content-addressed, deterministic
    assert stable_key("a", "b") != stable_key("b", "a")    # order is part of the content
    assert len(k) == 24


def test_bare_ledger_has_spine_only(tmp_path):
    lg = Ledger(tmp_path / "c.db")
    tables = {r[0] for r in lg.conn.execute(
        "select name from sqlite_master where type='table'").fetchall()}
    assert {"runs", "artifacts", "work_items", "graph_events", "reports",
            "questions", "answers", "meta"} <= tables
    assert "observations" not in tables and "findings" not in tables   # no domain leakage
    assert lg.conn.execute(
        "select value from meta where key='schema_version'").fetchone()[0] == SCHEMA_VERSION
    lg.close()


def test_run_lifecycle(ledger):
    row = ledger.get_run("r1")
    assert row["status"] == "running" and row["project_id"] == "p1"
    ledger.finish_run("r1", "completed", coverage={"x": 1}, summary={"y": 2})
    row = ledger.get_run("r1")
    assert row["status"] == "completed" and row["completed_at"] is not None


def test_enqueue_dedup_by_stable_key(ledger):
    k = stable_key("a", "inventory")
    a = ledger.enqueue(run_id="r1", key=k, target="a", operation="inventory",
                       category="disc", title="t")
    b = ledger.enqueue(run_id="r1", key=k, target="a", operation="inventory",
                       category="disc", title="t")
    assert a == b                                           # same content → same row
    assert ledger.count_work_items("r1") == 1


def test_lease_priority_and_drain(ledger):
    ledger.enqueue(run_id="r1", key=stable_key("lo"), target="lo", operation="op",
                   category="c", title="lo", priority=10)
    ledger.enqueue(run_id="r1", key=stable_key("hi"), target="hi", operation="op",
                   category="c", title="hi", priority=99)
    first = ledger.lease_next_actionable("r1")
    assert first["target"] == "hi" and first["status"] == "leased"   # highest priority first
    ledger.set_work_status(first["id"], "done")
    second = ledger.lease_next_actionable("r1")
    assert second["target"] == "lo"
    ledger.set_work_status(second["id"], "done")
    assert ledger.lease_next_actionable("r1") is None      # drained
    assert ledger.actionable_count("r1") == 0


def test_status_counts_and_coverage(ledger):
    ids = [ledger.enqueue(run_id="r1", key=stable_key(f"w{i}"), target=f"w{i}",
                          operation="op", category="c", title="t") for i in range(4)]
    ledger.set_work_status(ids[0], "done")
    ledger.set_work_status(ids[1], "done")
    ledger.set_work_status(ids[2], "blocked", error="needs vendor key")   # reason required
    ledger.set_work_status(ids[3], "failed", error="boom")
    cov = ledger.coverage("r1")
    assert cov["workItemsTotal"] == 4 and cov["done"] == 2
    assert cov["blocked"] == 1 and cov["failed"] == 1
    assert cov["terminal"] == 4 and cov["pending"] == 0 and cov["actionable"] == 0


def test_count_work_items_filters_by_operation_and_status(ledger):
    b = ledger.enqueue(run_id="r1", key=stable_key("bin"), target="bin",
                       operation="scan-binary", category="c", title="t")
    ledger.set_work_status(b, "blocked", error="no disassembler")
    f = ledger.enqueue(run_id="r1", key=stable_key("fetch"), target="u",
                       operation="fetch", category="c", title="t")
    ledger.set_work_status(f, "blocked", error="network denied")
    # both 'blocked' but different operations, so the filter separates a binary blind spot
    # from a network-blocked fetch (the bug the filtered count was added to fix).
    assert ledger.count_work_items("r1", operation="scan-binary", status="blocked") == 1
    assert ledger.count_work_items("r1", operation="fetch", status="blocked") == 1
    assert ledger.count_work_items("r1", status="blocked") == 2


def test_state_label_on_enqueue_set_and_clear(ledger):
    wid = ledger.enqueue(run_id="r1", key=stable_key("t"), target="t", operation="op",
                         category="c", title="t", state_label="triaged")
    assert ledger.get_work_item(wid)["state_label"] == "triaged"    # set at enqueue
    ledger.set_label(wid, "confirmed")
    assert ledger.get_work_item(wid)["state_label"] == "confirmed"  # moved independently
    ledger.set_label(wid, None)
    assert ledger.get_work_item(wid)["state_label"] is None         # explicit clear


def test_set_label_is_orthogonal_to_status(ledger):
    wid = ledger.enqueue(run_id="r1", key=stable_key("t"), target="t", operation="op",
                         category="c", title="t")
    ledger.set_label(wid, "reviewed")
    row = ledger.get_work_item(wid)
    assert row["status"] == "queued" and row["terminal_at"] is None  # label never touches status
    ledger.set_work_status(wid, "done")                             # status change leaves label alone
    row = ledger.get_work_item(wid)
    assert row["status"] == "done" and row["state_label"] == "reviewed"
    ledger.set_work_status(wid, "needs_review", state_label="disputed")  # stamp in same call
    assert ledger.get_work_item(wid)["state_label"] == "disputed"
    ledger.set_work_status(wid, "done", state_label=None)          # clear on transition
    assert ledger.get_work_item(wid)["state_label"] is None


def test_label_counts_and_filter(ledger):
    for i, lbl in enumerate(["confirmed", "confirmed", "triaged", None]):
        ledger.enqueue(run_id="r1", key=stable_key(f"w{i}"), target=f"w{i}",
                       operation="op", category="c", title="t", state_label=lbl)
    assert ledger.work_label_counts("r1") == {"confirmed": 2, "triaged": 1}  # NULLs excluded
    assert ledger.count_work_items("r1", state_label="confirmed") == 2
    assert ledger.count_work_items("r1") == 4                       # unfiltered still counts all


def test_dependency_dag_is_enforced_by_leasing(ledger):
    a = ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                       category="c", title="a")
    b = ledger.enqueue(run_id="r1", key=stable_key("b"), target="b", operation="op",
                       category="c", title="b", depends_on=[a])
    assert ledger.actionable_count("r1") == 1                # B waits on A; only A is actionable
    assert ledger.next_actionable("r1")["id"] == a
    first = ledger.lease_next_actionable("r1")
    assert first["id"] == a                                  # never leases B before A is terminal
    ledger.set_work_status(a, "done")
    assert ledger.actionable_count("r1") == 1                # A terminal → B unblocks
    assert ledger.lease_next_actionable("r1")["id"] == b


def test_enqueue_rejects_unknown_dependency(ledger):
    with pytest.raises(ValueError):
        ledger.enqueue(run_id="r1", key=stable_key("x"), target="x", operation="op",
                       category="c", title="t", depends_on=["wi-does-not-exist"])


def test_needs_evidence_is_actionable_and_re_leases(ledger):
    a = ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                       category="c", title="a")
    first = ledger.lease_next_actionable("r1")
    assert first["attempts"] == 1                            # lease bumps attempts
    ledger.need_evidence(a, note="need the header bytes")    # non-terminal, back to leasable
    row = ledger.get_work_item(a)
    assert row["status"] == "needs_evidence" and row["terminal_at"] is None
    assert ledger.next_actionable("r1")["id"] == a           # actionable again
    second = ledger.lease_next_actionable("r1")
    assert second["id"] == a and second["attempts"] == 2     # re-leased, attempts climbs (thrash signal)


def test_requeue_stale_leases_reclaims_crashed_items(ledger):
    ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                   category="c", title="a")
    ledger.lease_next_actionable("r1")                       # leased, then "crash" (never finished)
    assert ledger.actionable_count("r1") == 0                # a stale lease is not actionable
    assert ledger.requeue_stale_leases("r1") == 1
    assert ledger.actionable_count("r1") == 1                # reclaimed to queued


def test_deferred_and_blocked_require_a_reason(ledger):
    a = ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                       category="c", title="a")
    b = ledger.enqueue(run_id="r1", key=stable_key("b"), target="b", operation="op",
                       category="c", title="b")
    with pytest.raises(ValueError):
        ledger.set_work_status(a, "blocked")                 # no reason → refused
    with pytest.raises(ValueError):
        ledger.set_work_status(a, "deferred")
    ledger.block(a, "waiting on vendor")                     # verb supplies the reason
    ledger.defer(b, "out of scope this pass")
    assert ledger.get_work_item(a)["status"] == "blocked"
    assert ledger.get_work_item(a)["error"] == "waiting on vendor"
    assert ledger.get_work_item(b)["status"] == "deferred"


def test_resolve_records_result_and_evidence(ledger):
    a = ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                       category="c", title="a")
    ledger.resolve(a, result={"finding": "base64 decoder"}, evidence="xref from sub_401000")
    row = ledger.get_work_item(a)
    assert row["status"] == "done" and row["evidence"] == "xref from sub_401000"
    import json
    assert json.loads(row["result_json"])["finding"] == "base64 decoder"


def test_notes_append_and_read_in_order(ledger):
    a = ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                       category="c", title="a")
    ledger.add_note("r1", a, "looked promising", iteration=1)
    ledger.add_note("r1", a, "confirmed second pass", iteration=2)
    assert [n["text"] for n in ledger.get_notes(a)] == ["looked promising", "confirmed second pass"]


def test_assess_continue_then_complete(ledger):
    a = ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                       category="c", title="a")
    t = ledger.assess("r1")
    assert t.verdict == "continue" and t.actionable == 1 and t.pending == 1
    ledger.set_work_status(a, "done")
    t = ledger.assess("r1")
    assert t.verdict == "complete" and t.pending == 0


def test_assess_deadlocks_on_stale_lease(ledger):
    ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                   category="c", title="a")
    ledger.lease_next_actionable("r1")                       # in-flight but nothing drives it
    t = ledger.assess("r1")
    assert t.verdict == "deadlocked" and t.actionable == 0 and t.pending == 1


def test_assess_deadlocks_when_idle(ledger):
    ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                   category="c", title="a")                  # actionable but never touched
    verdicts = [ledger.assess("r1", idle_limit=2).verdict for _ in range(3)]
    assert verdicts == ["continue", "continue", "deadlocked"]


def test_assess_hits_max_iteration_ceiling(tmp_path):
    lg = Ledger(tmp_path / "m.db")
    lg.create_run(run_id="r", project_id="p", target_path="/t", target_root="/t",
                  storage_root=str(tmp_path), run_dir=str(tmp_path), config_json="{}",
                  max_iterations=2)
    lg.enqueue(run_id="r", key=stable_key("a"), target="a", operation="op",
               category="c", title="a")                      # always actionable → never completes
    verdicts = [lg.assess("r", idle_limit=99).verdict for _ in range(3)]
    assert verdicts[-1] == "max_iterations"
    lg.close()


def test_create_run_preserves_loop_state_on_resume(ledger):
    ledger.enqueue(run_id="r1", key=stable_key("a"), target="a", operation="op",
                   category="c", title="a")
    ledger.assess("r1")
    assert ledger.get_run("r1")["iteration"] == 1
    ledger.create_run(run_id="r1", project_id="p1", target_path="/t", target_root="/t",
                      storage_root="/tmp", run_dir="/tmp", config_json="{}")   # resume re-creates
    row = ledger.get_run("r1")
    assert row["iteration"] == 1 and row["status"] == "running"   # oracle state survived


def test_questions_and_answers(ledger):
    qid = stable_key("go?", "consent", "N")
    ledger.ask_question("r1", qid=qid, node="N", kind="consent", prompt="go?",
                        options=["y", "n"])
    ledger.ask_question("r1", qid=qid, node="N", kind="consent", prompt="go?")   # idempotent
    assert ledger.count_pending_questions("r1") == 1
    pend = ledger.pending_questions("r1")
    assert pend[0]["id"] == qid and pend[0]["options"] == ["y", "n"]
    ledger.record_answer("r1", qid, "y")
    assert ledger.get_answer("r1", qid) == "y"
    assert ledger.count_pending_questions("r1") == 0       # answered → no longer pending


def test_reset_run_derived_keeps_queue_run_and_answers(ledger):
    ledger.add_artifact(run_id="r1", kind="k", path="/p", logical_path="p", origin="inventory")
    wid = ledger.enqueue(run_id="r1", key=stable_key("w"), target="w", operation="op",
                         category="c", title="t")
    ledger.add_note("r1", wid, "an observation")
    qid = stable_key("q")
    ledger.ask_question("r1", qid=qid, node="N", kind="k", prompt="q")
    ledger.record_answer("r1", qid, "yes")
    ledger.reset_run_derived("r1")
    assert ledger.count_artifacts("r1") == 0               # artifacts re-recorded on re-drive → wiped
    assert ledger.count_work_items("r1") == 1              # durable queue survives (dedup on re-enqueue)
    assert len(ledger.get_notes(wid)) == 1                 # per-item notes ride with the durable queue
    assert ledger.count_pending_questions("r1") == 0       # questions are re-asked on re-drive
    assert ledger.get_run("r1") is not None                # the run row survives
    assert ledger.get_answer("r1", qid) == "yes"           # answers survive the reset


def test_extra_schema_and_reset_tables_registration(tmp_path):
    """An application registers a domain table and its resume-reset through the
    constructor: the composition seam that keeps muster ignorant of the domain."""
    class DomainLedger(Ledger):
        def __init__(self, db_path):
            super().__init__(
                db_path,
                extra_schema="create table if not exists facts "
                             "(id text primary key, run_id text, v text);",
                reset_tables=("facts",))

    lg = DomainLedger(tmp_path / "d.db")
    lg.create_run(run_id="r", project_id="p", target_path="/t", target_root="/t",
                  storage_root=str(tmp_path), run_dir=str(tmp_path), config_json="{}")
    lg.conn.execute("insert into facts (id, run_id, v) values ('f1','r','x')")
    lg.conn.commit()
    assert lg.conn.execute("select count(*) from facts").fetchone()[0] == 1
    lg.reset_run_derived("r")
    assert lg.conn.execute("select count(*) from facts").fetchone()[0] == 0   # domain wiped
    lg.close()


def test_context_manager_closes(tmp_path):
    import sqlite3
    with Ledger(tmp_path / "cm.db") as lg:
        lg.create_run(run_id="r", project_id="p", target_path="/t", target_root="/t",
                      storage_root=str(tmp_path), run_dir=str(tmp_path), config_json="{}")
    with pytest.raises(sqlite3.ProgrammingError):
        lg.conn.execute("select 1")                        # connection closed on exit
