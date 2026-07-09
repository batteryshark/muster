-- muster core ledger (per-run SQLite database): the spine tables.
--
-- This is the durable source of truth for coverage and resumability. The phase
-- graph reads ledger state to decide what runs next and when a run may finish;
-- the model never decides completion. An application registers its own domain
-- tables (whatever a "fact" or "finding" is for it) via Ledger(extra_schema=...);
-- muster never knows about them.

pragma journal_mode = wal;
pragma synchronous = normal;
pragma busy_timeout = 5000;
pragma foreign_keys = on;

create table if not exists meta (
    key   text primary key,
    value text not null
);

create table if not exists runs (
    id                     text primary key,
    project_id             text not null,
    target_path            text not null,
    target_root            text not null,
    storage_root           text not null,
    run_dir                text not null,
    status                 text not null,          -- queued|running|completed|partial|blocked|failed|canceled
    created_at             text not null,
    updated_at             text not null,
    completed_at           text,
    config_json            text not null,
    -- Termination-oracle state (see Ledger.assess). The loop-pass counter, its safety
    -- ceiling, and the idle-detection snapshot. Persistent, and preserved across resume,
    -- so a re-driven run keeps counting toward its ceiling instead of starting over.
    iteration              integer not null default 0,
    max_iterations         integer not null default 50,
    idle_rounds            integer not null default 0,
    items_at_last_check    integer not null default 0,
    terminal_at_last_check integer not null default 0,
    coverage_json          text,
    summary_json           text,
    error                  text
);

create table if not exists artifacts (
    id                 text primary key,
    run_id             text not null,
    kind               text not null,             -- consumer-defined artifact kind (e.g. source-file|archive|report|...)
    path               text not null,
    logical_path       text not null,
    parent_artifact_id text,
    sha256             text,
    size_bytes         integer,
    media_type         text,
    language           text,
    origin             text not null,             -- consumer-defined provenance (e.g. inventory|fetch|report)
    metadata_json      text not null default '{}',
    created_at         text not null
);
create index if not exists idx_artifacts_run on artifacts(run_id);

create table if not exists work_items (
    id             text primary key,
    run_id         text not null,
    stable_key     text not null,
    target         text not null,
    operation      text not null,                 -- consumer-defined unit of work (e.g. inventory|render-report|...)
    category       text not null,
    title          text not null,
    -- Loop-driver state machine. Terminal: done|failed|needs_review|deferred|blocked.
    -- Leasable (the drain loop may claim it, dependencies permitting): queued|needs_evidence.
    -- needs_evidence is a non-terminal "come back with more" state, so it re-leases.
    status         text not null,
    state_label    text,                           -- domain state, orthogonal to status; the drain loop ignores it (e.g. evaluating|reviewed|triaged|confirmed)
    priority       integer not null default 100,
    depends_on_json text not null default '[]',    -- ids that must be terminal before this item is actionable (the work DAG)
    payload_json   text not null default '{}',
    attempts       integer not null default 0,     -- bumped on every lease; a growing count on a still-pending item flags thrash
    created_at     text not null,
    updated_at     text not null,
    terminal_at    text,
    result_json    text,
    evidence       text,                           -- first-class proof/support for a resolution, distinct from the result blob
    error          text,                           -- why it did not resolve: the required reason for deferred/blocked, or the failure for failed
    unique(run_id, stable_key)
);
create index if not exists idx_work_run_status on work_items(run_id, status);

-- Append-only, per-item audit notes. A work item can accrue observations across passes
-- without mutating its result; parallel to graph_events but attributable to one item.
create table if not exists work_notes (
    id            text primary key,
    run_id        text not null,
    work_item_id  text not null,
    iteration     integer not null default 0,
    text          text not null,
    created_at    text not null
);
create index if not exists idx_work_notes_item on work_notes(work_item_id);

create table if not exists graph_events (
    id           text primary key,
    run_id       text not null,
    node         text not null,
    event        text not null,                   -- enter|exit|note|error
    payload_json text not null default '{}',
    created_at   text not null
);
create index if not exists idx_events_run on graph_events(run_id);

create table if not exists reports (
    id         text primary key,
    run_id     text not null,
    format     text not null,
    path       text not null,
    sha256     text,
    created_at text not null
);

-- Durable questions a node could not answer itself. The run finishes `needs_input`
-- with these pending; whoever is driving the run answers and resumes. Regenerated per drive
-- (re-asked when the graph re-runs), so it IS reset on resume.
create table if not exists questions (
    id           text primary key,               -- content-addressed (prompt+kind+node)
    run_id       text not null,
    node         text not null,
    kind         text not null,                  -- fetch-consent|lead-decision|...
    prompt       text not null,
    options_json text not null default '[]',
    created_at   text not null
);
create index if not exists idx_questions_run on questions(run_id);

-- Answers injected on resume. Keyed by the question's content-addressed id and NOT
-- reset on re-drive, so a resumed run's re-asked question finds its answer.
create table if not exists answers (
    id          text primary key,                -- the question id it answers
    run_id      text not null,
    answer      text not null,
    answered_at text not null
);
create index if not exists idx_answers_run on answers(run_id);
