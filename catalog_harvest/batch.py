"""Persistent three-stage batch orchestration: sync, warehouse, reports.

The existing entry points (``catalog_harvest.sync``,
``catalog_harvest.warehouse`` and ``catalog_harvest.affinity``) are complete
single-purpose commands. This module adds an *orchestrator* that chains them
for one frozen batch without changing any of their business semantics: every
stage still calls the shipping library function, and the batch only decides
order, skip, retry and evidence recording.

A batch is identified by a caller-supplied unique ``run_id`` and one strict
JSON plan. Inside ``<workdir>/<run_id>`` the orchestrator freezes:

* the exact plan bytes, the taxonomy bytes and the analysis-plan bytes
  (``inputs/``), together with their SHA256 digests in ``batch.json``;
* a SQLite ledger (``state.sqlite``) with one row per stage holding the
  planned intent, state, attempt history, last error and artifact evidence;
* the sync database, the warehouse database, one JSON report per plan date
  and finally a delivery manifest.

Stage order is fixed: ``sync`` -> ``warehouse`` -> one ``report:<date>`` per
plan date -> ``manifest``. A failed stage blocks every downstream stage for
that run. A stage whose state is ``succeeded`` is verified against its
recorded artifact digest and skipped, never re-executed. A stage left
``running`` (or still ``pending``) by a crashed process is *reattached* when
its published artifact can be proven to belong to this batch (identity +
logical content digests), so a commit that landed before the ledger update
never repeats the sync window, rebuilds a published warehouse or recomputes a
published report. A corrupt or foreign artifact is reported and preserved;
it is never silently overwritten. The complete delivery manifest is only
produced when every stage has succeeded.

Only the Python standard library and SQLite are used.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .affinity import AffinityError, publish_report
from .protocol import parse_iso_date
from .sync import SyncError, run_sync
from .warehouse import (
    WarehouseError, build_warehouse, read_formal_facts,
    source_facts_digest)

BATCH_FORMAT = "catalog_harvest_batch"
BATCH_FORMAT_VERSION = "1"
PLAN_SCHEMA_VERSION = 1

STAGE_SYNC = "sync"
STAGE_WAREHOUSE = "warehouse"
STAGE_MANIFEST = "manifest"
REPORT_STAGE_PREFIX = "report:"

STATE_PENDING = "pending"
STATE_RUNNING = "running"
STATE_SUCCEEDED = "succeeded"
STATE_FAILED = "failed"
STATE_BLOCKED = "blocked"

# Test-only deterministic hooks (not part of the operational contract):
# exit inside the commit/ledger gap, or make one named stage fail.
ENV_BARRIER = "CATALOG_HARVEST_BATCH_BARRIER"
ENV_FAIL_STAGE = "CATALOG_HARVEST_BATCH_FAIL_STAGE"

_RUN_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")


class BatchError(ValueError):
    """Plan, identity, lock or ledger failure the operator must fix."""


class VerificationError(BatchError):
    """A published artifact could not be proven to belong to this batch."""


def _now():
    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


# ---------------------------------------------------------------------------
# Strict plan parsing (duplicate keys rejected, typed fields only)
# ---------------------------------------------------------------------------

class _DuplicateKeyDetector:
    def __init__(self):
        self.duplicates = []

    def __call__(self, pairs):
        seen = set()
        result = {}
        for key, value in pairs:
            if key in seen:
                self.duplicates.append(key)
            seen.add(key)
            result[key] = value
        return result


PLAN_KEYS = {
    "schema_version", "sync", "warehouse", "reports", "affinity"}


def parse_plan(raw_bytes):
    """Validate the frozen batch plan and return structured contents."""
    if not isinstance(raw_bytes, bytes) or not raw_bytes:
        raise BatchError("plan must be a non-empty strict UTF-8 JSON document")
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BatchError("plan must be strict UTF-8 encoded JSON") from exc
    detector = _DuplicateKeyDetector()
    try:
        document = json.loads(text, object_pairs_hook=detector)
    except json.JSONDecodeError as exc:
        raise BatchError(f"plan is not valid JSON: {exc}") from exc
    if detector.duplicates:
        raise BatchError(
            "plan contains duplicate JSON object keys: "
            + ", ".join(map(str, detector.duplicates)))
    if not isinstance(document, dict) or set(document) != PLAN_KEYS:
        raise BatchError(
            "plan top level must contain exactly schema_version, sync, "
            "warehouse, reports and affinity")
    schema_version = document["schema_version"]
    if isinstance(schema_version, bool) or schema_version != PLAN_SCHEMA_VERSION:
        raise BatchError("plan schema_version must be the integer 1")

    sync = document["sync"]
    if not isinstance(sync, dict) or set(sync) != {
            "endpoint", "start_date", "until", "overlap_days", "max_pages"}:
        raise BatchError(
            "plan sync must contain exactly endpoint, start_date, until, "
            "overlap_days and max_pages")
    endpoint = sync["endpoint"]
    if not isinstance(endpoint, str):
        raise BatchError("plan sync.endpoint must be a string")
    start_text = sync["start_date"]
    if not isinstance(start_text, str):
        raise BatchError("plan sync.start_date must be a YYYY-MM-DD string")
    try:
        start_date = parse_iso_date(start_text)
    except ValueError as exc:
        raise BatchError(f"plan sync.start_date: {exc}") from exc
    until_text = sync["until"]
    if not isinstance(until_text, str):
        raise BatchError("plan sync.until must be a YYYY-MM-DD string")
    try:
        parse_iso_date(until_text)
    except ValueError as exc:
        raise BatchError(f"plan sync.until: {exc}") from exc
    overlap_days = sync["overlap_days"]
    if isinstance(overlap_days, bool) or not isinstance(overlap_days, int) \
            or not 1 <= overlap_days <= 365:
        raise BatchError("plan sync.overlap_days must be an integer in 1..365")
    max_pages = sync["max_pages"]
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) \
            or not 1 <= max_pages <= 1000:
        raise BatchError("plan sync.max_pages must be an integer in 1..1000")

    warehouse = document["warehouse"]
    if not isinstance(warehouse, dict) or set(warehouse) != {
            "start", "end", "taxonomy_file"}:
        raise BatchError(
            "plan warehouse must contain exactly start, end and taxonomy_file")
    try:
        warehouse_start = parse_iso_date(warehouse["start"])
        warehouse_end = parse_iso_date(warehouse["end"])
    except (TypeError, ValueError) as exc:
        raise BatchError(
            "plan warehouse.start/end must be strict YYYY-MM-DD strings") from exc
    if not warehouse_start < warehouse_end:
        raise BatchError("plan warehouse.start must precede warehouse.end")
    taxonomy_file = warehouse["taxonomy_file"]
    if not isinstance(taxonomy_file, str) or not taxonomy_file:
        raise BatchError("plan warehouse.taxonomy_file must be a nonempty string")

    reports = document["reports"]
    if not isinstance(reports, list) or not reports:
        raise BatchError("plan reports must be a nonempty array of dates")
    report_dates = []
    for item in reports:
        if not isinstance(item, str):
            raise BatchError("plan reports entries must be YYYY-MM-DD strings")
        try:
            report_day = parse_iso_date(item)
        except ValueError as exc:
            raise BatchError(f"plan reports entry: {exc}") from exc
        if not warehouse_start <= report_day < warehouse_end:
            raise BatchError(
                f"report date {item} must satisfy warehouse start <= date < end")
        if item in report_dates:
            raise BatchError(f"duplicate report date: {item}")
        report_dates.append(item)

    affinity = document["affinity"]
    if not isinstance(affinity, dict) or set(affinity) != {"plan_file"}:
        raise BatchError("plan affinity must contain exactly plan_file")
    affinity_file = affinity["plan_file"]
    if not isinstance(affinity_file, str) or not affinity_file:
        raise BatchError("plan affinity.plan_file must be a nonempty string")

    return {
        "schema_version": schema_version,
        "endpoint": endpoint,
        "start_date": start_date,
        "start_text": start_text,
        "until_text": until_text,
        "overlap_days": overlap_days,
        "max_pages": max_pages,
        "warehouse_start": warehouse_start,
        "warehouse_end": warehouse_end,
        "warehouse_start_text": warehouse_start.isoformat(),
        "warehouse_end_text": warehouse_end.isoformat(),
        "taxonomy_file": taxonomy_file,
        "report_dates": report_dates,
        "affinity_file": affinity_file,
    }


def plan_stage_names(plan):
    names = [STAGE_SYNC, STAGE_WAREHOUSE]
    names.extend(REPORT_STAGE_PREFIX + day for day in plan["report_dates"])
    names.append(STAGE_MANIFEST)
    return names


# ---------------------------------------------------------------------------
# Batch directory, frozen inputs and ledger
# ---------------------------------------------------------------------------

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS batch_meta (
 key TEXT PRIMARY KEY,
 value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stage (
 name TEXT PRIMARY KEY,
 position INTEGER NOT NULL,
 intent_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK (
   state IN ('pending','running','succeeded','failed','blocked')),
 attempted INTEGER NOT NULL DEFAULT 0,
 last_error TEXT,
 evidence_json TEXT,
 updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attempt (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 stage TEXT NOT NULL,
 attempt_no INTEGER NOT NULL,
 started_at TEXT NOT NULL,
 finished_at TEXT,
 outcome TEXT NOT NULL,
 error TEXT,
 UNIQUE (stage, attempt_no)
);
CREATE TABLE IF NOT EXISTS recovery_event (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 stage TEXT NOT NULL,
 action TEXT NOT NULL CHECK (
   action IN ('reattached','resumed','requeued','failed')),
 detail TEXT,
 recorded_at TEXT NOT NULL
);
"""

_LAYOUT = {
    "catalog": "catalog.sqlite",
    "warehouse": "warehouse.sqlite",
    "reports": "reports",
    "manifest": "manifest.json",
    "ledger": "state.sqlite",
    "metadata": "batch.json",
    "lock": "state.lock",
    "inputs": "inputs",
    "frozen_plan": "inputs/plan.json",
    "frozen_taxonomy": "inputs/taxonomy.json",
    "frozen_affinity": "inputs/affinity_plan.json",
}


def _atomic_write(path, data, mode=0o600):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = None
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
                prefix="." + path.name + "-", suffix=".tmp",
                dir=path.parent, delete=False) as handle:
            tmp_path = Path(handle.name)
        with open(tmp_path, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
        tmp_path = None
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def _validate_run_id(run_id):
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        raise BatchError(
            "run-id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63} and must not "
            "be a path; each run-id owns one private batch directory")
    if run_id in {".", ".."}:
        raise BatchError("run-id must not be '.' or '..'")
    return run_id


def _batch_paths(batch_dir):
    return {key: Path(batch_dir) / value for key, value in _LAYOUT.items()}


def _read_metadata(batch_dir):
    metadata_path = Path(batch_dir) / _LAYOUT["metadata"]
    try:
        raw = metadata_path.read_bytes()
    except FileNotFoundError as exc:
        raise BatchError(
            f"batch directory {batch_dir} has no batch.json; it is incomplete "
            "or belongs to another program and is left untouched") from exc
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BatchError(
            f"batch metadata {metadata_path} is not valid UTF-8 JSON and is "
            "left untouched") from exc
    if (not isinstance(document, dict)
            or document.get("format") != BATCH_FORMAT
            or document.get("format_version") != BATCH_FORMAT_VERSION):
        raise BatchError(
            f"batch metadata {metadata_path} is not a supported "
            "catalog_harvest batch; it is left untouched")
    required = {"run_id", "plan_sha256", "inputs", "created_at", "sources"}
    missing = required - set(document)
    if missing:
        raise BatchError(
            f"batch metadata {metadata_path} is missing keys "
            f"{sorted(missing)}; batch directory is damaged and left untouched")
    if not isinstance(document["run_id"], str) \
            or not isinstance(document["plan_sha256"], str) \
            or not isinstance(document["inputs"], dict) \
            or not isinstance(document["sources"], dict):
        raise BatchError(
            f"batch metadata {metadata_path} has malformed fields; batch "
            "directory is damaged and left untouched")
    return document


def _frozen_inputs(metadata, batch_dir):
    inputs = metadata.get("inputs")
    if not isinstance(inputs, dict):
        raise BatchError("batch metadata is missing frozen input records")
    frozen = {}
    for kind in ("plan", "taxonomy", "affinity"):
        record = inputs.get(kind)
        if (not isinstance(record, dict)
                or set(record) != {"path", "sha256"}):
            raise BatchError(f"batch metadata input {kind!r} is malformed")
        path = Path(batch_dir) / record["path"]
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise BatchError(
                f"frozen input {record['path']} is missing; batch directory "
                "is damaged and is left untouched") from exc
        actual = _sha256_bytes(data)
        if actual != record["sha256"]:
            raise BatchError(
                f"frozen input {record['path']} does not match its recorded "
                "SHA256; batch directory is damaged and is left untouched")
        frozen[kind] = data
    return frozen


def _check_external_sources(metadata, frozen):
    """Reject an input file whose content changed since the batch was frozen."""
    sources = metadata.get("sources") or {}
    for kind in ("taxonomy", "affinity"):
        source_text = sources.get(kind)
        if not source_text:
            continue
        source = Path(source_text)
        if not source.exists():
            continue  # frozen bytes remain authoritative and unchanged
        try:
            current = source.read_bytes()
        except OSError as exc:
            raise BatchError(f"cannot read input {source}: {exc}") from exc
        if current != frozen[kind]:
            raise BatchError(
                f"input {kind} at {source} changed since this batch was "
                "created; use a new run-id for changed inputs")


def _create_batch(plan_path, workdir, run_id):
    plan_path = Path(plan_path)
    try:
        plan_bytes = plan_path.read_bytes()
    except FileNotFoundError as exc:
        raise BatchError(f"plan not found: {plan_path}") from exc
    plan = parse_plan(plan_bytes)
    plan_digest = _sha256_bytes(plan_bytes)

    # Validate the referenced inputs and the loopback endpoint before any
    # batch directory is created or any stage runs.
    taxonomy_path = Path(plan["taxonomy_file"]).resolve()
    affinity_path = Path(plan["affinity_file"]).resolve()
    try:
        taxonomy_bytes = taxonomy_path.read_bytes()
    except FileNotFoundError as exc:
        raise BatchError(f"taxonomy file not found: {taxonomy_path}") from exc
    try:
        affinity_bytes = affinity_path.read_bytes()
    except FileNotFoundError as exc:
        raise BatchError(
            f"affinity plan file not found: {affinity_path}") from exc
    from .warehouse import parse_taxonomy
    from .affinity import parse_plan as parse_affinity_plan
    from .transport import window_fetcher
    try:
        parse_taxonomy(taxonomy_bytes)
    except WarehouseError as exc:
        raise BatchError(f"taxonomy is invalid: {exc}") from exc
    try:
        parse_affinity_plan(affinity_bytes)
    except AffinityError as exc:
        raise BatchError(f"affinity plan is invalid: {exc}") from exc
    try:
        window_fetcher(plan["endpoint"])
    except ValueError as exc:
        raise BatchError(f"sync endpoint is invalid: {exc}") from exc

    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    batch_dir = workdir / run_id
    if batch_dir.exists():
        return None  # another creator won the race; resume it instead
    if os.path.islink(batch_dir):
        raise BatchError(
            f"batch path {batch_dir} is a symlink; refusing to use it")
    staging = Path(tempfile.mkdtemp(prefix=f".{run_id}.", dir=workdir))
    try:
        paths = _batch_paths(staging)
        paths["inputs"].mkdir(mode=0o700, exist_ok=True)
        _atomic_write(paths["frozen_plan"], plan_bytes)
        _atomic_write(paths["frozen_taxonomy"], taxonomy_bytes)
        _atomic_write(paths["frozen_affinity"], affinity_bytes)

        metadata = {
            "format": BATCH_FORMAT,
            "format_version": BATCH_FORMAT_VERSION,
            "run_id": run_id,
            "plan_sha256": plan_digest,
            "created_at": _now(),
            "inputs": {
                "plan": {"path": _LAYOUT["frozen_plan"],
                         "sha256": plan_digest},
                "taxonomy": {"path": _LAYOUT["frozen_taxonomy"],
                             "sha256": _sha256_bytes(taxonomy_bytes)},
                "affinity": {"path": _LAYOUT["frozen_affinity"],
                             "sha256": _sha256_bytes(affinity_bytes)},
            },
            "sources": {
                "taxonomy": str(taxonomy_path),
                "affinity": str(affinity_path),
            },
        }

        # Build the ledger fully before publishing the directory, so a crash
        # can never expose a half-initialised batch.
        Path(paths["lock"]).touch(mode=0o600)
        ledger = _connect_ledger(paths["ledger"], writable=True, create=True)
        try:
            _initialise_ledger(ledger, run_id, metadata, plan)
        finally:
            ledger.close()
        _atomic_write(
            paths["metadata"], _stable_json(metadata).encode("utf-8"))
        try:
            os.rename(staging, batch_dir)
        except OSError:
            if not batch_dir.exists():
                raise
            # Another creator renamed first; this staging copy is discarded.
            return None
        staging = None
        return batch_dir
    finally:
        if staging is not None:
            import shutil
            shutil.rmtree(staging, ignore_errors=True)


def _connect_ledger(path, writable, create=False):
    path = Path(path)
    if writable:
        if not create and not path.exists():
            raise BatchError("batch ledger is missing; directory is damaged")
        conn = sqlite3.connect(path, timeout=5)
    else:
        if not path.exists():
            raise BatchError("batch ledger is missing; directory is damaged")
        conn = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True,
                               timeout=5)
        conn.execute("PRAGMA query_only=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _intent_for(stage_name, plan, digests):
    if stage_name == STAGE_SYNC:
        return {
            "action": "sync one frozen date window",
            "endpoint": plan["endpoint"],
            "start_date": plan["start_text"],
            "until": plan["until_text"],
            "overlap_days": plan["overlap_days"],
            "max_pages": plan["max_pages"],
            "database": _LAYOUT["catalog"],
        }
    if stage_name == STAGE_WAREHOUSE:
        return {
            "action": "build offline historical subject warehouse",
            "source_database": _LAYOUT["catalog"],
            "database": _LAYOUT["warehouse"],
            "start": plan["warehouse_start_text"],
            "end": plan["warehouse_end_text"],
            "taxonomy_sha256": digests["taxonomy"],
        }
    if stage_name.startswith(REPORT_STAGE_PREFIX):
        report_day = stage_name[len(REPORT_STAGE_PREFIX):]
        return {
            "action": "publish one co-occurrence report",
            "date": report_day,
            "warehouse": _LAYOUT["warehouse"],
            "affinity_plan_sha256": digests["affinity"],
            "output": f"{_LAYOUT['reports']}/{report_day}.json",
        }
    return {
        "action": "publish the complete delivery manifest",
        "output": _LAYOUT["manifest"],
        "requires": list(plan_stage_names(plan)[:-1]),
    }


def _initialise_ledger(ledger, run_id, metadata, plan):
    digests = {
        "plan": metadata["inputs"]["plan"]["sha256"],
        "taxonomy": metadata["inputs"]["taxonomy"]["sha256"],
        "affinity": metadata["inputs"]["affinity"]["sha256"],
    }
    names = plan_stage_names(plan)
    with ledger:
        ledger.executescript(LEDGER_DDL)
        ledger.executemany(
            "INSERT OR REPLACE INTO batch_meta VALUES (?,?)",
            [("format", BATCH_FORMAT),
             ("format_version", BATCH_FORMAT_VERSION),
             ("run_id", run_id),
             ("plan_sha256", digests["plan"]),
             ("created_at", metadata["created_at"])])
        for position, name in enumerate(names):
            ledger.execute(
                "INSERT OR IGNORE INTO stage "
                "(name, position, intent_json, state, attempted, updated_at) "
                "VALUES (?,?,?,?,0,?)",
                (name, position, _stable_json(_intent_for(name, plan, digests)),
                 STATE_PENDING, _now()))


class _BatchLock:
    """Exclusive non-blocking lock; a competing process is refused at once."""

    def __init__(self, path):
        self.path = Path(path)
        self.handle = None

    def __enter__(self):
        try:
            import fcntl
        except ImportError as exc:  # pragma: no cover - POSIX target
            raise BatchError(
                "exclusive batch locking requires fcntl on this platform") from exc
        # O_NOFOLLOW: never follow a planted symlink to an unrelated file.
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise BatchError(
                f"batch lock {self.path} cannot be opened safely: {exc}") from exc
        handle = os.fdopen(fd, "r+b")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise BatchError(
                "another process is already running this batch; its lock is "
                "held, so this run is refused immediately without waiting"
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()} {_now()}\n".encode("utf-8"))
        handle.flush()
        self.handle = handle
        return self

    def __exit__(self, *exc):
        if self.handle is not None:
            try:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            self.handle.close()
            self.handle = None
        return False


# ---------------------------------------------------------------------------
# Stage evidence: identity + logical content proof for reattachment
# ---------------------------------------------------------------------------

class StageFailure(BatchError):
    """One stage failed; the batch is stopped or partially completed."""

    def __init__(self, message, summary):
        super().__init__(message)
        self.summary = summary


def _sqlite_tables(conn):
    return {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view')")}


def _formal_rows(conn):
    for table in ("pages", "revisions"):
        if table in _sqlite_tables(conn):
            if conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]:
                return True
    return False


def _sync_evidence_if_complete(paths, plan):
    """Proof that a committed sync window belongs to this frozen batch.

    Returns ``None`` when nothing has committed yet (a resume may continue the
    same open window), evidence when the single frozen window is complete, and
    raises :class:`VerificationError` when the database exists but is foreign
    or damaged. A file merely existing is never accepted as success.
    """
    database = paths["catalog"]
    if not database.exists():
        return None
    if database.stat().st_size == 0:
        return None
    conn = sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        tables = _sqlite_tables(conn)
        if "sync_meta" not in tables or "sync_window" not in tables:
            if _formal_rows(conn):
                raise VerificationError(
                    f"{database} contains formal pages/revisions but no "
                    "incremental batch state; it is foreign to this batch and "
                    "is preserved unchanged")
            return None
        meta = dict(conn.execute("SELECT key, value FROM sync_meta").fetchall())
        if (meta.get("endpoint") != plan["endpoint"]
                or meta.get("base_date") != plan["start_text"]):
            raise VerificationError(
                f"{database} is bound to a different batch identity "
                f"(endpoint={meta.get('endpoint')!r}, "
                f"base={meta.get('base_date')!r}); preserved unchanged")
        watermark = meta.get("watermark")
        if watermark is not None and watermark != plan["until_text"]:
            raise VerificationError(
                f"{database} has success watermark {watermark!r}, not this "
                f"batch's {plan['until_text']!r}; preserved unchanged")
        window = _single_sync_window(conn)
        if window is not None:
            expected = (plan["endpoint"], plan["start_text"],
                        plan["until_text"], plan["overlap_days"],
                        plan["max_pages"])
            frozen = (window["endpoint"], window["window_from"],
                      window["window_until"], window["overlap_days"],
                      window["max_pages"])
            if frozen != expected:
                raise VerificationError(
                    f"{database} opened a sync window with frozen parameters "
                    f"{frozen!r}, not this batch's {expected!r}; preserved "
                    "unchanged")
        if window is None or window["state"] != "complete" \
                or window["stage"] != "done" or watermark is None:
            return None
        window_id = window["window_id"]
        pages = conn.execute(
            "SELECT count(*), coalesce(sum(record_count),0) FROM sync_stage_pages "
            "WHERE window_id=?", (window_id,)).fetchone()
        revisions = conn.execute("SELECT count(*) FROM revisions").fetchone()[0]
        active, deleted = conn.execute(
            "SELECT sum(deleted=0), sum(deleted=1) FROM current_records").fetchone()
    except sqlite3.DatabaseError as exc:
        raise VerificationError(
            f"{database} cannot be read as a sync database ({exc}); preserved "
            "unchanged") from exc
    finally:
        conn.close()

    # Logical content re-validation (not merely file existence): the factual
    # pages/revisions must hash, parse and agree with their canonical payloads.
    facts = read_formal_facts(database)
    facts_digest = source_facts_digest(facts)
    return {
        "artifact": _LAYOUT["catalog"],
        "sha256": _sha256_file(database),
        "endpoint": meta["endpoint"],
        "window_from": window["window_from"],
        "window_until": window["window_until"],
        "watermark": meta["watermark"],
        "overlap_days": window["overlap_days"],
        "max_pages": window["max_pages"],
        "pages": pages[0],
        "records_seen": pages[1],
        "inserted_revisions": revisions,
        "active_records": active or 0,
        "deleted_records": deleted or 0,
        "source_facts_sha256": facts_digest,
    }


def _single_sync_window(conn):
    """Exactly one window is allowed; more than one is foreign to a batch."""
    rows = conn.execute("SELECT * FROM sync_window ORDER BY window_id").fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise VerificationError(
            f"sync database holds {len(rows)} windows, but a batch opens "
            "exactly one; preserved unchanged")
    return rows[0]


def _warehouse_evidence_if_present(paths, plan, expected_facts_sha256,
                                   expected_taxonomy_sha256):
    """Proof that a published warehouse belongs to this batch."""
    database = paths["warehouse"]
    if not database.exists():
        return None
    if database.stat().st_size == 0:
        raise VerificationError(
            f"{database} exists but is empty; a published warehouse cannot be "
            "0 bytes. It is preserved unchanged")
    try:
        conn = sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        try:
            tables = _sqlite_tables(conn)
            if "wh_meta" not in tables:
                raise VerificationError(
                    f"{database} is not a catalog_harvest warehouse; it is "
                    "foreign to this batch and preserved unchanged")
            meta = dict(conn.execute(
                "SELECT key, value FROM wh_meta ORDER BY key").fetchall())
            expected = {
                "format": "catalog_harvest_warehouse",
                "format_version": "1",
                "build_start": plan["warehouse_start_text"],
                "build_end": plan["warehouse_end_text"],
                "source_facts_sha256": expected_facts_sha256,
                "taxonomy_sha256": expected_taxonomy_sha256,
            }
            mismatched = {key: (meta.get(key), value)
                          for key, value in expected.items()
                          if meta.get(key) != value}
            if mismatched:
                raise VerificationError(
                    f"{database} does not match this batch's warehouse "
                    f"identity/content ({sorted(mismatched)}); preserved unchanged")
            integrity = conn.execute("PRAGMA quick_check").fetchone()[0]
            if integrity != "ok":
                raise VerificationError(
                    f"{database} failed its integrity check ({integrity}); "
                    "preserved unchanged")
            versions = conn.execute(
                "SELECT count(*) FROM wh_versions").fetchone()[0]
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        raise VerificationError(
            f"{database} cannot be read as a warehouse ({exc}); preserved "
            "unchanged") from exc
    return {
        "artifact": _LAYOUT["warehouse"],
        "sha256": _sha256_file(database),
        "source_facts_sha256": meta["source_facts_sha256"],
        "taxonomy_sha256": meta["taxonomy_sha256"],
        "build_start": meta["build_start"],
        "build_end": meta["build_end"],
        "versions": versions,
    }


def _report_evidence_if_present(paths, plan, day, expected_facts_sha256,
                                expected_taxonomy_sha256,
                                expected_plan_sha256):
    """Proof that a published report belongs to this batch and is intact."""
    output = Path(paths["reports"]) / f"{day}.json"
    if not output.exists():
        return None
    try:
        raw = output.read_bytes()
        report = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VerificationError(
            f"{output} is not readable strict JSON ({exc}); preserved "
            "unchanged") from exc
    if not isinstance(report, dict):
        raise VerificationError(
            f"{output} is not a JSON object; preserved unchanged")
    warehouse = report.get("warehouse") if isinstance(
        report.get("warehouse"), dict) else {}
    digests = report.get("digests") if isinstance(
        report.get("digests"), dict) else {}
    identity = {
        "format": (report.get("format"), "catalog_harvest_affinity_report"),
        "schema_version": (report.get("schema_version"), 1),
        "date": (report.get("date"), day),
        "warehouse_format": (warehouse.get("format"),
                             "catalog_harvest_warehouse"),
        "warehouse_format_version": (warehouse.get("format_version"), "1"),
        "build_start": (warehouse.get("build_start"),
                        plan["warehouse_start_text"]),
        "build_end": (warehouse.get("build_end"),
                      plan["warehouse_end_text"]),
        "source_facts_sha256": (warehouse.get("source_facts_sha256"),
                                expected_facts_sha256),
        "taxonomy_sha256": (warehouse.get("taxonomy_sha256"),
                            expected_taxonomy_sha256),
        "plan_bytes_sha256": (digests.get("plan_bytes_sha256"),
                              expected_plan_sha256),
    }
    mismatched = {key: values for key, values in identity.items()
                  if values[0] != values[1]}
    if mismatched:
        raise VerificationError(
            f"{output} does not match this batch's report identity "
            f"({sorted(mismatched)}); preserved unchanged")
    # Recompute the report's own logical digests; tampering with the evidence
    # section is detected without re-running the analysis.
    from .affinity import metadata_digest, relations_digest, evidence_digest
    for key, function in (("metadata_sha256", metadata_digest),
                          ("selected_relations_sha256", relations_digest),
                          ("record_evidence_sha256", evidence_digest)):
        actual = function(report)
        if digests.get(key) != actual:
            raise VerificationError(
                f"{output} has an inconsistent {key} content digest; "
                "preserved unchanged")
    population = report.get("population") or {}
    family = report.get("family") or {}
    return {
        "artifact": f"{_LAYOUT['reports']}/{day}.json",
        "sha256": _sha256_bytes(raw),
        "date": day,
        "plan_bytes_sha256": digests["plan_bytes_sha256"],
        "metadata_sha256": digests["metadata_sha256"],
        "record_evidence_sha256": digests["record_evidence_sha256"],
        "N": population.get("N"),
        "family_size": family.get("m"),
        "clues": len(report.get("clues") or []),
    }


def _verify_recorded_evidence(batch_dir, name, evidence):
    """A succeeded stage is skipped only when its artifact still matches."""
    relative = evidence.get("artifact") if isinstance(evidence, dict) else None
    recorded = evidence.get("sha256") if isinstance(evidence, dict) else None
    if not isinstance(relative, str) or not isinstance(recorded, str):
        raise VerificationError(
            f"stage {name} has malformed artifact evidence; batch is damaged")
    path = Path(batch_dir) / relative
    if not path.is_file():
        raise VerificationError(
            f"stage {name} recorded success but its artifact {relative} is "
            "missing; the stage is not re-run and the batch is left intact")
    actual = _sha256_file(path)
    if actual != recorded:
        raise VerificationError(
            f"stage {name} artifact {relative} does not match its recorded "
            f"SHA256 (recorded {recorded}, actual {actual}); preserved "
            "unchanged and not re-run")
    return evidence


# ---------------------------------------------------------------------------
# Stage execution
# ---------------------------------------------------------------------------

def _barrier(name):
    """Deterministic test-only pause exactly in the commit/ledger gap."""
    if os.environ.get(ENV_BARRIER) != name:
        return
    sys.stdout.write(json.dumps({"barrier": name}, sort_keys=True) + "\n")
    sys.stdout.flush()
    os._exit(75)


def _run_stage(batch_dir, plan, name):
    """Execute one stage by calling the shipping library entry point."""
    from .affinity import analyze as affinity_analyze
    paths = _batch_paths(batch_dir)
    catalog = paths["catalog"]
    warehouse = paths["warehouse"]
    if name == STAGE_SYNC:
        return run_sync(
            plan["endpoint"], catalog, start_date=plan["start_text"],
            until=plan["until_text"], overlap_days=plan["overlap_days"],
            max_pages=plan["max_pages"])
    if name == STAGE_WAREHOUSE:
        return build_warehouse(
            catalog, paths["frozen_taxonomy"],
            plan["warehouse_start_text"], plan["warehouse_end_text"],
            warehouse)
    if name.startswith(REPORT_STAGE_PREFIX):
        day = name[len(REPORT_STAGE_PREFIX):]
        report_dir = paths["reports"]
        report_dir.mkdir(parents=True, exist_ok=True)
        report = affinity_analyze(warehouse, paths["frozen_affinity"], day)
        publish_report(warehouse, paths["frozen_affinity"],
                       report_dir / f"{day}.json", report)
        return {"date": day}
    raise BatchError(f"unknown stage {name!r}")


def _facts_digest_or_none(paths):
    """Logical formal-facts digest, or ``None`` before any database exists.

    A sync database that exists but cannot be revalidated is a hard
    verification failure, never an empty success.
    """
    database = paths["catalog"]
    if not database.exists() or database.stat().st_size == 0:
        return None
    try:
        facts = read_formal_facts(database)
    except (WarehouseError, OSError, sqlite3.DatabaseError) as exc:
        raise VerificationError(
            f"{database} cannot be revalidated as formal facts ({exc}); "
            "preserved unchanged") from exc
    return source_facts_digest(facts)


def _frozen_digest(paths, kind):
    return _sha256_bytes(Path(paths[kind]).read_bytes())


def _stage_evidence(batch_dir, plan, name):
    """Derive artifact evidence directly from committed/published artifacts.

    Used both after a stage call returns and when reattaching a stage left
    ``running`` by a crashed process. Every branch revalidates content; file
    existence alone is never treated as success. ``None`` means this stage
    has not published anything yet.
    """
    paths = _batch_paths(batch_dir)
    if name == STAGE_SYNC:
        return _sync_evidence_if_complete(paths, plan)
    if name == STAGE_WAREHOUSE:
        facts_digest = _facts_digest_or_none(paths)
        if facts_digest is None:
            return None
        return _warehouse_evidence_if_present(
            paths, plan, facts_digest,
            _frozen_digest(paths, "frozen_taxonomy"))
    if name.startswith(REPORT_STAGE_PREFIX):
        facts_digest = _facts_digest_or_none(paths)
        if facts_digest is None:
            return None
        return _report_evidence_if_present(
            paths, plan, name[len(REPORT_STAGE_PREFIX):],
            facts_digest, _frozen_digest(paths, "frozen_taxonomy"),
            _frozen_digest(paths, "frozen_affinity"))
    raise BatchError(f"unknown stage {name!r}")


def _set_stage(ledger, name, state, error=None, evidence=None):
    with ledger:
        if evidence is not None:
            ledger.execute(
                "UPDATE stage SET state=?, last_error=?, evidence_json=?, "
                "updated_at=? WHERE name=?",
                (state, error, _stable_json(evidence), _now(), name))
        elif state == STATE_FAILED:
            ledger.execute(
                "UPDATE stage SET state=?, last_error=?, updated_at=? "
                "WHERE name=?", (state, error, _now(), name))
        elif error is None and state == STATE_RUNNING:
            ledger.execute(
                "UPDATE stage SET state=?, last_error=NULL, updated_at=? "
                "WHERE name=?", (state, _now(), name))
        else:
            ledger.execute(
                "UPDATE stage SET state=?, updated_at=? WHERE name=?",
                (state, _now(), name))


def _record_attempt(ledger, name, attempt_no, outcome, error=None):
    with ledger:
        ledger.execute(
            "UPDATE attempt SET finished_at=?, outcome=?, error=? "
            "WHERE stage=? AND attempt_no=?",
            (_now(), outcome, error, name, attempt_no))
        ledger.execute(
            "UPDATE stage SET attempted=attempted+1 WHERE name=?", (name,))


def _log_recovery(ledger, stage, action, detail=None):
    """Persist a recovery decision next to the attempts it explains."""
    with ledger:
        ledger.execute(
            "INSERT INTO recovery_event (stage, action, detail, recorded_at) "
            "VALUES (?,?,?,?)", (stage, action, detail, _now()))


def _block_downstream(ledger, failed_name):
    """A failed stage blocks every later stage until it is retried."""
    rows = ledger.execute(
        "SELECT name, position, state FROM stage ORDER BY position").fetchall()
    failed_position = next(row["position"] for row in rows
                           if row["name"] == failed_name)
    blocked = []
    with ledger:
        for row in rows:
            if row["position"] <= failed_position:
                continue
            if row["state"] in (STATE_PENDING, STATE_RUNNING):
                ledger.execute(
                    "UPDATE stage SET state=?, last_error=?, updated_at=? "
                    "WHERE name=?",
                    (STATE_BLOCKED,
                     f"blocked by failed upstream stage {failed_name}",
                     _now(), row["name"]))
                blocked.append(row["name"])
    return blocked


def _reopen_failed_stages(ledger):
    """Requeue only stages left failed/blocked; successes are never re-run.

    Called on every explicit ``run``. Within one run a failure immediately
    blocks downstream stages; a later explicit run re-attempts the failed
    stage (and the unexecuted stages it blocked) while every verified
    ``succeeded`` stage is skipped. Report tasks therefore retry individually:
    a successful report is never recomputed, and a failed one is re-attempted
    alone. Retrying the sync stage cannot silently reopen a window -- the
    sync machine itself refuses until an explicit ``discard`` clears a failed
    window.
    """
    requeued = []
    with ledger:
        for row in ledger.execute(
                "SELECT name, state FROM stage ORDER BY position").fetchall():
            # A verified success is never re-queued, even if an operator
            # marked it failed; it is re-verified and skipped instead.
            if row["state"] in (STATE_FAILED, STATE_BLOCKED):
                # The previous error stays on the row (and in the attempt
                # history) until this new attempt replaces it.
                ledger.execute(
                    "UPDATE stage SET state=?, updated_at=? WHERE name=?",
                    (STATE_PENDING, _now(), row["name"]))
                ledger.execute(
                    "INSERT INTO recovery_event "
                    "(stage, action, detail, recorded_at) VALUES (?,?,?,?)",
                    (row["name"], "requeued", row["state"], _now()))
                requeued.append(row["name"])
    return requeued


def _finalize_interrupted(ledger, name, outcome):
    """Close attempt rows left open by a process that died mid-stage."""
    with ledger:
        rows = ledger.execute(
            "SELECT attempt_no FROM attempt WHERE stage=? AND outcome='running'",
            (name,)).fetchall()
        for row in rows:
            ledger.execute(
                "UPDATE attempt SET finished_at=?, outcome=? "
                "WHERE stage=? AND attempt_no=?",
                (_now(), outcome, name, row["attempt_no"]))
        if rows:
            ledger.execute(
                "UPDATE stage SET attempted=attempted+? WHERE name=?",
                (len(rows), name))


def _reconcile_manifest(ledger, batch_dir, plan, metadata, paths, state):
    """Reattach a manifest published in the commit/ledger gap, or resume it.

    The manifest is fully derived from the upstream evidence, so a published
    file is provable: recompute the exact bytes and compare. An existing file
    that differs is foreign/damaged and fails the stage instead of being
    overwritten.
    """
    path = paths["manifest"]
    if not path.exists():
        if state == STATE_RUNNING:
            _set_stage(ledger, STAGE_MANIFEST, STATE_PENDING)
            _log_recovery(ledger, STAGE_MANIFEST, "resumed")
            return [{"stage": STAGE_MANIFEST, "action": "resumed"}]
        return []
    if any(stage["name"] != STAGE_MANIFEST
           and stage["state"] != STATE_SUCCEEDED for stage in _stages(ledger)):
        message = (
            f"{path} exists while an upstream stage is not succeeded; "
            "preserved unchanged and not adopted")
        _set_stage(ledger, STAGE_MANIFEST, STATE_FAILED, error=message)
        _log_recovery(ledger, STAGE_MANIFEST, "failed", message)
        return [{"stage": STAGE_MANIFEST, "action": "failed",
                 "error": message}]
    row = ledger.execute(
        "SELECT attempted FROM stage WHERE name=?",
        (STAGE_MANIFEST,)).fetchone()
    try:
        expected = _manifest_bytes(
            ledger, plan, metadata, row["attempted"])
    except BatchError as exc:
        _set_stage(ledger, STAGE_MANIFEST, STATE_FAILED, error=str(exc))
        _log_recovery(ledger, STAGE_MANIFEST, "failed", str(exc))
        return [{"stage": STAGE_MANIFEST, "action": "failed",
                 "error": str(exc)}]
    actual = path.read_bytes()
    if actual != expected:
        message = (
            f"{path} does not equal the manifest derived from this batch's "
            "verified stage evidence; preserved unchanged")
        _set_stage(ledger, STAGE_MANIFEST, STATE_FAILED, error=message)
        _log_recovery(ledger, STAGE_MANIFEST, "failed", message)
        return [{"stage": STAGE_MANIFEST, "action": "failed",
                 "error": message}]
    evidence = {
        "artifact": _LAYOUT["manifest"],
        "sha256": _sha256_bytes(actual),
        "stages": json.loads(actual.decode("utf-8"))["stage_order"],
    }
    _set_stage(ledger, STAGE_MANIFEST, STATE_SUCCEEDED, evidence=evidence)
    _log_recovery(ledger, STAGE_MANIFEST, "reattached", evidence["sha256"])
    return [{"stage": STAGE_MANIFEST, "action": "reattached"}]


def _reconcile(ledger, batch_dir, plan, metadata, paths):
    """Bring the ledger in line with artifacts this batch can prove.

    Returns ``(notes, halted_stage_or_None)``. Called under the lock before
    executing anything:

    * ``succeeded`` rows are re-verified against their recorded digest; a
      damaged artifact fails loudly and is never re-run or overwritten;
    * a ``running``/``pending`` row whose artifact already proves this
      batch's identity and logical content (completed sync window, replaced
      warehouse, published report) is reattached and never re-executed;
    * a ``running`` row with no committed artifact returns to ``pending`` and
      resumes normally;
    * a foreign or corrupt artifact fails the stage and blocks downstream
      stages instead of being overwritten;
    * a manifest published in its own commit/ledger gap is accepted only when
      its bytes equal the manifest derived from the verified upstream
      evidence.
    """
    notes = []
    states = {row["name"]: row["state"] for row in _stages(ledger)}
    for row in _stages(ledger):
        name, state = row["name"], row["state"]
        if state == STATE_RUNNING:
            _finalize_interrupted(ledger, name, "interrupted")
        if state == STATE_SUCCEEDED:
            record = json.loads(row["evidence_json"]) if row["evidence_json"] else None
            _verify_recorded_evidence(batch_dir, name, record)
            continue
        if state not in (STATE_PENDING, STATE_RUNNING):
            continue
        if name == STAGE_MANIFEST:
            notes.extend(_reconcile_manifest(
                ledger, batch_dir, plan, metadata, paths, state))
            continue
        if name.startswith(REPORT_STAGE_PREFIX) \
                and states.get(STAGE_WAREHOUSE) != STATE_SUCCEEDED:
            # A report is only meaningful against an accepted warehouse; do
            # not adopt one while the warehouse itself is unproven.
            continue
        try:
            evidence = _stage_evidence(batch_dir, plan, name)
        except (VerificationError, BatchError, OSError,
                sqlite3.DatabaseError) as exc:
            _set_stage(ledger, name, STATE_FAILED, error=str(exc))
            _block_downstream(ledger, name)
            _log_recovery(ledger, name, "failed", str(exc))
            notes.append({"stage": name, "action": "failed",
                          "error": str(exc)})
            return notes, name
        if evidence is None:
            if state == STATE_RUNNING:
                _set_stage(ledger, name, STATE_PENDING)
                _log_recovery(ledger, name, "resumed")
                notes.append({"stage": name, "action": "resumed"})
            continue
        # A committed sync window, a replaced warehouse or a published report
        # whose ledger row never turned green is reattached after the identity
        # and content checks above -- never re-executed. In particular a
        # completed sync window is not reopened just because the ledger still
        # says 'running' or 'pending'. The interrupted attempt was already
        # closed at the top of the loop.
        _set_stage(ledger, name, STATE_SUCCEEDED, evidence=evidence)
        _log_recovery(ledger, name, "reattached",
                      evidence.get("sha256") if evidence else None)
        notes.append({"stage": name, "action": "reattached"})
    return notes, None


# ---------------------------------------------------------------------------
# Manifest: only after every other stage succeeded
# ---------------------------------------------------------------------------

MANIFEST_FORMAT = "catalog_harvest_batch_manifest"
MANIFEST_FORMAT_VERSION = 1


def _manifest_bytes(ledger, plan, metadata, attempt_no):
    stages = _stages(ledger)
    if any(row["name"] != STAGE_MANIFEST and row["state"] != STATE_SUCCEEDED
           for row in stages):
        raise BatchError(
            "a complete manifest requires every upstream stage to have "
            "succeeded; refusing to publish a partial manifest")
    evidence = {}
    for row in stages:
        if row["name"] == STAGE_MANIFEST:
            continue
        evidence[row["name"]] = json.loads(row["evidence_json"])
    sync_record = evidence[STAGE_SYNC]
    warehouse_record = evidence[STAGE_WAREHOUSE]
    reports = []
    for day in plan["report_dates"]:
        record = evidence[REPORT_STAGE_PREFIX + day]
        reports.append({
            "date": day,
            "path": record["artifact"],
            "sha256": record["sha256"],
            "N": record["N"],
            "family_size": record["family_size"],
            "clues": record["clues"],
        })
    manifest = {
        "format": MANIFEST_FORMAT,
        "format_version": MANIFEST_FORMAT_VERSION,
        "run_id": metadata["run_id"],
        "plan_sha256": metadata["plan_sha256"],
        "plan": {
            "schema_version": plan["schema_version"],
            "sync": {
                "endpoint": plan["endpoint"],
                "start_date": plan["start_text"],
                "until": plan["until_text"],
                "overlap_days": plan["overlap_days"],
                "max_pages": plan["max_pages"],
            },
            "warehouse": {
                "start": plan["warehouse_start_text"],
                "end": plan["warehouse_end_text"],
                "taxonomy_sha256":
                    metadata["inputs"]["taxonomy"]["sha256"],
                "affinity_plan_sha256":
                    metadata["inputs"]["affinity"]["sha256"],
            },
            "reports": list(plan["report_dates"]),
        },
        "stage_order": [row["name"] for row in stages],
        "stage_attempts": {
            row["name"]: (attempt_no if row["name"] == STAGE_MANIFEST
                          else row["attempted"])
            for row in stages},
        "inputs_sha256": {
            kind: metadata["inputs"][kind]["sha256"]
            for kind in ("plan", "taxonomy", "affinity")},
        "sync": sync_record,
        "warehouse": warehouse_record,
        "reports": reports,
    }
    return _stable_json(manifest).encode("utf-8")


# ---------------------------------------------------------------------------
# Public entry point: run (or resume) one batch
# ---------------------------------------------------------------------------

def run_batch(plan_path, workdir, run_id, on_stage=None):
    """Run or resume a batch; return a JSON-serialisable summary.

    ``on_stage(name, state)`` is a test-only observation callback fired after
    every ledger transition; it is not part of the operational contract.
    """
    run_id = _validate_run_id(run_id)
    workdir = Path(workdir)
    batch_dir = workdir / run_id
    try:
        plan_bytes = Path(plan_path).read_bytes()
    except FileNotFoundError as exc:
        raise BatchError(f"plan not found: {plan_path}") from exc
    plan_digest = _sha256_bytes(plan_bytes)
    plan = parse_plan(plan_bytes)

    if not batch_dir.exists():
        _create_batch(plan_path, workdir, run_id)
        if not batch_dir.exists():
            raise BatchError(
                f"batch directory {batch_dir} could not be created")
    if os.path.islink(batch_dir) or not batch_dir.is_dir():
        raise BatchError(f"batch path {batch_dir} is not a real directory")

    metadata = _read_metadata(batch_dir)
    if metadata["run_id"] != run_id:
        raise BatchError(
            f"batch metadata run_id {metadata['run_id']!r} does not match "
            f"{run_id!r}; refusing to continue")
    if metadata["plan_sha256"] != plan_digest:
        raise BatchError(
            "the supplied plan no longer matches the frozen plan bytes of "
            f"this batch ({metadata['plan_sha256']} vs {plan_digest}); use a "
            "new run-id for a changed plan")
    frozen = _frozen_inputs(metadata, batch_dir)
    if frozen["plan"] != plan_bytes:
        raise BatchError(
            "frozen plan bytes are inconsistent; batch is damaged")
    _check_external_sources(metadata, frozen)
    frozen_plan = parse_plan(frozen["plan"])

    paths = _batch_paths(batch_dir)
    with _BatchLock(paths["lock"]):
        return _run_locked(batch_dir, frozen_plan, paths, metadata, on_stage)


def _run_locked(batch_dir, plan, paths, metadata, on_stage):
    ledger = _connect_ledger(paths["ledger"], writable=True)
    try:
        ledger.executescript(LEDGER_DDL)
        _ensure_stage_rows(ledger, plan, metadata)
        # Retry is explicit: a previous run's failed/blocked stages (and the
        # unexecuted downstream they blocked) return to pending. Verified
        # successes are never reset. This happens before reconciliation so a
        # retried stage whose artifact actually committed can still be
        # reattached instead of executed again.
        retried = _reopen_failed_stages(ledger)
        try:
            notes, halted = _reconcile(ledger, batch_dir, plan, metadata, paths)
        except VerificationError as exc:
            raise BatchError(f"batch cannot resume: {exc}") from exc
        if retried:
            notes.append({"requeued": retried})
        if halted is not None:
            raise StageFailure(
                _last_error(ledger, halted), _summary(batch_dir, ledger))
        summary = _drive(ledger, batch_dir, plan, paths, metadata, on_stage)
        if notes:
            summary["recovery"] = notes
        return summary
    finally:
        ledger.close()


def _ensure_stage_rows(ledger, plan, metadata):
    """Reconcile the ledger with the frozen plan (never drops history)."""
    names = plan_stage_names(plan)
    existing = {row["name"] for row in _stages(ledger)}
    unexpected = existing - set(names)
    if unexpected:
        raise BatchError(
            "ledger holds stages not present in the frozen plan: "
            + ", ".join(sorted(unexpected)))
    digests = {
        "plan": metadata["inputs"]["plan"]["sha256"],
        "taxonomy": metadata["inputs"]["taxonomy"]["sha256"],
        "affinity": metadata["inputs"]["affinity"]["sha256"],
    }
    with ledger:
        for position, name in enumerate(names):
            expected = _stable_json(_intent_for(name, plan, digests))
            row = ledger.execute(
                "SELECT intent_json FROM stage WHERE name=?", (name,)).fetchone()
            if row is None:
                ledger.execute(
                    "INSERT INTO stage (name, position, intent_json, state, "
                    "attempted, updated_at) VALUES (?,?,?,?,0,?)",
                    (name, position, expected, STATE_PENDING, _now()))
            elif row["intent_json"] != expected:
                raise BatchError(
                    f"recorded intent for stage {name} differs from the "
                    "frozen plan; the batch directory is damaged")


def _drive(ledger, batch_dir, plan, paths, metadata, on_stage):
    while True:
        row = _next_runnable(ledger)
        if row is None:
            break
        outcome = _execute_one(
            ledger, batch_dir, plan, paths, metadata, row["name"], on_stage)
        if outcome == "failed":
            raise StageFailure(
                _last_error(ledger, row["name"]),
                _summary(batch_dir, ledger))
    if _complete(ledger):
        return _summary(batch_dir, ledger, {"result": "complete"})
    failed = next((row for row in _stages(ledger)
                   if row["state"] == STATE_FAILED), None)
    if failed is not None:
        raise StageFailure(_last_error(ledger, failed["name"]),
                           _summary(batch_dir, ledger))
    raise BatchError(
        "batch stopped: a stage is blocked with no recorded failure; inspect "
        "'status'")


def _next_runnable(ledger):
    """The earliest non-final stage whose immediate predecessor succeeded.

    Execution is strictly ordered; a non-succeeded predecessor (pending,
    running, failed, blocked) means nothing downstream may start.
    """
    rows = ledger.execute(
        "SELECT name, position, state FROM stage ORDER BY position").fetchall()
    for index, row in enumerate(rows):
        if row["state"] in (STATE_SUCCEEDED, STATE_FAILED, STATE_BLOCKED):
            continue
        if index == 0 or rows[index - 1]["state"] == STATE_SUCCEEDED:
            return row
        return None
    return None


def _stages(ledger):
    return ledger.execute(
        "SELECT name, position, state, attempted, last_error, evidence_json "
        "FROM stage ORDER BY position").fetchall()


def _complete(ledger):
    return all(row["state"] == STATE_SUCCEEDED for row in _stages(ledger))


def _summary(batch_dir, ledger, extra=None):
    attempts = {
        row["name"]: row["attempted"] for row in _stages(ledger)}
    summary = {
        "run_id": ledger.execute(
            "SELECT value FROM batch_meta WHERE key='run_id'").fetchone()[0],
        "batch_dir": str(batch_dir),
        "complete": _complete(ledger),
        "stages": [
            {
                "name": row["name"],
                "state": row["state"],
                "attempts": attempts[row["name"]],
                "last_error": row["last_error"],
            }
            for row in _stages(ledger)],
    }
    if extra:
        summary.update(extra)
    return summary


def _last_error(ledger, name):
    row = ledger.execute(
        "SELECT last_error FROM stage WHERE name=?", (name,)).fetchone()
    return row["last_error"] or f"stage {name} failed"


def _execute_one(ledger, batch_dir, plan, paths, metadata, name, on_stage):
    if on_stage:
        on_stage(name, "start")
    _set_stage(ledger, name, STATE_RUNNING, error=None)
    if on_stage:
        on_stage(name, STATE_RUNNING)
    attempt_no = ledger.execute(
        "SELECT coalesce(max(attempt_no),0)+1 FROM attempt WHERE stage=?",
        (name,)).fetchone()[0]
    with ledger:
        ledger.execute(
            "INSERT INTO attempt (stage, attempt_no, started_at, outcome) "
            "VALUES (?,?,?,?)", (name, attempt_no, _now(), "running"))
    try:
        _barrier("before-" + name)
        if os.environ.get(ENV_FAIL_STAGE) == name:
            raise BatchError(
                f"injected stage failure for {name} (test hook)")
        if name == STAGE_MANIFEST:
            # The manifest is derived from the ledger's verified evidence and
            # published only when every other stage has succeeded.
            manifest = _manifest_bytes(
                ledger, plan, metadata, attempt_no)
            _atomic_write(paths["manifest"], manifest)
            _barrier("after-" + name)
            evidence = {
                "artifact": _LAYOUT["manifest"],
                "sha256": _sha256_bytes(manifest),
                "stages": json.loads(manifest.decode("utf-8"))["stage_order"],
            }
        else:
            _run_stage(batch_dir, plan, name)
            _barrier("after-" + name)
            evidence = _stage_evidence(batch_dir, plan, name)
            if evidence is None:
                raise BatchError(
                    f"stage {name} returned without a verifiable artifact")
        _record_attempt(ledger, name, attempt_no, "succeeded")
        _set_stage(ledger, name, STATE_SUCCEEDED, evidence=evidence)
        if on_stage:
            on_stage(name, STATE_SUCCEEDED)
        return "succeeded"
    except (SyncError, WarehouseError, BatchError, AffinityError, OSError,
            sqlite3.DatabaseError, RuntimeError, ValueError) as exc:
        # WindowFailed is a SyncError; every failure above is attributed to
        # this stage and blocks the remainder of the batch.
        message = f"{type(exc).__name__}: {exc}"
        _record_attempt(ledger, name, attempt_no, "failed", message)
        _set_stage(ledger, name, STATE_FAILED, error=message)
        _block_downstream(ledger, name)
        if on_stage:
            on_stage(name, STATE_FAILED)
        return "failed"


# ---------------------------------------------------------------------------
# Read-only status and the run/status CLI
# ---------------------------------------------------------------------------

def _read_plan_from_batch(batch_dir, metadata):
    record = (metadata.get("inputs") or {}).get("plan")
    if not isinstance(record, dict) or "path" not in record:
        raise BatchError("batch metadata has no frozen plan record")
    path = Path(batch_dir) / record["path"]
    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise BatchError("frozen plan is missing; batch is damaged") from exc
    if _sha256_bytes(raw) != record.get("sha256"):
        raise BatchError("frozen plan digest does not match; batch is damaged")
    return parse_plan(raw)


def status_batch(workdir, run_id):
    """Read-only batch inspection. Never takes the lock, never networks."""
    run_id = _validate_run_id(run_id)
    batch_dir = Path(workdir) / run_id
    if not batch_dir.is_dir():
        raise BatchError(f"batch directory not found: {batch_dir}")
    metadata = _read_metadata(batch_dir)
    if metadata["run_id"] != run_id:
        raise BatchError(
            f"batch metadata run_id {metadata['run_id']!r} does not match "
            f"{run_id!r}")
    plan = _read_plan_from_batch(batch_dir, metadata)
    # Read-only validation of the frozen input copies (no writes, no lock).
    _frozen_inputs(metadata, batch_dir)
    paths = _batch_paths(batch_dir)
    try:
        ledger = _connect_ledger(paths["ledger"], writable=False)
    except sqlite3.DatabaseError as exc:
        raise BatchError(f"batch ledger is unreadable: {exc}") from exc
    try:
        stage_rows = ledger.execute(
            "SELECT name, position, intent_json, state, attempted, last_error, "
            "evidence_json, updated_at FROM stage ORDER BY position").fetchall()
        attempts = ledger.execute(
            "SELECT stage, attempt_no, started_at, finished_at, outcome, error "
            "FROM attempt ORDER BY stage, attempt_no").fetchall()
        recoveries = ledger.execute(
            "SELECT stage, action, detail, recorded_at FROM recovery_event "
            "ORDER BY id").fetchall()
        names = plan_stage_names(plan)
        recorded = [row["name"] for row in stage_rows]
        if recorded != names:
            raise BatchError(
                "batch ledger stages do not match the frozen plan; damaged")
    finally:
        ledger.close()
    manifest_path = paths["manifest"]
    manifest = None
    manifest_sha = None
    if manifest_path.is_file():
        raw = manifest_path.read_bytes()
        manifest_sha = _sha256_bytes(raw)
        try:
            manifest = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            manifest = {"unreadable": True}
    manifest_present = (
        manifest is not None and not manifest.get("unreadable")
        and manifest.get("format") == MANIFEST_FORMAT)
    # A manifest is only ever produced for a fully successful batch. Detect
    # the contradiction before reporting any per-stage view.
    # A manifest row that is still 'running' next to its published file is the
    # documented commit/ledger gap and is reported as incomplete, not corrupt.
    # An upstream stage that is not succeeded is a genuine contradiction.
    if manifest_present and any(
            row["name"] != STAGE_MANIFEST
            and row["state"] != STATE_SUCCEEDED for row in stage_rows):
        raise BatchError(
            "a delivery manifest exists although not every stage succeeded; "
            "the batch directory is inconsistent and left untouched")
    stages = []
    all_succeeded = True
    for row in stage_rows:
        evidence = json.loads(row["evidence_json"]) if row["evidence_json"] else None
        stages.append({
            "name": row["name"],
            "position": row["position"],
            "state": row["state"],
            "attempted": row["attempted"],
            "last_error": row["last_error"],
            "intent": json.loads(row["intent_json"]),
            "artifact": None if evidence is None else {
                "path": evidence.get("artifact"),
                "sha256": evidence.get("sha256"),
            },
            "updated_at": row["updated_at"],
        })
        if row["state"] != STATE_SUCCEEDED:
            all_succeeded = False
    return {
        "run_id": run_id,
        "batch_dir": str(batch_dir),
        "format": metadata.get("format"),
        "format_version": metadata.get("format_version"),
        "plan_sha256": metadata.get("plan_sha256"),
        "inputs_sha256": {
            kind: metadata["inputs"][kind]["sha256"]
            for kind in ("plan", "taxonomy", "affinity")},
        "complete": all_succeeded,
        "delivery_manifest": None if manifest is None else {
            "path": _LAYOUT["manifest"],
            "sha256": manifest_sha,
            "valid": manifest_present,
        },
        "stages": stages,
        "attempts": [dict(row) for row in attempts],
        "recovery_events": [dict(row) for row in recoveries],
    }


def _print_status(info):
    print(f"run id       : {info['run_id']}")
    print(f"batch dir    : {info['batch_dir']}")
    print(f"plan sha256  : {info['plan_sha256']}")
    print(f"complete     : {'yes' if info['complete'] else 'no'}")
    manifest = info["delivery_manifest"]
    if manifest is None:
        print("manifest     : (not produced; batches need every stage)")
    else:
        print(f"manifest     : {manifest['path']} sha256={manifest['sha256']}"
              f" valid={manifest['valid']}")
    for stage in info["stages"]:
        print(f"[{stage['state']:<9}] {stage['name']:<16} attempts={stage['attempted']}")
        if stage["last_error"]:
            print(f"            error: {stage['last_error']}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m catalog_harvest.batch",
        description="Persistent sync -> warehouse -> report batch orchestration")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser(
        "run", help="run or resume one frozen batch")
    p_run.add_argument("--plan", required=True)
    p_run.add_argument("--workdir", default="run/batches")
    p_run.add_argument("--run-id", required=True, dest="run_id")
    p_run.add_argument("--json", action="store_true")

    p_status = sub.add_parser(
        "status", help="read-only batch status (never takes the lock or nets)")
    p_status.add_argument("--workdir", default="run/batches")
    p_status.add_argument("--run-id", required=True, dest="run_id")
    p_status.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            result = run_batch(args.plan, args.workdir, args.run_id)
            if args.json:
                print(json.dumps(result, sort_keys=True, ensure_ascii=False))
            else:
                _print_status(status_batch(args.workdir, args.run_id))
            if not result.get("complete"):
                parser.exit(
                    3, "batch is not complete; inspect status and retry\n")
        else:
            info = status_batch(args.workdir, args.run_id)
            if args.json:
                print(json.dumps(info, sort_keys=True, ensure_ascii=False))
            else:
                _print_status(info)
    except StageFailure as exc:
        summary = exc.summary
        if args.json:
            print(json.dumps(summary, sort_keys=True, ensure_ascii=False))
        else:
            _print_status(status_batch(args.workdir, args.run_id))
        parser.exit(3, f"batch {args.command} stopped: {exc}\n")
    except (BatchError, OSError, sqlite3.DatabaseError) as exc:
        parser.exit(2, f"batch {args.command} failed: {exc}\n")


if __name__ == "__main__":
    main()
