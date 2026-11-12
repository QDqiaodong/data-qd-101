"""Resumable incremental date-window harvest for one bound loopback endpoint.

The full harvest in :mod:`catalog_harvest.harvest` keeps everything in memory
and commits once at the end. This module instead drives one frozen OAI-PMH
date window at a time and checkpoints every confirmed page into separate
staging tables inside the same SQLite database. The official ``pages``,
``revisions`` and ``current_records`` view are only touched inside the single
publish transaction after the whole window traversal has succeeded.

Recovery boundary
-----------------
* A checkpoint is the SQLite transaction that stores one fully validated
  page: raw XML, parsed revisions, page order, the request kind used, the
  next opaque token and the per-traversal token cycle set. Only after that
  transaction commits may the next HTTP request be sent.
* On a new process the machine resumes from the persisted next token, so
  checkpoint-confirmed pages are never requested again. If the terminal page
  (empty resumption token) is already persisted, publish runs without any
  HTTP traffic.
* The watermark W is the *date upper bound of a fully traversed window*, not
  the maximum record datestamp observed. Empty windows advance W as well.
* A ``badResumptionToken`` continuation failure may restart the current
  window exactly once. The budget is persisted; restarting discards every
  staged page/revision/token of the abandoned traversal and replays the
  identical frozen ``from``/``until`` first request. Nothing from the old
  traversal is spliced into the new one.
* Hard content failures (bad XML, out-of-window datestamps, revision
  conflicts, token cycles, exhausted page/restart budgets) put the window
  into ``failed``; further ``run`` calls refuse until ``discard`` clears the
  unpublished work. HTTP/OS failures leave the last complete checkpoint
  intact and the window resumable.

Only the Python standard library and SQLite are used.
"""
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .harvest import DDL as FULL_DDL, is_managed_incremental
from .protocol import canonical, parse_iso_date, parse_response
from .transport import window_fetcher

SYNC_DDL = """
CREATE TABLE IF NOT EXISTS sync_meta (
 key TEXT PRIMARY KEY,
 value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_window (
 window_id INTEGER PRIMARY KEY AUTOINCREMENT,
 state TEXT NOT NULL CHECK (state IN ('active','failed','complete')),
 stage TEXT NOT NULL CHECK (stage IN ('fetching','publishing','done')),
 endpoint TEXT NOT NULL,
 window_from TEXT NOT NULL,
 window_until TEXT NOT NULL,
 overlap_days INTEGER NOT NULL,
 max_pages INTEGER NOT NULL,
 traversal INTEGER NOT NULL,
 restarts_used INTEGER NOT NULL,
 pages_confirmed INTEGER NOT NULL,
 records_confirmed INTEGER NOT NULL,
 next_token TEXT,
 last_error TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_stage_pages (
 window_id INTEGER NOT NULL,
 traversal INTEGER NOT NULL,
 page_seq INTEGER NOT NULL,
 sha256 TEXT NOT NULL,
 xml_body BLOB NOT NULL,
 record_count INTEGER NOT NULL,
 request_kind TEXT NOT NULL CHECK (request_kind IN ('first','resume')),
 next_token TEXT,
 PRIMARY KEY (window_id, traversal, page_seq)
);
CREATE TABLE IF NOT EXISTS sync_stage_revisions (
 window_id INTEGER NOT NULL,
 traversal INTEGER NOT NULL,
 identifier TEXT NOT NULL,
 datestamp TEXT NOT NULL,
 deleted INTEGER NOT NULL,
 title TEXT,
 subjects_json TEXT NOT NULL,
 canonical_json TEXT NOT NULL,
 page_sha256 TEXT NOT NULL,
 page_seq INTEGER NOT NULL,
 PRIMARY KEY (window_id, traversal, identifier, datestamp)
);
CREATE TABLE IF NOT EXISTS sync_seen_tokens (
 window_id INTEGER NOT NULL,
 traversal INTEGER NOT NULL,
 token TEXT NOT NULL,
 seen_at_page INTEGER NOT NULL,
 PRIMARY KEY (window_id, traversal, token)
);
"""

META_ENDPOINT = "endpoint"
META_BASE = "base_date"
META_WATERMARK = "watermark"


class SyncError(ValueError):
    """Validation or protocol failure that the operator must address."""


class WindowFailed(SyncError):
    """Raised after the open window has been persisted in state='failed'."""


def _now():
    return datetime.now(timezone.utc).isoformat()


def _connect(database, writable=True):
    path = Path(database)
    if writable:
        path.parent.mkdir(parents=True, exist_ok=True)
    mode = "rwc" if writable else "ro"
    db = sqlite3.connect(f"file:{path}?mode={mode}", uri=True)
    db.execute("PRAGMA foreign_keys=ON")
    return db


def _meta(db):
    return dict(db.execute("SELECT key, value FROM sync_meta").fetchall())


def _latest_window(db):
    return db.execute(
        "SELECT * FROM sync_window ORDER BY window_id DESC LIMIT 1").fetchone()


def _window_cols(db, window_id):
    cols = [r[1] for r in db.execute("PRAGMA table_info(sync_window)").fetchall()]
    row = db.execute("SELECT * FROM sync_window WHERE window_id=?", (window_id,)).fetchone()
    return dict(zip(cols, row))


def _formal_has_rows(db):
    for table in ("pages", "revisions"):
        exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        if exists and db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]:
            return True
    return False


def _validate_cli_params(overlap_days, max_pages):
    if not isinstance(overlap_days, int) or not 1 <= overlap_days <= 365:
        raise SyncError("overlap-days must be an integer in 1..365")
    if not isinstance(max_pages, int) or not 1 <= max_pages <= 1000:
        raise SyncError("page limit must be an integer in 1..1000")


def _open_or_bind(db, endpoint, start_date, until_date, overlap_days, max_pages):
    """Validate binding rules and return (meta, base_date, window_or_None).

    Runs entirely before any HTTP request.
    """
    db.executescript(FULL_DDL)
    db.executescript(SYNC_DDL)
    meta = _meta(db)
    window = _latest_window(db)
    if window is not None and window[1] == "complete":
        window = None
    if META_ENDPOINT in meta:
        if meta[META_ENDPOINT] != endpoint:
            raise SyncError(
                "database is already bound to endpoint "
                f"{meta[META_ENDPOINT]!r}; changing endpoints is rejected")
        base = parse_iso_date(meta[META_BASE])
        if start_date is not None and start_date != base:
            raise SyncError(
                f"fixed start date B is {base.isoformat()}; a different start-date is rejected")
    else:
        if start_date is None or until_date is None:
            raise SyncError(
                "first initialization requires both --start-date and --until")
        if _formal_has_rows(db):
            raise SyncError(
                "refusing to bind an incremental endpoint: database already contains "
                "full-harvest pages/revisions; harvest into a new database instead")
        db.execute("INSERT INTO sync_meta VALUES (?,?)", (META_ENDPOINT, endpoint))
        db.execute("INSERT INTO sync_meta VALUES (?,?)", (META_BASE, start_date.isoformat()))
        meta = _meta(db)
        base = start_date
    if window is not None:
        if window[1] == "failed":
            raise SyncError(
                "window is in failed state; run 'discard' before starting again")
        # Frozen parameters must match exactly; no recomputation on resume.
        frozen = {"endpoint": window[3], "window_from": window[4],
                  "window_until": window[5], "overlap_days": window[6],
                  "max_pages": window[7]}
        if (frozen["endpoint"] != endpoint
                or frozen["overlap_days"] != overlap_days
                or frozen["max_pages"] != max_pages
                or frozen["window_until"] != until_date.isoformat()):
            raise SyncError(
                "an unfinished window exists with frozen parameters "
                f"from={frozen['window_from']} until={frozen['window_until']} "
                f"overlap-days={frozen['overlap_days']} max-pages={frozen['max_pages']}; "
                "resume with the identical parameters or discard it first")
        if start_date is not None and start_date != base:
            raise SyncError("fixed start date B cannot be changed while a window is open")
    return meta, base, window


def _new_window_bounds(base, watermark_text, until_date, overlap_days):
    if until_date < base:
        raise SyncError("until must not be earlier than the fixed start date B")
    if watermark_text is not None:
        watermark = parse_iso_date(watermark_text)
        if until_date < watermark:
            raise SyncError(
                "until must not be earlier than the existing success watermark "
                f"({watermark.isoformat()}); equal is allowed for re-harvest")
        lower = max(base, watermark - timedelta(days=overlap_days - 1))
    else:
        lower = base
    return lower, until_date


def _mark_failed(db, window_id, message):
    db.execute(
        "UPDATE sync_window SET state='failed', last_error=?, updated_at=? "
        "WHERE window_id=?", (message, _now(), window_id))


def _record_transient(db, window_id, message):
    db.execute(
        "UPDATE sync_window SET last_error=?, updated_at=? WHERE window_id=?",
        (message, _now(), window_id))


def _stage_revision(db, win, seq, digest, record):
    """Insert one revision into staging with identity/conflict rules.

    Returns True when a new staging row was written, False when an identical
    revision was already present (de-duplicated).
    """
    window_id, traversal = win
    encoded = canonical(record)
    existing = db.execute(
        "SELECT canonical_json FROM sync_stage_revisions "
        "WHERE window_id=? AND traversal=? AND identifier=? AND datestamp=?",
        (window_id, traversal, record["identifier"], record["datestamp"])).fetchone()
    if existing is not None:
        if existing[0] != encoded:
            raise SyncError("conflicting revision payload for same identity in window")
        return False
    formal = db.execute(
        "SELECT canonical_json FROM revisions WHERE identifier=? AND datestamp=?",
        (record["identifier"], record["datestamp"])).fetchone()
    if formal is not None and formal[0] != encoded:
        raise SyncError("conflicting revision payload against already stored fact")
    db.execute(
        "INSERT INTO sync_stage_revisions VALUES (?,?,?,?,?,?,?,?,?,?)",
        (window_id, traversal, record["identifier"], record["datestamp"],
         int(record["deleted"]), record["title"], json.dumps(record["subjects"]),
         encoded, digest, seq))
    return True


def _checkpoint_page(db, win, seq, raw, records, token, request_kind):
    import hashlib
    window_id, traversal = win
    digest = hashlib.sha256(raw).hexdigest()
    with db:  # one atomic checkpoint transaction
        current = db.execute(
            "SELECT count(*) FROM sync_stage_pages WHERE window_id=? AND traversal=?",
            (window_id, traversal)).fetchone()[0]
        if current + 1 != seq:
            raise SyncError("staging page order is inconsistent; refusing to checkpoint")
        inserted_revisions = 0
        for record in records:
            if _stage_revision(db, win, seq, digest, record):
                inserted_revisions += 1
        db.execute(
            "INSERT INTO sync_stage_pages VALUES (?,?,?,?,?,?,?,?)",
            (window_id, traversal, seq, digest, raw, len(records), request_kind, token))
        if token:
            db.execute(
                "INSERT OR IGNORE INTO sync_seen_tokens VALUES (?,?,?,?)",
                (window_id, traversal, token, seq))
        db.execute(
            "UPDATE sync_window SET pages_confirmed=?, records_confirmed=?, next_token=?, "
            "updated_at=? WHERE window_id=?",
            (seq,
             db.execute(
                 "SELECT count(*) FROM sync_stage_revisions WHERE window_id=? AND traversal=?",
                 (window_id, traversal)).fetchone()[0],
             token if token else None, _now(), window_id))
    return digest, inserted_revisions


def _restart_traversal(db, window_id, win_row):
    """Discard the abandoned traversal and consume the one persisted restart."""
    traversal = win_row["traversal"]
    with db:
        for table in ("sync_stage_pages", "sync_stage_revisions", "sync_seen_tokens"):
            db.execute(f"DELETE FROM {table} WHERE window_id=? AND traversal=?",
                       (window_id, traversal))
        db.execute(
            "UPDATE sync_window SET traversal=?, restarts_used=?, pages_confirmed=0, "
            "records_confirmed=0, next_token=NULL, last_error=NULL, updated_at=? "
            "WHERE window_id=?",
            (traversal + 1, win_row["restarts_used"] + 1, _now(), window_id))


def _publish(db, window_id):
    """Merge the final traversal into official tables, atomically with W."""
    if os.environ.get("CATALOG_HARVEST_PUBLISH_FAIL"):
        raise RuntimeError("injected publish failure (test hook)")
    with db:
        win = _window_cols(db, window_id)
        if win["state"] != "active":
            raise SyncError("window is not publishable")
        traversal = win["traversal"]
        db.execute("UPDATE sync_window SET stage='publishing', updated_at=? WHERE window_id=?",
                   (_now(), window_id))
        pages = db.execute(
            "SELECT page_seq, sha256, xml_body FROM sync_stage_pages "
            "WHERE window_id=? AND traversal=? ORDER BY page_seq",
            (window_id, traversal)).fetchall()
        for _seq, digest, body in pages:
            db.execute("INSERT OR IGNORE INTO pages VALUES (?,?)", (digest, body))
        inserted = 0
        revisions = db.execute(
            "SELECT identifier, datestamp, deleted, title, subjects_json, canonical_json, "
            "page_sha256 FROM sync_stage_revisions WHERE window_id=? AND traversal=?",
            (window_id, traversal)).fetchall()
        for identifier, stamp, deleted, title, subjects_json, encoded, digest in revisions:
            formal = db.execute(
                "SELECT canonical_json FROM revisions WHERE identifier=? AND datestamp=?",
                (identifier, stamp)).fetchone()
            if formal is not None:
                if formal[0] != encoded:
                    raise SyncError("conflicting revision payload against already stored fact")
                continue
            db.execute("INSERT INTO revisions VALUES (?,?,?,?,?,?,?)",
                       (identifier, stamp, deleted, title, subjects_json, encoded, digest))
            inserted += 1
        db.execute(
            "INSERT INTO sync_meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (META_WATERMARK, win["window_until"]))
        db.execute(
            "UPDATE sync_window SET state='complete', stage='done', updated_at=? "
            "WHERE window_id=?", (_now(), window_id))
        active, deleted = db.execute(
            "SELECT sum(deleted=0), sum(deleted=1) FROM current_records").fetchone()
    return inserted, active or 0, deleted or 0


def run_sync(endpoint, database, start_date=None, until=None,
             overlap_days=2, max_pages=20):
    """Run (or resume) one incremental window. See module docstring."""
    _validate_cli_params(overlap_days, max_pages)
    try:
        if start_date is not None:
            start_date = parse_iso_date(start_date) if isinstance(start_date, str) else start_date
        if until is None:
            raise SyncError("--until is required")
        until_date = parse_iso_date(until) if isinstance(until, str) else until
    except ValueError as exc:
        raise SyncError(str(exc)) from exc

    db = _connect(database)
    stats = {"http_requests": 0, "pages_fetched_this_run": 0,
             "fetched_total": 0, "restarted": 0}
    try:
        with db:
            meta, base, existing = _open_or_bind(
                db, endpoint, start_date, until_date, overlap_days, max_pages)
        if existing is None:
            watermark = meta.get(META_WATERMARK)
            lower, upper = _new_window_bounds(base, watermark, until_date, overlap_days)
            with db:
                cur = db.execute(
                    "INSERT INTO sync_window (state, stage, endpoint, window_from, window_until, "
                    "overlap_days, max_pages, traversal, restarts_used, pages_confirmed, "
                    "records_confirmed, next_token, last_error, created_at, updated_at) "
                    "VALUES ('active','fetching',?,?,?,?,?,1,0,0,0,NULL,NULL,?,?)",
                    (endpoint, lower.isoformat(), upper.isoformat(), overlap_days, max_pages,
                     _now(), _now()))
                window_id = cur.lastrowid
        else:
            window_id = existing[0]

        win = _window_cols(db, window_id)
        fetcher = window_fetcher(endpoint)
        lower = parse_iso_date(win["window_from"])
        upper = parse_iso_date(win["window_until"])

        # Terminal page already persisted by a previous process: publish only.
        if win["pages_confirmed"] > 0 and win["next_token"] is None:
            return _finish(db, window_id, stats, empty=False)

        while True:
            win = _window_cols(db, window_id)
            traversal = win["traversal"]
            pair = (window_id, traversal)
            page_seq = win["pages_confirmed"] + 1
            try:
                if win["next_token"] is None and win["pages_confirmed"] == 0:
                    raw = fetcher.first(win["window_from"], win["window_until"])
                    request_kind = "first"
                else:
                    raw = fetcher.next_(win["next_token"])
                    request_kind = "resume"
                stats["http_requests"] += 1
            except OSError as exc:
                with db:
                    _record_transient(db, window_id, f"HTTP failure: {exc}")
                raise
            try:
                if page_seq > win["max_pages"]:
                    raise SyncError("page limit exceeded")
                kind, payload = parse_response(raw)
                if kind == "error":
                    codes = payload
                    if request_kind == "first" and codes == ["noRecordsMatch"]:
                        # Legal empty window: no ListRecords, nothing to fetch.
                        return _finish(db, window_id, stats, empty=True)
                    if request_kind == "resume" and "noRecordsMatch" in codes:
                        raise SyncError(
                            "noRecordsMatch on a continuation request is not a valid end")
                    if "badResumptionToken" in codes:
                        if request_kind == "first":
                            raise SyncError(
                                "badResumptionToken on the dated first request is rejected")
                        if win["restarts_used"] >= 1:
                            raise SyncError("restart budget exhausted")
                        _restart_traversal(db, window_id, win)
                        stats["restarted"] += 1
                        continue
                    raise SyncError("OAI protocol error: " + ",".join(codes))
                records, token = payload
                for record in records:
                    stamp = parse_iso_date(record["datestamp"])
                    if not lower <= stamp <= upper:
                        raise SyncError(
                            f"record {record['identifier']} datestamp {record['datestamp']} "
                            f"falls outside frozen window [{lower}, {upper}]")
                if token:
                    cycle = db.execute(
                        "SELECT 1 FROM sync_seen_tokens WHERE window_id=? AND traversal=? "
                        "AND token=?", (window_id, traversal, token)).fetchone()
                    if cycle is not None:
                        raise SyncError("continuation token cycle")
                _checkpoint_page(db, pair, page_seq, raw, records, token, request_kind)
                stats["pages_fetched_this_run"] += 1
                stats["fetched_total"] += 1
                if os.environ.get("CATALOG_HARVEST_EXIT_AFTER_PAGE") == str(page_seq):
                    os._exit(7)
                if (os.environ.get("CATALOG_HARVEST_EXIT_AFTER_RESTART_PAGE")
                        and stats["restarted"] > 0
                        and os.environ["CATALOG_HARVEST_EXIT_AFTER_RESTART_PAGE"]
                        == str(page_seq)):
                    os._exit(7)
                if not token:
                    if os.environ.get("CATALOG_HARVEST_EXIT_AFTER_FINAL_PAGE"):
                        os._exit(9)
                    return _finish(db, window_id, stats, empty=False)
            except SyncError as exc:
                with db:
                    _mark_failed(db, window_id, str(exc))
                raise WindowFailed(str(exc)) from exc
            except ValueError as exc:
                # XML/encoding/protocol-shape failures from parse_response.
                with db:
                    _mark_failed(db, window_id, str(exc))
                raise WindowFailed(str(exc)) from exc
    finally:
        db.close()


def _finish(db, window_id, stats, empty):
    win = _window_cols(db, window_id)
    if not empty and os.environ.get("CATALOG_HARVEST_EXIT_BEFORE_PUBLISH"):
        os._exit(8)
    inserted, active, deleted = _publish(db, window_id)
    final = _window_cols(db, window_id)
    final_pages = 0 if empty else db.execute(
        "SELECT count(*) FROM sync_stage_pages WHERE window_id=? AND traversal=?",
        (window_id, final["traversal"])).fetchone()[0]
    records_seen = 0 if empty else db.execute(
        "SELECT coalesce(sum(record_count),0) FROM sync_stage_pages "
        "WHERE window_id=? AND traversal=?", (window_id, final["traversal"])).fetchone()[0]
    # Pages confirmed by earlier processes/traversals but not fetched now.
    recovered = max(0, final_pages - stats["fetched_total"])
    return {
        "endpoint": final["endpoint"],
        "window_from": final["window_from"],
        "window_until": final["window_until"],
        "watermark": final["window_until"],
        "state": "complete",
        "empty_window": bool(empty),
        "pages": final_pages,
        "records_seen": records_seen,
        "inserted_revisions": inserted,
        "active_records": active,
        "deleted_records": deleted,
        "recovered_pages": recovered,
        "pages_fetched_this_run": stats["fetched_total"],
        "http_requests_this_run": stats["http_requests"],
        "traversal_restarts": stats["restarted"],
    }


def status(database):
    """Read-only local state inspection; never opens a network connection."""
    path = Path(database)
    if not path.exists():
        raise SyncError(f"database not found: {path}")
    db = _connect(database, writable=False)
    try:
        tables = {r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "sync_meta" not in tables:
            return {"bound": False, "managed": False}
        meta = _meta(db)
        result = {
            "bound": True,
            "managed": True,
            "endpoint": meta.get(META_ENDPOINT),
            "base_date": meta.get(META_BASE),
            "watermark": meta.get(META_WATERMARK),
            "unfinished_window": None,
        }
        row = _latest_window(db)
        if row is not None and row[1] != "complete":
            cols = [r[1] for r in db.execute("PRAGMA table_info(sync_window)")]
            win = dict(zip(cols, row))
            result["unfinished_window"] = {
                "window_id": win["window_id"],
                "state": win["state"],
                "stage": win["stage"],
                "window_from": win["window_from"],
                "window_until": win["window_until"],
                "overlap_days": win["overlap_days"],
                "max_pages": win["max_pages"],
                "traversal": win["traversal"],
                "restarts_used": win["restarts_used"],
                "pages_confirmed": win["pages_confirmed"],
                "records_confirmed": win["records_confirmed"],
                "next_token": win["next_token"],
                "last_error": win["last_error"],
            }
        return result
    finally:
        db.close()


def discard(database):
    """Drop unpublished window work. No HTTP; official facts and W are kept."""
    path = Path(database)
    if not path.exists():
        raise SyncError(f"database not found: {path}")
    if not is_managed_incremental(database):
        raise SyncError("database is not under incremental sync management")
    db = _connect(database)
    try:
        row = _latest_window(db)
        if row is None or row[1] == "complete":
            return {"discarded": False,
                    "reason": "no unfinished window; official facts and watermark unchanged"}
        window_id = row[0]
        with db:
            for table in ("sync_stage_pages", "sync_stage_revisions", "sync_seen_tokens"):
                db.execute(f"DELETE FROM {table} WHERE window_id=?", (window_id,))
            db.execute("DELETE FROM sync_window WHERE window_id=?", (window_id,))
        meta = _meta(db)
        return {"discarded": True, "window_id": window_id,
                "watermark_retained": meta.get(META_WATERMARK)}
    finally:
        db.close()


def _print_human_status(info):
    if not info.get("managed"):
        print("not a managed incremental database")
        return
    print(f"bound endpoint : {info['endpoint']}")
    print(f"fixed start B  : {info['base_date']}")
    print(f"watermark W    : {info['watermark'] or '(none)'}")
    win = info["unfinished_window"]
    if win is None:
        print("unfinished     : none")
        return
    print(f"unfinished     : window {win['window_id']} state={win['state']} "
          f"stage={win['stage']}")
    print(f"  frozen range : {win['window_from']} .. {win['window_until']} (inclusive)")
    print(f"  overlap-days : {win['overlap_days']}  max-pages: {win['max_pages']}")
    print(f"  traversal    : {win['traversal']}  restarts used: {win['restarts_used']}/1")
    print(f"  confirmed    : {win['pages_confirmed']} pages, "
          f"{win['records_confirmed']} unique staged revisions")
    print(f"  next token   : {win['next_token']!r}")
    print(f"  last error   : {win['last_error'] or '(none)'}")


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(
        prog="python -m catalog_harvest.sync",
        description="Resumable incremental date-window harvest")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run or resume one date window")
    p_run.add_argument("--endpoint", required=True)
    p_run.add_argument("--db", required=True)
    p_run.add_argument("--start-date", default=None,
                       help="fixed B (YYYY-MM-DD); required only on first initialization")
    p_run.add_argument("--until", required=True, help="inclusive upper bound YYYY-MM-DD")
    p_run.add_argument("--overlap-days", type=int, default=2)
    p_run.add_argument("--max-pages", type=int, default=20)
    p_run.add_argument("--json", action="store_true")

    p_status = sub.add_parser("status", help="read-only local sync state")
    p_status.add_argument("--db", required=True)
    p_status.add_argument("--json", action="store_true")

    p_discard = sub.add_parser("discard", help="abandon unpublished window work")
    p_discard.add_argument("--db", required=True)
    p_discard.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            result = run_sync(
                args.endpoint, args.db, start_date=args.start_date, until=args.until,
                overlap_days=args.overlap_days, max_pages=args.max_pages)
            if args.json:
                print(json.dumps(result, sort_keys=True, ensure_ascii=False))
            else:
                print(f"window {result['window_from']}..{result['window_until']} complete; "
                      f"W={result['watermark']}{' (empty)' if result['empty_window'] else ''}")
                print(f"pages={result['pages']} (recovered {result['recovered_pages']}, "
                      f"fetched now {result['pages_fetched_this_run']}), "
                      f"record appearances={result['records_seen']}, "
                      f"new revisions={result['inserted_revisions']}")
                print(f"current active={result['active_records']} "
                      f"deleted={result['deleted_records']}, "
                      f"HTTP requests this run={result['http_requests_this_run']}, "
                      f"traversal restarts={result['traversal_restarts']}")
        elif args.command == "status":
            info = status(args.db)
            if args.json:
                print(json.dumps(info, sort_keys=True, ensure_ascii=False))
            else:
                _print_human_status(info)
        else:
            result = discard(args.db)
            if args.json:
                print(json.dumps(result, sort_keys=True, ensure_ascii=False))
            else:
                print(result)
    except (SyncError, OSError, RuntimeError) as exc:
        parser.exit(2, f"sync {args.command} failed: {exc}\n")


if __name__ == "__main__":
    main()
