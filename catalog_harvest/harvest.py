"""Fetch a complete bounded traversal, then commit revisions as one batch."""
import hashlib
import sqlite3
from pathlib import Path
from .protocol import parse_page, canonical

DDL = """
CREATE TABLE IF NOT EXISTS pages (sha256 TEXT PRIMARY KEY, xml_body BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS revisions (
 identifier TEXT NOT NULL, datestamp TEXT NOT NULL, deleted INTEGER NOT NULL,
 title TEXT, subjects_json TEXT NOT NULL, canonical_json TEXT NOT NULL,
 page_sha256 TEXT NOT NULL REFERENCES pages(sha256),
 PRIMARY KEY(identifier, datestamp));
CREATE VIEW IF NOT EXISTS current_records AS
 SELECT r.* FROM revisions r
 WHERE datestamp=(SELECT max(x.datestamp) FROM revisions x WHERE x.identifier=r.identifier);
"""


def is_managed_incremental(database):
    """True when the SQLite file holds catalog_harvest.sync managed state."""
    path = Path(database)
    if not path.exists():
        return False
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as probe:
            row = probe.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_meta'").fetchone()
        return row is not None
    except sqlite3.DatabaseError:
        return False


def _reject_managed(database):
    if is_managed_incremental(database):
        raise ValueError(
            "database is bound to incremental sync; use 'python -m catalog_harvest.sync run' "
            "instead of the full harvest entry point")


def harvest(fetch, database, max_pages=20):
    if not 1 <= max_pages <= 1000:
        raise ValueError("max_pages must be 1..1000")
    _reject_managed(database)
    pages, marker, seen = [], "", set()
    for _ in range(max_pages):
        raw = fetch(marker)
        records, next_marker = parse_page(raw)
        pages.append((raw,records))
        if not next_marker:
            break
        if next_marker in seen:
            raise ValueError("continuation cycle")
        seen.add(next_marker)
        marker = next_marker
    else:
        raise ValueError("page limit exceeded")
    Path(database).parent.mkdir(parents=True,exist_ok=True)
    inserted = 0
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(DDL)
        for raw,records in pages:
            digest = hashlib.sha256(raw).hexdigest()
            db.execute("INSERT OR IGNORE INTO pages VALUES (?,?)", (digest,raw))
            for record in records:
                encoded = canonical(record)
                previous = db.execute("SELECT canonical_json FROM revisions WHERE identifier=? AND datestamp=?",(record["identifier"],record["datestamp"])).fetchone()
                if previous:
                    if previous[0] != encoded:
                        raise ValueError("conflicting revision identity")
                    continue
                import json
                db.execute("INSERT INTO revisions VALUES (?,?,?,?,?,?,?)",(record["identifier"],record["datestamp"],record["deleted"],record["title"],json.dumps(record["subjects"]),encoded,digest))
                inserted += 1
        active,deleted = db.execute("SELECT sum(deleted=0),sum(deleted=1) FROM current_records").fetchone()
    return {"pages":len(pages),"records_seen":sum(len(r) for _,r in pages),"inserted_revisions":inserted,"active_records":active or 0,"deleted_records":deleted or 0}
