"""Offline historical subject warehouse for the Research Catalog Harvester.

This module turns an *official* harvest database (the ``pages`` /
``revisions`` tables produced by either :mod:`catalog_harvest.harvest` or the
published side of :mod:`catalog_harvest.sync`) plus a fixed static taxonomy
into a self-contained, point-in-time analytical SQLite database.

Design contract (see ``docs/warehouse.md`` for the full narrative):

* Only formal ``pages`` and ``revisions`` are read, through one explicit
  read-only transaction. ``sync_stage_*`` tables, unfinished windows and the
  success watermark are never queried and never exported. The source database
  is never modified and is not created when missing.
* Every formal revision is re-validated (keys, strict dates,
  deleted/title/subjects/canonical consistency, page reference and the raw
  page byte SHA256) and must be re-parseable by the shipping OAI parser with
  an identical-key, identical-canonical record.
* Versions are half-open intervals ``[valid_from, valid_to)``. A revision is
  effective from its own datestamp until the next revision of the same
  identifier; the last one extends to ``end``. A revision before ``start``
  still determines the state at ``start``; identifiers with no revision at or
  before a day are simply unknown (never back-filled, never fake-deleted).
* The taxonomy is fixed for the whole build. The transitive closure (including
  self rows, shortest edge distance only, one row per pair) and all bridge
  tables are materialized so the product database answers every question in
  plain SQL without the source or taxonomy file.
* Multi-valued subjects are de-duplicated before aggregation: several
  subjects (or several synonym subjects bound to one node, or diamond-shaped
  ancestor paths) contribute at most one vote per record per topic.
* Counts are stored as *sparse maximal half-open intervals* produced by a
  boundary sweep, never by expanding every calendar day.

Only the Python standard library and SQLite are used; there is no HTTP and no
long-running service.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from collections import defaultdict
from datetime import date
from pathlib import Path

from .protocol import canonical, parse_iso_date, parse_page

WAREHOUSE_FORMAT = "catalog_harvest_warehouse"
WAREHOUSE_FORMAT_VERSION = "1"

# Test-only failure injection hooks (not part of the operational contract).
ENV_WRITE_FAIL = "CATALOG_HARVEST_WH_WRITE_FAIL"
ENV_REPLACE_FAIL = "CATALOG_HARVEST_WH_REPLACE_FAIL"


class WarehouseError(ValueError):
    """Validation, source-integrity or build failure the operator must fix."""


# ---------------------------------------------------------------------------
# Stable, unambiguous encoding for logical source-fact fingerprints
# ---------------------------------------------------------------------------

def _stable_json(obj):
    """Deterministic JSON text: sorted keys, compact separators, real Unicode."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Source database: read-only, one transaction, formal facts only
# ---------------------------------------------------------------------------

def _require_columns(db, table, required):
    present = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
    missing = required - present
    if missing:
        raise WarehouseError(
            f"source table {table!r} is missing required columns: "
            + ", ".join(sorted(missing)))


def read_formal_facts(source_db):
    """Read and validate official ``pages`` / ``revisions`` from a source DB.

    The connection is read-only (``mode=ro``) and runs inside one explicit
    read transaction. No ``sync_stage_*`` / ``sync_meta`` / ``sync_window``
    object is ever queried. A missing source is an error, never an empty
    database creation.
    """
    path = Path(source_db)
    if not path.is_file():
        raise WarehouseError(f"source database not found: {path}")
    uri = f"file:{path.resolve()}?mode=ro"
    db = sqlite3.connect(uri, uri=True)
    try:
        db.execute("PRAGMA query_only=ON")
        objects = {
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
        for table in ("pages", "revisions"):
            if table not in objects:
                raise WarehouseError(
                    f"source database is not a harvest database: "
                    f"required table {table!r} is absent")
        _require_columns(db, "pages", {"sha256", "xml_body"})
        _require_columns(
            db, "revisions",
            {"identifier", "datestamp", "deleted", "title", "subjects_json",
             "canonical_json", "page_sha256"})
        # One explicit read transaction gives a consistent snapshot of the
        # formal facts. No sync_* table is referenced anywhere in this module.
        with db:
            page_rows = db.execute(
                "SELECT sha256, xml_body FROM pages ORDER BY sha256").fetchall()
            revision_rows = db.execute(
                "SELECT identifier, datestamp, deleted, title, subjects_json, "
                "canonical_json, page_sha256 FROM revisions "
                "ORDER BY identifier, datestamp").fetchall()
    finally:
        db.close()

    pages = {}
    parsed_pages = {}
    for digest, raw in page_rows:
        if not isinstance(raw, bytes):
            raise WarehouseError(f"page {digest!r}: xml_body must be raw bytes")
        actual = _sha256_hex(raw)
        if actual != digest:
            raise WarehouseError(
                f"page {digest!r}: stored sha256 does not match raw bytes "
                f"(actual {actual})")
        try:
            records, _token = parse_page(raw)
        except ValueError as exc:
            raise WarehouseError(
                f"page {digest!r}: formal page cannot be parsed: {exc}") from exc
        pages[digest] = raw
        parsed_pages[digest] = records

    revisions = []
    seen_identities = set()
    for (identifier, datestamp, deleted, title, subjects_json,
         canonical_json, page_sha256) in revision_rows:
        identity = f"{identifier}@{datestamp}"
        if not isinstance(identifier, str) or not identifier.strip():
            raise WarehouseError(f"revision {identity!r}: identifier must be a nonempty string")
        if identity in seen_identities:
            raise WarehouseError(f"revision {identity!r}: duplicate revision identity")
        seen_identities.add(identity)
        try:
            parse_iso_date(datestamp)
        except ValueError as exc:
            raise WarehouseError(
                f"revision {identity!r}: datestamp must be strict YYYY-MM-DD") from exc
        if deleted not in (0, 1):
            raise WarehouseError(f"revision {identity!r}: deleted must be 0 or 1")
        try:
            subjects = json.loads(subjects_json)
        except (TypeError, ValueError) as exc:
            raise WarehouseError(
                f"revision {identity!r}: subjects_json is not valid JSON") from exc
        if (not isinstance(subjects, list)
                or any(not isinstance(s, str) for s in subjects)):
            raise WarehouseError(
                f"revision {identity!r}: subjects must be a JSON list of strings")
        if any(not s or s != " ".join(s.split()) for s in subjects):
            raise WarehouseError(
                f"revision {identity!r}: subjects must be nonempty whitespace-"
                "normalized strings")
        if subjects != sorted(set(subjects)):
            raise WarehouseError(
                f"revision {identity!r}: subjects must be unique and sorted")
        if deleted:
            if title is not None or subjects:
                raise WarehouseError(
                    f"revision {identity!r}: deleted revision must carry no "
                    "title or subjects")
        else:
            if not isinstance(title, str) or not title.strip():
                raise WarehouseError(
                    f"revision {identity!r}: active revision needs a nonempty title")
        record = {
            "identifier": identifier, "datestamp": datestamp,
            "deleted": bool(deleted), "title": title, "subjects": subjects}
        if canonical(record) != canonical_json:
            raise WarehouseError(
                f"revision {identity!r}: canonical_json does not match the "
                "stored revision payload")
        if page_sha256 not in pages:
            raise WarehouseError(
                f"revision {identity!r}: references missing source page "
                f"{page_sha256!r}")
        # The cited formal page must parse and contain the same identity with
        # the same canonical payload. It need not be the record's earliest
        # appearance across all source pages.
        match = next((r for r in parsed_pages[page_sha256]
                      if r["identifier"] == identifier
                      and r["datestamp"] == datestamp), None)
        if match is None:
            raise WarehouseError(
                f"revision {identity!r}: not present on cited page "
                f"{page_sha256!r}")
        if canonical(match) != canonical_json:
            raise WarehouseError(
                f"revision {identity!r}: page {page_sha256!r} carries a "
                "different payload for the same identity")
        revisions.append({
            "identifier": identifier, "datestamp": datestamp,
            "deleted": int(deleted), "title": title,
            "subjects": subjects, "canonical_json": canonical_json,
            "page_sha256": page_sha256})

    return {"pages": pages, "revisions": revisions}


def source_facts_digest(facts):
    """Logical-content fingerprint of the formal read transaction.

    The fingerprint is computed over a documented, stable key sequence with an
    unambiguous JSON encoding of the parsed logical facts (page XML text and
    full revision payloads ordered by business keys). It deliberately is *not*
    a hash of the source SQLite file bytes, so staging-only changes that leave
    the formal facts untouched leave this digest unchanged.
    """
    payload = {
        "format": "catalog_harvest_source_facts",
        "format_version": 1,
        "pages": [
            {"sha256": digest, "xml_body": facts["pages"][digest].decode("utf-8")}
            for digest in sorted(facts["pages"])],
        "revisions": [
            {"identifier": r["identifier"], "datestamp": r["datestamp"],
             "deleted": r["deleted"], "title": r["title"],
             "subjects": r["subjects"], "canonical_json": r["canonical_json"],
             "page_sha256": r["page_sha256"]}
            for r in facts["revisions"]],
    }
    return _sha256_hex(_stable_json(payload).encode("utf-8"))


# ---------------------------------------------------------------------------
# Taxonomy: strict JSON validation, closure and bindings
# ---------------------------------------------------------------------------

def _is_clean_nonempty_string(value):
    return (isinstance(value, str) and value != ""
            and value == value.strip())


def parse_taxonomy(raw_bytes):
    """Validate the taxonomy document and return its structured contents."""
    if not isinstance(raw_bytes, bytes) or not raw_bytes:
        raise WarehouseError("taxonomy must be a non-empty UTF-8 JSON document")
    try:
        document = json.loads(raw_bytes.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise WarehouseError("taxonomy must be UTF-8 encoded JSON") from exc
    except json.JSONDecodeError as exc:
        raise WarehouseError(f"taxonomy is not valid JSON: {exc}") from exc
    if not isinstance(document, dict) or set(document) != {
            "schema_version", "nodes", "edges", "bindings"}:
        raise WarehouseError(
            "taxonomy top level must contain exactly schema_version, nodes, "
            "edges and bindings")
    schema_version = document["schema_version"]
    # bool is a subclass of int; reject booleans explicitly.
    if isinstance(schema_version, bool) or schema_version != 1:
        raise WarehouseError("taxonomy schema_version must be the integer 1")
    nodes_raw, edges_raw, bindings_raw = (
        document["nodes"], document["edges"], document["bindings"])
    if not all(isinstance(x, list) for x in (nodes_raw, edges_raw, bindings_raw)):
        raise WarehouseError("taxonomy nodes, edges and bindings must be arrays")

    nodes = {}
    for node in nodes_raw:
        if not isinstance(node, dict) or set(node) != {"topic_id", "label"}:
            raise WarehouseError(
                "each taxonomy node must be exactly {topic_id, label}")
        topic_id, label = node["topic_id"], node["label"]
        if not _is_clean_nonempty_string(topic_id):
            raise WarehouseError("topic_id must be a nonempty string without "
                                 "leading or trailing whitespace")
        if not isinstance(label, str) or not label:
            raise WarehouseError(
                f"node {topic_id!r}: label must be a nonempty string")
        if label != label.strip():
            raise WarehouseError(
                f"node {topic_id!r}: label must not have leading or trailing "
                "whitespace")
        if topic_id in nodes:
            raise WarehouseError(f"duplicate topic_id: {topic_id!r}")
        nodes[topic_id] = label

    edges = []
    edge_keys = set()
    parents = defaultdict(list)
    for edge in edges_raw:
        if not isinstance(edge, dict) or set(edge) != {"child", "parent"}:
            raise WarehouseError(
                "each taxonomy edge must be exactly {child, parent}")
        child, parent = edge["child"], edge["parent"]
        if not all(isinstance(x, str) for x in (child, parent)):
            raise WarehouseError("edge endpoints must be strings")
        if child not in nodes or parent not in nodes:
            raise WarehouseError(
                f"edge {child!r}->{parent!r} references an unknown node")
        if child == parent:
            raise WarehouseError(f"self edge is not allowed: {child!r}")
        key = (child, parent)
        if key in edge_keys:
            raise WarehouseError(f"duplicate edge: {child!r}->{parent!r}")
        edge_keys.add(key)
        edges.append(key)
        parents[child].append(parent)

    _reject_cycles(nodes, parents)

    bindings = []
    bound_subjects = set()
    for binding in bindings_raw:
        if not isinstance(binding, dict) or set(binding) != {
                "subject", "topic_id"}:
            raise WarehouseError(
                "each taxonomy binding must be exactly {subject, topic_id}")
        subject, topic_id = binding["subject"], binding["topic_id"]
        if not isinstance(subject, str) or not subject:
            raise WarehouseError(
                "binding subject must be a nonempty string")
        if subject != subject.strip():
            raise WarehouseError(
                f"binding subject {subject!r} has leading or trailing "
                "whitespace; only the parser's whitespace-normalized form is "
                "accepted")
        # The parser normalizes subjects with " ".join(text.split()); a bound
        # subject must already be in that exact canonical form.
        if subject != " ".join(subject.split()):
            raise WarehouseError(
                f"binding subject {subject!r} is not in the parser's "
                "whitespace-normalized form")
        if topic_id not in nodes:
            raise WarehouseError(
                f"binding for subject {subject!r} references unknown topic "
                f"{topic_id!r}")
        if subject in bound_subjects:
            raise WarehouseError(
                f"subject {subject!r} is bound more than once")
        bound_subjects.add(subject)
        bindings.append((subject, topic_id))

    closure = build_closure(nodes, parents)
    return {
        "schema_version": schema_version,
        "nodes": sorted(nodes.items()),       # (topic_id, label), sorted
        "edges": sorted(edges),
        "bindings": sorted(bindings),
        "parents": parents,
        "subject_to_topic": {s: t for s, t in bindings},
        "closure": closure,
    }


def _reject_cycles(nodes, parents):
    """Tri-color DFS over parent edges; any back edge in any component fails."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {topic: WHITE for topic in nodes}

    def visit(topic, stack):
        color[topic] = GRAY
        for parent in parents.get(topic, ()):
            if color[parent] == GRAY:
                cycle = " -> ".join(stack + [parent])
                raise WarehouseError(f"taxonomy cycle detected: {cycle}")
            if color[parent] == WHITE:
                visit(parent, stack + [parent])
        color[topic] = BLACK

    for topic in sorted(nodes):
        if color[topic] == WHITE:
            visit(topic, [topic])


def build_closure(nodes, parents):
    """Shortest-distance ancestor closure including the self row.

    One row per ``(descendant, ancestor)`` pair holding the minimum edge count.
    Multiple paths never create duplicate rows and no exponential path set is
    stored.
    """
    closure = {}
    for descendant in sorted(nodes):
        distances = {descendant: 0}
        frontier = [descendant]
        while frontier:
            current = frontier.pop(0)
            next_distance = distances[current] + 1
            for parent in parents.get(current, ()):
                if parent not in distances:
                    distances[parent] = next_distance
                    frontier.append(parent)
        for ancestor, distance in distances.items():
            closure[(descendant, ancestor)] = distance
    return closure


# ---------------------------------------------------------------------------
# Version timeline and de-duplicated bridges
# ---------------------------------------------------------------------------

def build_versions(facts, taxonomy, start: date, end: date):
    """Clip formal revisions to ``[start, end)`` version intervals."""
    binding = taxonomy["subject_to_topic"]
    closure = taxonomy["closure"]
    grouped = defaultdict(list)
    for revision in facts["revisions"]:
        grouped[revision["identifier"]].append(revision)

    versions = []
    for identifier in sorted(grouped):
        revisions = sorted(grouped[identifier], key=lambda r: r["datestamp"])
        for index, revision in enumerate(revisions):
            effective = parse_iso_date(revision["datestamp"])
            next_date = (parse_iso_date(revisions[index + 1]["datestamp"])
                         if index + 1 < len(revisions) else end)
            valid_from = max(effective, start)
            valid_to = min(next_date, end)
            if valid_from >= valid_to:
                continue  # out-of-range revision: kept at source layer only
            subjects = sorted(revision["subjects"])
            direct_topics = sorted({binding[s] for s in subjects
                                    if s in binding})
            unmapped = sorted(s for s in subjects if s not in binding)
            ancestor_topics = sorted({
                ancestor
                for topic in direct_topics
                for (descendant, ancestor), distance in closure.items()
                if descendant == topic})
            versions.append({
                "identifier": identifier,
                "datestamp": revision["datestamp"],
                "deleted": revision["deleted"],
                "title": revision["title"],
                "valid_from": valid_from.isoformat(),
                "valid_to": valid_to.isoformat(),
                "page_sha256": revision["page_sha256"],
                "canonical_json": revision["canonical_json"],
                "subjects": subjects,
                "direct_topics": direct_topics,
                "ancestor_topics": ancestor_topics,
                "unmapped_subjects": unmapped,
                "has_subjects": int(bool(subjects)),
                "has_unmapped": int(bool(unmapped)),
            })
    # Stable surrogate identifier independent of insertion happenstance.
    for version_id, version in enumerate(versions, start=1):
        version["version_id"] = version_id
    return versions


# ---------------------------------------------------------------------------
# Sparse maximal half-open count intervals (boundary sweep, no day expansion)
# ---------------------------------------------------------------------------

def _sparse_intervals(start: date, end: date, events):
    """Collapse signed boundary events into maximal constant-count segments.

    ``events`` maps a boundary date to a list of integer deltas. All deltas at
    the same date are applied together between emitted segments, so an old
    version leaving and a new version entering on the same day never produce a
    transient negative or double-counted interval.
    """
    boundaries = sorted(d for d in events if start <= d < end)
    current = 0
    cursor = start
    segments = []
    for boundary in boundaries:
        if boundary > cursor:
            segments.append((cursor, boundary, current))
        current += sum(events[boundary])
        cursor = boundary
    if end > cursor:
        segments.append((cursor, end, current))
    # Merge adjacent segments that hold the same count into maximal intervals.
    merged = []
    for seg_from, seg_to, count in segments:
        if merged and merged[-1][1] == seg_from and merged[-1][2] == count:
            merged[-1] = (merged[-1][0], seg_to, count)
        else:
            merged.append((seg_from, seg_to, count))
    return merged


def build_topic_intervals(versions, taxonomy, start: date, end: date):
    events = defaultdict(lambda: defaultdict(list))
    for version in versions:
        if version["deleted"]:
            continue
        entry = date.fromisoformat(version["valid_from"])
        exit_day = date.fromisoformat(version["valid_to"])
        for topic in version["ancestor_topics"]:
            events[topic][entry].append(+1)
            events[topic][exit_day].append(-1)
    intervals = {}
    for topic in sorted(t for t, _ in taxonomy["nodes"]):
        intervals[topic] = _sparse_intervals(start, end, events.get(topic, {}))
    return intervals


CATALOG_METRICS = (
    ("active", "active identifiers (excludes unknown and deleted states)"),
    ("deleted", "identifiers whose effective version is a deletion tombstone"),
    ("active_no_subject", "active versions carrying no subject at all"),
    ("active_with_unmapped", "active versions carrying at least one unbound subject"),
)


def build_catalog_intervals(versions, start: date, end: date):
    events = defaultdict(lambda: defaultdict(list))
    for version in versions:
        entry = date.fromisoformat(version["valid_from"])
        exit_day = date.fromisoformat(version["valid_to"])
        metric = "deleted" if version["deleted"] else "active"
        events[metric][entry].append(+1)
        events[metric][exit_day].append(-1)
        if not version["deleted"] and not version["has_subjects"]:
            events["active_no_subject"][entry].append(+1)
            events["active_no_subject"][exit_day].append(-1)
        if not version["deleted"] and version["has_unmapped"]:
            events["active_with_unmapped"][entry].append(+1)
            events["active_with_unmapped"][exit_day].append(-1)
    return {metric: _sparse_intervals(start, end, events.get(metric, {}))
            for metric, _description in CATALOG_METRICS}


# ---------------------------------------------------------------------------
# Product database schema and atomic single-writer publication
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE wh_meta (
 key TEXT PRIMARY KEY,
 value TEXT NOT NULL
);
CREATE TABLE wh_source_pages (
 sha256 TEXT PRIMARY KEY,
 xml_body BLOB NOT NULL
);
CREATE TABLE wh_source_revisions (
 identifier TEXT NOT NULL,
 datestamp TEXT NOT NULL,
 deleted INTEGER NOT NULL CHECK (deleted IN (0,1)),
 title TEXT,
 subjects_json TEXT NOT NULL,
 canonical_json TEXT NOT NULL,
 page_sha256 TEXT NOT NULL REFERENCES wh_source_pages(sha256),
 PRIMARY KEY (identifier, datestamp)
);
CREATE TABLE wh_taxonomy_provenance (
 taxonomy_sha256 TEXT PRIMARY KEY,
 schema_version INTEGER NOT NULL,
 raw_json BLOB NOT NULL
);
CREATE TABLE wh_taxonomy_nodes (
 topic_id TEXT PRIMARY KEY,
 label TEXT NOT NULL
);
CREATE TABLE wh_taxonomy_edges (
 child TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id),
 parent TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id),
 PRIMARY KEY (child, parent),
 CHECK (child <> parent)
);
CREATE TABLE wh_taxonomy_bindings (
 subject TEXT PRIMARY KEY,
 topic_id TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id)
);
CREATE TABLE wh_topic_closure (
 descendant TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id),
 ancestor TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id),
 distance INTEGER NOT NULL CHECK (distance >= 0),
 PRIMARY KEY (descendant, ancestor)
);
CREATE TABLE wh_versions (
 version_id INTEGER PRIMARY KEY,
 identifier TEXT NOT NULL,
 datestamp TEXT NOT NULL,
 deleted INTEGER NOT NULL CHECK (deleted IN (0,1)),
 title TEXT,
 valid_from TEXT NOT NULL,
 valid_to TEXT NOT NULL,
 has_subjects INTEGER NOT NULL CHECK (has_subjects IN (0,1)),
 has_unmapped INTEGER NOT NULL CHECK (has_unmapped IN (0,1)),
 page_sha256 TEXT NOT NULL REFERENCES wh_source_pages(sha256),
 canonical_json TEXT NOT NULL,
 UNIQUE (identifier, datestamp, valid_from),
 CHECK (valid_from < valid_to)
);
CREATE TABLE wh_version_subjects (
 version_id INTEGER NOT NULL REFERENCES wh_versions(version_id),
 subject TEXT NOT NULL,
 PRIMARY KEY (version_id, subject)
);
CREATE TABLE wh_version_subject_topics (
 version_id INTEGER NOT NULL REFERENCES wh_versions(version_id),
 subject TEXT NOT NULL,
 topic_id TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id),
 PRIMARY KEY (version_id, subject, topic_id),
 FOREIGN KEY (version_id, subject) REFERENCES wh_version_subjects(version_id, subject)
);
CREATE TABLE wh_version_topics (
 version_id INTEGER NOT NULL REFERENCES wh_versions(version_id),
 topic_id TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id),
 PRIMARY KEY (version_id, topic_id)
);
CREATE TABLE wh_version_ancestors (
 version_id INTEGER NOT NULL REFERENCES wh_versions(version_id),
 topic_id TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id),
 PRIMARY KEY (version_id, topic_id)
);
CREATE TABLE wh_topic_intervals (
 topic_id TEXT NOT NULL REFERENCES wh_taxonomy_nodes(topic_id),
 valid_from TEXT NOT NULL,
 valid_to TEXT NOT NULL,
 record_count INTEGER NOT NULL CHECK (record_count >= 0),
 PRIMARY KEY (topic_id, valid_from),
 CHECK (valid_from < valid_to)
);
CREATE TABLE wh_catalog_intervals (
 metric TEXT NOT NULL,
 valid_from TEXT NOT NULL,
 valid_to TEXT NOT NULL,
 record_count INTEGER NOT NULL CHECK (record_count >= 0),
 PRIMARY KEY (metric, valid_from),
 CHECK (valid_from < valid_to)
);
CREATE VIEW wh_topic_record_days AS
SELECT topic_id,
       CAST(sum(CAST(julianday(valid_to) - julianday(valid_from) AS INTEGER)
                * record_count) AS INTEGER) AS record_days
FROM wh_topic_intervals
GROUP BY topic_id;
CREATE VIEW wh_unmapped_subjects AS
SELECT vs.version_id, vs.subject
FROM wh_version_subjects vs
LEFT JOIN wh_taxonomy_bindings b ON b.subject = vs.subject
WHERE b.subject IS NULL;
"""


def _ensure_distinct_paths(*labeled_paths):
    """Reject same-file aliases (symlinks/hard links) between inputs/output."""
    resolved = {}
    stats = {}
    for label, candidate in labeled_paths:
        path = Path(candidate)
        real = os.path.realpath(path)
        if real in resolved:
            raise WarehouseError(
                f"{label} must not be the same file as {resolved[real]} "
                f"(resolved path {real})")
        resolved[real] = label
        if path.exists():
            stats[(label, real)] = os.stat(path)
    seen_inodes = {}
    for (label, real), stat_result in stats.items():
        inode = (stat_result.st_dev, stat_result.st_ino)
        if inode in seen_inodes:
            raise WarehouseError(
                f"{label} is a hard-link alias of {seen_inodes[inode]}")
        seen_inodes[inode] = label


def _verify_interval_coverage(db, start_text, end_text):
    for table, key in (("wh_topic_intervals", "topic_id"),
                       ("wh_catalog_intervals", "metric")):
        groups = db.execute(
            f"SELECT {key}, valid_from, valid_to FROM {table} "
            f"ORDER BY {key}, valid_from").fetchall()
        covered = defaultdict(list)
        for group, seg_from, seg_to in groups:
            covered[group].append((seg_from, seg_to))
        for group, segments in covered.items():
            if segments[0][0] != start_text or segments[-1][1] != end_text:
                raise WarehouseError(
                    f"{table} does not fully cover [{start_text},{end_text}) "
                    f"for {group!r}")
            for (prev_from, prev_to), (next_from, _next_to) in zip(
                    segments, segments[1:]):
                if prev_to != next_from:
                    raise WarehouseError(
                        f"{table} has a non-contiguous interval for {group!r}")


def build_warehouse(source_db, taxonomy_path, start, end, database):
    """Build the offline warehouse and atomically replace ``database``."""
    start_date = parse_iso_date(start) if isinstance(start, str) else start
    end_date = parse_iso_date(end) if isinstance(end, str) else end
    if not start_date < end_date:
        raise WarehouseError("start must be a strict date earlier than end")
    taxonomy_path = Path(taxonomy_path)
    database = Path(database)
    _ensure_distinct_paths(
        ("source database", source_db),
        ("taxonomy file", taxonomy_path),
        ("warehouse database", database))

    facts = read_formal_facts(source_db)
    taxonomy_bytes = taxonomy_path.read_bytes()
    taxonomy = parse_taxonomy(taxonomy_bytes)
    taxonomy_digest = _sha256_hex(taxonomy_bytes)
    facts_digest = source_facts_digest(facts)

    versions = build_versions(facts, taxonomy, start_date, end_date)
    topic_intervals = build_topic_intervals(
        versions, taxonomy, start_date, end_date)
    catalog_intervals = build_catalog_intervals(versions, start_date, end_date)

    database.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    db = None
    try:
        from tempfile import NamedTemporaryFile
        with NamedTemporaryFile(
                prefix=".whbuild-", suffix=".tmp", dir=database.parent,
                delete=False) as handle:
            tmp_path = Path(handle.name)
        db = sqlite3.connect(tmp_path)
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(DDL)
        if os.environ.get(ENV_WRITE_FAIL):
            raise WarehouseError("injected write failure (test hook)")
        with db:
            meta = {
                "format": WAREHOUSE_FORMAT,
                "format_version": WAREHOUSE_FORMAT_VERSION,
                "build_start": start_date.isoformat(),
                "build_end": end_date.isoformat(),
                "source_facts_sha256": facts_digest,
                "taxonomy_sha256": taxonomy_digest,
                "taxonomy_schema_version": str(taxonomy["schema_version"]),
            }
            db.executemany("INSERT INTO wh_meta VALUES (?,?)",
                           sorted(meta.items()))
            db.executemany(
                "INSERT INTO wh_source_pages VALUES (?,?)",
                sorted(facts["pages"].items()))
            db.executemany(
                "INSERT INTO wh_source_revisions VALUES (?,?,?,?,?,?,?)",
                [(r["identifier"], r["datestamp"], r["deleted"], r["title"],
                  json.dumps(r["subjects"], ensure_ascii=False),
                  r["canonical_json"], r["page_sha256"])
                 for r in facts["revisions"]])
            db.execute("INSERT INTO wh_taxonomy_provenance VALUES (?,?,?)",
                       (taxonomy_digest, taxonomy["schema_version"],
                        taxonomy_bytes))
            db.executemany("INSERT INTO wh_taxonomy_nodes VALUES (?,?)",
                           taxonomy["nodes"])
            db.executemany("INSERT INTO wh_taxonomy_edges VALUES (?,?)",
                           taxonomy["edges"])
            db.executemany("INSERT INTO wh_taxonomy_bindings VALUES (?,?)",
                           taxonomy["bindings"])
            db.executemany(
                "INSERT INTO wh_topic_closure VALUES (?,?,?)",
                [(descendant, ancestor, distance)
                 for (descendant, ancestor), distance in
                 sorted(taxonomy["closure"].items())])
            for version in versions:
                db.execute(
                    "INSERT INTO wh_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (version["version_id"], version["identifier"],
                     version["datestamp"], version["deleted"], version["title"],
                     version["valid_from"], version["valid_to"],
                     version["has_subjects"], version["has_unmapped"],
                     version["page_sha256"], version["canonical_json"]))
                db.executemany(
                    "INSERT INTO wh_version_subjects VALUES (?,?)",
                    [(version["version_id"], s) for s in version["subjects"]])
                db.executemany(
                    "INSERT INTO wh_version_subject_topics VALUES (?,?,?)",
                    [(version["version_id"], s,
                      taxonomy["subject_to_topic"][s])
                     for s in version["subjects"]
                     if s in taxonomy["subject_to_topic"]])
                db.executemany(
                    "INSERT INTO wh_version_topics VALUES (?,?)",
                    [(version["version_id"], t)
                     for t in version["direct_topics"]])
                db.executemany(
                    "INSERT INTO wh_version_ancestors VALUES (?,?)",
                    [(version["version_id"], t)
                     for t in version["ancestor_topics"]])
            for topic in sorted(topic_intervals):
                db.executemany(
                    "INSERT INTO wh_topic_intervals VALUES (?,?,?,?)",
                    [(topic, seg_from.isoformat(), seg_to.isoformat(), count)
                     for seg_from, seg_to, count in topic_intervals[topic]])
            for metric, _description in CATALOG_METRICS:
                db.executemany(
                    "INSERT INTO wh_catalog_intervals VALUES (?,?,?,?)",
                    [(metric, seg_from.isoformat(), seg_to.isoformat(), count)
                     for seg_from, seg_to, count
                     in catalog_intervals[metric]])

        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise WarehouseError(f"warehouse integrity check failed: {integrity}")
        foreign_keys = db.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_keys:
            raise WarehouseError(
                f"warehouse foreign key violations: {foreign_keys[:5]}")
        _verify_interval_coverage(
            db, start_date.isoformat(), end_date.isoformat())
        db.close()
        db = None
        if os.environ.get(ENV_REPLACE_FAIL):
            raise WarehouseError("injected replace failure (test hook)")
        os.replace(tmp_path, database)
        tmp_path = None
    finally:
        if db is not None:
            db.close()
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass

    summary = {
        "build_start": start_date.isoformat(),
        "build_end": end_date.isoformat(),
        "source_facts_sha256": facts_digest,
        "taxonomy_sha256": taxonomy_digest,
        "source_pages": len(facts["pages"]),
        "source_revisions": len(facts["revisions"]),
        "taxonomy_nodes": len(taxonomy["nodes"]),
        "taxonomy_edges": len(taxonomy["edges"]),
        "taxonomy_bindings": len(taxonomy["bindings"]),
        "closure_rows": len(taxonomy["closure"]),
        "versions": len(versions),
        "active_versions": sum(1 for v in versions if not v["deleted"]),
        "deleted_versions": sum(1 for v in versions if v["deleted"]),
        "version_subject_rows": sum(len(v["subjects"]) for v in versions),
        "direct_topic_bridge_rows": sum(
            len(v["direct_topics"]) for v in versions),
        "ancestor_bridge_rows": sum(
            len(v["ancestor_topics"]) for v in versions),
        "topic_interval_rows": sum(
            len(segments) for segments in topic_intervals.values()),
        "catalog_interval_rows": sum(
            len(segments) for segments in catalog_intervals.values()),
    }
    return summary


# ---------------------------------------------------------------------------
# As-of reads against the self-contained product database
# ---------------------------------------------------------------------------

def _open_warehouse(database):
    path = Path(database)
    if not path.is_file():
        raise WarehouseError(f"warehouse database not found: {path}")
    db = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    try:
        marker = db.execute(
            "SELECT value FROM wh_meta WHERE key='format'").fetchone()
    except sqlite3.OperationalError as exc:
        db.close()
        raise WarehouseError(
            "database is not a catalog_harvest warehouse") from exc
    if marker is None or marker[0] != WAREHOUSE_FORMAT:
        db.close()
        raise WarehouseError("database is not a catalog_harvest warehouse")
    return db


def as_of(database, day):
    """Return all versions effective on ``day`` and de-duplicated counts."""
    day_date = parse_iso_date(day) if isinstance(day, str) else day
    db = _open_warehouse(database)
    try:
        start_text = db.execute(
            "SELECT value FROM wh_meta WHERE key='build_start'").fetchone()[0]
        end_text = db.execute(
            "SELECT value FROM wh_meta WHERE key='build_end'").fetchone()[0]
        if not (parse_iso_date(start_text) <= day_date
                < parse_iso_date(end_text)):
            raise WarehouseError(
                f"as-of date {day_date.isoformat()} must satisfy "
                f"{start_text} <= date < {end_text}")
        day_text = day_date.isoformat()
        rows = db.execute(
            "SELECT version_id, identifier, datestamp, deleted, title, "
            "valid_from, valid_to, page_sha256 "
            "FROM wh_versions WHERE valid_from <= ? AND ? < valid_to "
            "ORDER BY identifier, datestamp",
            (day_text, day_text)).fetchall()
        versions = []
        for (version_id, identifier, datestamp, deleted, title,
             valid_from, valid_to, page_sha256) in rows:
            subjects = [r[0] for r in db.execute(
                "SELECT subject FROM wh_version_subjects WHERE version_id=? "
                "ORDER BY subject", (version_id,))]
            direct_topics = [r[0] for r in db.execute(
                "SELECT topic_id FROM wh_version_topics WHERE version_id=? "
                "ORDER BY topic_id", (version_id,))]
            ancestor_topics = [r[0] for r in db.execute(
                "SELECT topic_id FROM wh_version_ancestors "
                "WHERE version_id=? ORDER BY topic_id", (version_id,))]
            unmapped = [r[0] for r in db.execute(
                "SELECT subject FROM wh_unmapped_subjects WHERE version_id=? "
                "ORDER BY subject", (version_id,))]
            versions.append({
                "version_id": version_id,
                "identifier": identifier,
                "datestamp": datestamp,
                "deleted": bool(deleted),
                "title": title,
                "subjects": subjects,
                "direct_topics": direct_topics,
                "ancestor_topics": ancestor_topics,
                "unmapped_subjects": unmapped,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "page_sha256": page_sha256,
            })

        def interval_count(table, group_column, group_value):
            row = db.execute(
                f"SELECT record_count FROM {table} "
                f"WHERE {group_column}=? AND valid_from <= ? AND ? < valid_to",
                (group_value, day_text, day_text)).fetchone()
            return row[0] if row else 0

        topic_counts = {
            topic_id: interval_count(
                "wh_topic_intervals", "topic_id", topic_id)
            for topic_id, in db.execute(
                "SELECT topic_id FROM wh_taxonomy_nodes ORDER BY topic_id")}
        metrics = {
            metric: interval_count(
                "wh_catalog_intervals", "metric", metric)
            for metric, _description in CATALOG_METRICS}
        return {
            "date": day_text,
            "build_start": start_text,
            "build_end": end_text,
            "versions": versions,
            "known_versions": len(versions),
            "identifiers": len({v["identifier"] for v in versions}),
            "counts": {
                "active": metrics["active"],
                "deleted": metrics["deleted"],
                "active_no_subject": metrics["active_no_subject"],
                "active_with_unmapped": metrics["active_with_unmapped"],
            },
            "topic_record_counts": topic_counts,
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Command line: python -m catalog_harvest.warehouse {build,as-of}
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m catalog_harvest.warehouse",
        description="Offline historical subject warehouse builder and reader")
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build", help="build a self-contained warehouse")
    p_build.add_argument("--source-db", required=True)
    p_build.add_argument("--taxonomy", required=True)
    p_build.add_argument("--start", required=True, help="YYYY-MM-DD inclusive")
    p_build.add_argument("--end", required=True, help="YYYY-MM-DD exclusive")
    p_build.add_argument("--database", required=True)

    p_asof = sub.add_parser("as-of", help="read a warehouse at a historical date")
    p_asof.add_argument("--database", required=True)
    p_asof.add_argument("--date", required=True, dest="day")

    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            result = build_warehouse(
                args.source_db, args.taxonomy, args.start, args.end,
                args.database)
        else:
            result = as_of(args.database, args.day)
    except (WarehouseError, OSError, sqlite3.DatabaseError) as exc:
        parser.exit(2, f"warehouse {args.command} failed: {exc}\n")
    print(json.dumps(result, sort_keys=True, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
