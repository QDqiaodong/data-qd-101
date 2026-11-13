"""Cross-topic co-occurrence affinity analysis over the historical warehouse.

This module is an *independent reader* of a warehouse produced by
:mod:`catalog_harvest.warehouse`. It never harvests, syncs, builds or mutates
a warehouse or taxonomy; it opens the existing product read-only inside one
explicit SQLite read transaction and writes a single deterministic JSON
report through an atomic temp-file replacement.

Analysis contract (see ``docs/affinity.md`` for the full narrative):

* The plan is strict UTF-8 JSON whose top level is exactly
  ``schema_version``, ``topics``, ``alpha`` and ``min_shared``. Duplicate
  object keys are rejected. The exact plan bytes are hashed once.
* The population is one vote per *active* identifier version effective on the
  requested day (``valid_from <= date < valid_to`` and ``deleted = 0``).
  Deleted versions known on the day stay in the per-record evidence and the
  known-deleted count but never enter the denominator ``N``; records with no
  subjects, only unmapped subjects or no selected-topic hit still enter ``N``.
* A record's selected-topic set is taken from the de-duplicated
  ``wh_version_ancestors`` bridge. Synonym subjects, multiple direct topics
  and diamond ancestor paths can never create more than one vote per topic.
* The fixed testing family is decided before any statistic is inspected:
  every unordered pair of selected topics whose members are mutually
  non-ancestral is tested, including zero-co-occurrence and low-count pairs.
  Ancestor-related pairs are reported separately with closure evidence.
* Every probability is an exact one-sided fixed-margin Fisher tail computed
  with arbitrary-precision integers and :class:`fractions.Fraction`; no
  float, rounding, asymptotic approximation or random sampling is used.
  Benjamini--Hochberg adjustment uses a reverse cumulative minimum over the
  whole family, with stable ``(p, pair)`` ordering.
* Clues require ``a >= min_shared``, ``lift > 1`` and ``q <= alpha``; the
  shared-count filter is applied only after adjustment and never changes the
  family, the ranks or the adjusted values.

Only the Python standard library and SQLite are used; there is no service,
no web page and no network access.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
from fractions import Fraction
from pathlib import Path

from .protocol import parse_iso_date
from .warehouse import WAREHOUSE_FORMAT

AFFINITY_FORMAT = "catalog_harvest_affinity_report"
AFFINITY_SCHEMA_VERSION = 1
AFFINITY_MODE = "cross_topic_cooccurrence"
SUPPORTED_WAREHOUSE_VERSIONS = frozenset({"1"})

# Test-only hooks (not part of the operational contract).
ENV_SNAPSHOT_HOOK = "CATALOG_HARVEST_AFFINITY_SNAPSHOT_HOOK"
ENV_WRITE_FAIL = "CATALOG_HARVEST_AFFINITY_WRITE_FAIL"
ENV_REPLACE_FAIL = "CATALOG_HARVEST_AFFINITY_REPLACE_FAIL"

_DIGEST_FORMAT_VERSION = 1

# Ordinary finite decimal: a single whole digit (0 or 1), optionally followed
# by a decimal point and one to six fractional digits. No exponent, sign,
# whitespace, NaN or Infinity can match.
_ALPHA_RE = re.compile(r"[01](?:\.\d{1,6})?")


class AffinityError(ValueError):
    """Validation, warehouse-integrity or publication failure to fix."""


# ---------------------------------------------------------------------------
# Deterministic encoding helpers (kept local so the reader is self-contained)
# ---------------------------------------------------------------------------

def stable_json(obj) -> str:
    """Deterministic JSON text: sorted keys, compact separators, real Unicode."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fraction_text(value) -> str:
    """Canonical reduced ``numerator/positive-denominator`` string."""
    if value is None:
        return None
    if not isinstance(value, Fraction):
        value = Fraction(value)
    return f"{value.numerator}/{value.denominator}"


def parse_strict_date(value):
    """Strict YYYY-MM-DD validation surfacing failures as AffinityError."""
    try:
        return parse_iso_date(value)
    except ValueError as exc:
        raise AffinityError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Plan parsing: one byte capture, strict validation, duplicate keys rejected
# ---------------------------------------------------------------------------

class _DuplicateKeyDetector:
    """``object_pairs_hook`` that records every repeated JSON object key."""

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


def parse_plan(raw_bytes: bytes) -> dict:
    """Validate the plan document and return its structured, typed contents."""
    if not isinstance(raw_bytes, bytes) or not raw_bytes:
        raise AffinityError(
            "plan must be a non-empty strict UTF-8 JSON document")
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AffinityError("plan must be strict UTF-8 encoded JSON") from exc
    detector = _DuplicateKeyDetector()
    try:
        document = json.loads(text, object_pairs_hook=detector)
    except json.JSONDecodeError as exc:
        raise AffinityError(f"plan is not valid JSON: {exc}") from exc
    if detector.duplicates:
        raise AffinityError(
            "plan contains duplicate JSON object keys: "
            + ", ".join(map(str, detector.duplicates)))
    if not isinstance(document, dict) or set(document) != {
            "schema_version", "topics", "alpha", "min_shared"}:
        raise AffinityError(
            "plan top level must contain exactly schema_version, topics, "
            "alpha and min_shared")

    schema_version = document["schema_version"]
    # bool is a subclass of int; reject booleans explicitly.
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        raise AffinityError("plan schema_version must be the integer 1")
    if schema_version != 1:
        raise AffinityError("plan schema_version must be the integer 1")

    topics = document["topics"]
    if not isinstance(topics, list) or not (2 <= len(topics) <= 50):
        raise AffinityError(
            "plan topics must be an array of 2 to 50 unique topic ids")
    seen_topics = set()
    for topic in topics:
        if not isinstance(topic, str) or not topic or topic != topic.strip():
            raise AffinityError(
                "each plan topic must be a nonempty string without leading "
                "or trailing whitespace; ids are never trimmed or re-cased")
        if topic in seen_topics:
            raise AffinityError(f"plan topics must be unique: {topic!r}")
        seen_topics.add(topic)

    alpha_raw = document["alpha"]
    if not isinstance(alpha_raw, str):
        raise AffinityError(
            "plan alpha must be an ordinary decimal string, not a JSON number")
    if not _ALPHA_RE.fullmatch(alpha_raw):
        raise AffinityError(
            "plan alpha must be an ordinary decimal string such as \"0.05\" "
            "with at most six decimal places; exponents, signs, whitespace, "
            "NaN and Infinity are rejected")
    alpha_value = Fraction(alpha_raw)
    if not (Fraction(0) < alpha_value <= Fraction(1)):
        raise AffinityError("plan alpha must satisfy 0 < alpha <= 1")

    min_shared = document["min_shared"]
    if isinstance(min_shared, bool) or not isinstance(min_shared, int):
        raise AffinityError("plan min_shared must be an integer")
    if not (1 <= min_shared <= 1_000_000_000):
        raise AffinityError("plan min_shared must be in 1..1000000000")

    return {
        "schema_version": schema_version,
        "topics": list(topics),
        "alpha_raw": alpha_raw,
        "alpha": alpha_value,
        "min_shared": min_shared,
    }


# ---------------------------------------------------------------------------
# Read-only warehouse snapshot: one explicit read transaction
# ---------------------------------------------------------------------------

_REQUIRED_COLUMNS = {
    "wh_meta": {"key", "value"},
    "wh_taxonomy_nodes": {"topic_id", "label"},
    "wh_taxonomy_edges": {"child", "parent"},
    "wh_topic_closure": {"descendant", "ancestor", "distance"},
    "wh_versions": {
        "version_id", "identifier", "datestamp", "deleted", "title",
        "valid_from", "valid_to", "page_sha256"},
    "wh_version_ancestors": {"version_id", "topic_id"},
    "wh_version_topics": {"version_id", "topic_id"},
}

_REQUIRED_TABLES = tuple(_REQUIRED_COLUMNS)
_REQUIRED_META_KEYS = (
    "format", "format_version", "build_start", "build_end",
    "source_facts_sha256", "taxonomy_sha256")


def _require_columns(db, table, required):
    present = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
    missing = required - present
    if missing:
        raise AffinityError(
            f"warehouse table {table!r} is missing required columns: "
            + ", ".join(sorted(missing)))


def _run_snapshot_hook(hook, database_path, day_text):
    if hook is None:
        return
    if isinstance(hook, str):
        command = [sys.executable, "-I", hook, str(database_path), day_text]
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode != 0:
            raise AffinityError(
                f"snapshot hook failed ({completed.returncode}): "
                f"{completed.stderr.strip()}")
    else:
        hook(database_path, day_text)


def read_snapshot(database, day_text, selected_topics, snapshot_hook=None):
    """Read every fact this analysis uses inside one read-only transaction.

    The metadata is read first; an optional test-only hook may let another
    connection commit *after* the metadata read but *before* the version and
    bridge reads. Because all reads share one explicit deferred transaction,
    the returned snapshot still comes from a single point in time and can
    never mix facts from two commits.
    """
    path = Path(database)
    if not path.is_file():
        raise AffinityError(f"warehouse database not found: {path}")
    db = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    # Autocommit mode so the explicit BEGIN/COMMIT below is the only
    # transaction boundary; every SELECT then shares the same snapshot.
    db.isolation_level = None
    try:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        try:
            objects = {
                row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type IN ('table','view')")}
            absent = [table for table in _REQUIRED_TABLES
                      if table not in objects]
            if absent:
                raise AffinityError(
                    "database is not a supported catalog_harvest warehouse; "
                    "missing required tables: " + ", ".join(absent))

            meta_rows = db.execute(
                "SELECT key, value FROM wh_meta ORDER BY key").fetchall()
            meta = dict(meta_rows)
            missing_meta = [key for key in _REQUIRED_META_KEYS
                            if key not in meta]
            if missing_meta:
                raise AffinityError(
                    "warehouse wh_meta is missing keys: "
                    + ", ".join(missing_meta))
            if meta["format"] != WAREHOUSE_FORMAT:
                raise AffinityError(
                    "database is not a catalog_harvest warehouse "
                    f"(format={meta['format']!r})")
            if meta["format_version"] not in SUPPORTED_WAREHOUSE_VERSIONS:
                raise AffinityError(
                    "unsupported warehouse format version: "
                    f"{meta['format_version']!r}")
            start_text, end_text = meta["build_start"], meta["build_end"]
            start_day = parse_strict_date(start_text)
            end_day = parse_strict_date(end_text)
            day = parse_strict_date(day_text)
            if not (start_day <= day < end_day):
                raise AffinityError(
                    f"analysis date {day_text} must satisfy "
                    f"{start_text} <= date < {end_text}")

            # Hook point: metadata observed, versions/bridges not yet read.
            _run_snapshot_hook(snapshot_hook, path, day_text)

            for table, columns in _REQUIRED_COLUMNS.items():
                _require_columns(db, table, columns)

            labels = dict(db.execute(
                "SELECT topic_id, label FROM wh_taxonomy_nodes "
                "ORDER BY topic_id").fetchall())
            unknown = sorted(set(selected_topics) - set(labels))
            if unknown:
                raise AffinityError(
                    "plan references topic ids not registered in the "
                    "warehouse: " + ", ".join(unknown))

            edges = db.execute(
                "SELECT child, parent FROM wh_taxonomy_edges "
                "ORDER BY child, parent").fetchall()
            closure_rows = db.execute(
                "SELECT descendant, ancestor, distance FROM wh_topic_closure "
                "ORDER BY descendant, ancestor").fetchall()

            version_rows = db.execute(
                "SELECT version_id, identifier, datestamp, deleted, title, "
                "valid_from, valid_to, page_sha256 "
                "FROM wh_versions "
                "WHERE valid_from <= ? AND ? < valid_to "
                "ORDER BY identifier, datestamp, valid_from, version_id",
                (day_text, day_text)).fetchall()
            ancestor_rows = db.execute(
                "SELECT version_id, topic_id FROM wh_version_ancestors "
                "ORDER BY version_id, topic_id").fetchall()
            direct_rows = db.execute(
                "SELECT version_id, topic_id FROM wh_version_topics "
                "ORDER BY version_id, topic_id").fetchall()
        except Exception:
            db.execute("ROLLBACK")
            raise
        db.execute("COMMIT")
    finally:
        db.close()

    return _validate_snapshot(
        meta, labels, edges, closure_rows, version_rows,
        ancestor_rows, direct_rows, day_text)


def _validate_snapshot(meta, labels, edges, closure_rows, version_rows,
                       ancestor_rows, direct_rows, day_text):
    """Validate the structure of every fact this analysis actually uses."""
    # Closure: unique pairs, non-negative integer distances, known endpoints.
    closure = {}
    for descendant, ancestor, distance in closure_rows:
        if descendant not in labels or ancestor not in labels:
            raise AffinityError(
                f"corrupt closure row references unknown topic: "
                f"({descendant!r}, {ancestor!r})")
        if isinstance(distance, bool) or not isinstance(distance, int) \
                or distance < 0:
            raise AffinityError(
                f"corrupt closure row ({descendant!r}, {ancestor!r}): "
                "distance must be a non-negative integer")
        pair = (descendant, ancestor)
        if pair in closure:
            raise AffinityError(f"duplicate closure pair: {pair}")
        closure[pair] = distance
    for edge_child, edge_parent in edges:
        if edge_child not in labels or edge_parent not in labels:
            raise AffinityError(
                f"corrupt taxonomy edge references unknown topic: "
                f"({edge_child!r}, {edge_parent!r})")
        if (edge_child, edge_parent) not in closure:
            raise AffinityError(
                f"taxonomy edge {edge_child!r}->{edge_parent!r} has no "
                "matching closure row")

    # Bridges: unique rows referencing known topics and known versions.
    effective_ids = {row[0] for row in version_rows}
    ancestor_map = {}
    for version_id, topic_id in ancestor_rows:
        if topic_id not in labels:
            raise AffinityError(
                f"corrupt ancestor bridge for version_id={version_id}: "
                f"unknown topic {topic_id!r}")
        bucket = ancestor_map.setdefault(version_id, set())
        if topic_id in bucket:
            raise AffinityError(
                f"duplicate ancestor bridge row: "
                f"(version_id={version_id}, topic_id={topic_id!r})")
        bucket.add(topic_id)
    direct_map = {}
    for version_id, topic_id in direct_rows:
        if topic_id not in labels:
            raise AffinityError(
                f"corrupt direct-topic bridge for version_id={version_id}: "
                f"unknown topic {topic_id!r}")
        bucket = direct_map.setdefault(version_id, set())
        if topic_id in bucket:
            raise AffinityError(
                f"duplicate direct-topic bridge row: "
                f"(version_id={version_id}, topic_id={topic_id!r})")
        bucket.add(topic_id)

    versions = []
    identifiers_seen = {}
    for (version_id, identifier, datestamp, deleted, title, valid_from,
         valid_to, page_sha256) in version_rows:
        if not isinstance(identifier, str) or not identifier:
            raise AffinityError(
                f"corrupt version row version_id={version_id}: "
                "identifier must be a nonempty string")
        if identifier in identifiers_seen:
            raise AffinityError(
                f"corrupt version facts: identifier {identifier!r} has more "
                f"than one effective version on {day_text} "
                f"(version_id {identifiers_seen[identifier]} and {version_id})")
        identifiers_seen[identifier] = version_id
        parse_strict_date(valid_from)
        parse_strict_date(valid_to)
        if not valid_from < valid_to:
            raise AffinityError(
                f"corrupt version {identifier!r}@{datestamp}: "
                f"valid_from {valid_from!r} must precede valid_to {valid_to!r}")
        if not (valid_from <= day_text < valid_to):
            raise AffinityError(
                f"corrupt version {identifier!r}@{datestamp}: interval does "
                f"not cover {day_text}")
        if deleted not in (0, 1):
            raise AffinityError(
                f"corrupt version {identifier!r}@{datestamp}: "
                "deleted must be 0 or 1")
        if not isinstance(page_sha256, str) or not page_sha256:
            raise AffinityError(
                f"corrupt version {identifier!r}@{datestamp}: "
                "page_sha256 must be a nonempty string")
        if deleted:
            if title is not None:
                raise AffinityError(
                    f"corrupt deleted version {identifier!r}@{datestamp}: "
                    "deleted versions must carry no title")
            if ancestor_map.get(version_id) or direct_map.get(version_id):
                raise AffinityError(
                    f"corrupt deleted version {identifier!r}@{datestamp}: "
                    "deleted versions must carry no topic bridges")
        else:
            if not isinstance(title, str) or not title \
                    or title != title.strip():
                raise AffinityError(
                    f"corrupt active version {identifier!r}@{datestamp}: "
                    "title must be a nonempty trimmed string")
        direct = direct_map.get(version_id, set())
        ancestors = ancestor_map.get(version_id, set())
        if not direct <= ancestors:
            raise AffinityError(
                f"corrupt bridges for {identifier!r}@{datestamp}: direct "
                "topics must be a subset of the de-duplicated ancestor set")
        versions.append({
            "version_id": version_id,
            "identifier": identifier,
            "datestamp": datestamp,
            "deleted": int(deleted),
            "title": title,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "page_sha256": page_sha256,
            "direct_topics": sorted(direct),
            "ancestor_topics": sorted(ancestors),
        })

    return {
        "meta": meta,
        "labels": labels,
        "edges": edges,
        "closure": closure,
        "versions": versions,
    }


# ---------------------------------------------------------------------------
# Exact statistics: contingency tables, Fisher tail, Benjamini--Hochberg
# ---------------------------------------------------------------------------

def ordered_pair(left, right):
    """Sort two topic ids by Unicode code point into ``(A, B)``."""
    return tuple(sorted((left, right)))


def fisher_one_sided_tail(n, n_a, n_b, a):
    """Exact one-sided fixed-margin Fisher enrichment tail as a Fraction.

    ``p = sum_{k=a..min(nA,nB)} C(nA,k) C(n-nA,nB-k) / C(n,nB)`` over
    feasible ``k`` only. All arithmetic is exact; zero-margin legal tables
    yield ``1/1``.
    """
    if n <= 0:
        return None
    for name, value in (("N", n), ("nA", n_a), ("nB", n_b), ("a", a)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AffinityError(f"{name} must be a non-negative integer")
    upper = min(n_a, n_b)
    lower = max(0, n_a + n_b - n)
    if not (lower <= a <= upper):
        raise AffinityError(
            f"infeasible contingency cell a={a} for margins N={n}, "
            f"nA={n_a}, nB={n_b}")
    denominator = math.comb(n, n_b)
    total = Fraction(0)
    for k in range(a, upper + 1):
        if k < lower:
            continue
        total += Fraction(
            math.comb(n_a, k) * math.comb(n - n_a, n_b - k), denominator)
    return total


def benjamini_hochberg(p_values, m):
    """BH adjustment by reverse cumulative minimum, stable on equal p.

    ``p_values`` is a sequence of ``(pair, Fraction)`` with exactly ``m``
    members. Returns a mapping ``pair -> (rank, p, q)`` where ties keep the
    order implied by the pair's code-point ordering.
    """
    if len(p_values) != m:
        raise AffinityError("BH family size does not match its p-value count")
    if m == 0:
        return {}
    ordering = sorted(p_values, key=lambda item: (item[1], item[0]))
    adjusted = {}
    running = Fraction(1)
    for rank in range(m, 0, -1):
        pair, p_value = ordering[rank - 1]
        candidate = Fraction(m) * p_value / rank
        if candidate < running:
            running = candidate
        if running > 1:
            running = Fraction(1)
        adjusted[pair] = (rank, p_value, running)
    return adjusted


def _ratio_or_none(numerator, denominator):
    if denominator == 0:
        return None
    return Fraction(numerator, denominator)


def build_family(snapshot, plan):
    """Decide exclusions and the fixed testing family before any statistic."""
    closure = snapshot["closure"]
    selected = sorted(plan["topics"])
    excluded = []
    testable_pairs = []
    for index, left in enumerate(selected):
        for right in selected[index + 1:]:
            pair = ordered_pair(left, right)
            first, second = pair
            if (first, second) in closure:
                excluded.append({
                    "pair": [first, second],
                    "status": "ancestor_related",
                    "descendant": first,
                    "ancestor": second,
                    "shortest_distance": closure[(first, second)],
                    "evidence": "wh_topic_closure",
                })
            elif (second, first) in closure:
                excluded.append({
                    "pair": [first, second],
                    "status": "ancestor_related",
                    "descendant": second,
                    "ancestor": first,
                    "shortest_distance": closure[(second, first)],
                    "evidence": "wh_topic_closure",
                })
            else:
                testable_pairs.append(pair)
    # Family membership is now fixed; counts and p-values cannot change it.
    return selected, excluded, testable_pairs


def analyze(database, plan_path, day, snapshot_hook=None):
    """Read the warehouse snapshot and build the in-memory report dict.

    ``database`` is never modified and no report is written here; call
    :func:`serialize_report` and :func:`publish_report` to publish it. The
    exact plan bytes are captured once and shared by parsing and hashing.
    """
    plan_path = Path(plan_path)
    database = Path(database)
    plan_bytes = plan_path.read_bytes()
    plan = parse_plan(plan_bytes)
    plan_digest = sha256_hex(plan_bytes)
    day_text = day.isoformat() if hasattr(day, "isoformat") else str(day)
    parse_strict_date(day_text)

    snapshot = read_snapshot(database, day_text, plan["topics"], snapshot_hook)
    return _build_report(snapshot, plan, plan_bytes, plan_digest, day_text)


def serialize_report(report) -> bytes:
    """Deterministic compact sorted-key UTF-8 bytes for a report dict."""
    return json.dumps(
        report, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False).encode("utf-8")


def publish_report(database, plan_path, output, report):
    """Atomically publish ``report`` and return its byte SHA256.

    Serialization happens before any output file is touched. The report is
    written to a same-directory temporary file, closed and only then
    atomically replaced into place. The output may not alias the database or
    the plan, including via symlinks or hard links.
    """
    output = Path(output)
    _ensure_distinct_paths(
        ("warehouse database", Path(database)),
        ("plan file", Path(plan_path)),
        ("report output", output))
    payload = serialize_report(report)
    _publish_atomic(output, payload)
    return sha256_hex(payload)


def _build_report(snapshot, plan, plan_bytes, plan_digest, day_text):
    meta = snapshot["meta"]
    labels = snapshot["labels"]
    selected, excluded, testable_pairs = build_family(snapshot, plan)
    m = len(testable_pairs)

    # One vote per active effective version; deleted versions stay in the
    # evidence and the known-deleted count but never enter N.
    active_versions = [v for v in snapshot["versions"] if not v["deleted"]]
    n_population = len(active_versions)
    known_deleted = sum(1 for v in snapshot["versions"] if v["deleted"])

    selected_set = set(selected)
    record_hits = {}
    evidence = []
    for version in sorted(snapshot["versions"],
                          key=lambda v: (v["identifier"], v["datestamp"],
                                         v["valid_from"], v["version_id"])):
        hits = sorted(
            selected_set.intersection(version["ancestor_topics"]))
        record_hits[version["version_id"]] = hits
        evidence.append({
            "identifier": version["identifier"],
            "datestamp": version["datestamp"],
            "valid_from": version["valid_from"],
            "valid_to": version["valid_to"],
            "deleted": bool(version["deleted"]),
            "title": version["title"],
            "page_summary": {"page_sha256": version["page_sha256"]},
            "selected_topics": hits,
        })

    tests = []
    p_values = []
    for pair in testable_pairs:
        first, second = pair
        a = b = c = d = 0
        for version in active_versions:
            hits = set(record_hits[version["version_id"]])
            has_first = first in hits
            has_second = second in hits
            if has_first and has_second:
                a += 1
            elif has_first:
                b += 1
            elif has_second:
                c += 1
            else:
                d += 1
        if a + b + c + d != n_population:
            raise AffinityError(
                f"internal error: table for {pair} does not sum to N")
        n_a = a + b
        n_b = a + c
        if n_population > 0:
            p_value = fisher_one_sided_tail(
                n_population, n_a, n_b, a)
            lift = _ratio_or_none(a * n_population, n_a * n_b)
            metrics = {
                "a_over_n": Fraction(a, n_population),
                "confidence_a_to_b": _ratio_or_none(a, n_a),
                "confidence_b_to_a": _ratio_or_none(a, n_b),
                "lift": lift,
            }
            p_values.append((pair, p_value))
        else:
            p_value = None
            metrics = {
                "a_over_n": None,
                "confidence_a_to_b": _ratio_or_none(a, n_a),
                "confidence_b_to_a": _ratio_or_none(a, n_b),
                "lift": _ratio_or_none(a * n_population, n_a * n_b),
            }
        tests.append({
            "pair": [first, second],
            "table": {"a": a, "b": b, "c": c, "d": d, "N": n_population},
            "marginals": {
                "n_A": n_a,
                "n_B": n_b,
                "neither": d,
            },
            "metrics": metrics,
            "_p": p_value,
        })

    adjusted = benjamini_hochberg(p_values, m) if n_population > 0 else {}
    alpha = plan["alpha"]
    min_shared = plan["min_shared"]
    clues = []
    for entry in tests:
        pair = tuple(entry["pair"])
        if n_population > 0:
            rank, p_value, q_value = adjusted[pair]
            entry["fisher"] = {
                "rank": rank,
                "p": p_value,
                "q": q_value,
                "family_size": m,
            }
            lift = entry["metrics"]["lift"]
            share_ok = entry["table"]["a"] >= min_shared
            lift_ok = lift is not None and lift > 1
            q_ok = q_value <= alpha
            selected_now = share_ok and lift_ok and q_ok
            reasons = {
                "a_ge_min_shared": {
                    "value": entry["table"]["a"],
                    "threshold": min_shared,
                    "passed": share_ok,
                },
                "lift_gt_1": {
                    "value": fraction_text(lift),
                    "passed": lift_ok,
                },
                "q_le_alpha": {
                    "value": fraction_text(q_value),
                    "threshold_raw": plan["alpha_raw"],
                    "threshold_exact": fraction_text(alpha),
                    "passed": q_ok,
                },
            }
        else:
            entry["fisher"] = {
                "rank": None, "p": None, "q": None, "family_size": m}
            selected_now = False
            reasons = {
                "a_ge_min_shared": {
                    "value": entry["table"]["a"],
                    "threshold": min_shared,
                    "passed": False,
                },
                "lift_gt_1": {"value": None, "passed": False},
                "q_le_alpha": {
                    "value": None,
                    "threshold_raw": plan["alpha_raw"],
                    "threshold_exact": fraction_text(alpha),
                    "passed": False,
                },
                "diagnostic": "no_active_population",
            }
        entry["selection"] = {
            "selected": selected_now,
            "reasons": reasons,
            "criteria": {
                "alpha_raw": plan["alpha_raw"],
                "alpha_exact": fraction_text(alpha),
                "min_shared": min_shared,
            },
        }
        if selected_now:
            clues.append(entry)

    # Exact-value ranking: q asc, lift desc, a desc, pair asc.
    clues.sort(key=lambda entry: (
        entry["fisher"]["q"],
        -entry["metrics"]["lift"],
        -entry["table"]["a"],
        tuple(entry["pair"]),
    ))

    diagnostics = []
    if n_population == 0:
        diagnostics.append("no_active_population")
    if m == 0:
        diagnostics.append("no_eligible_pairs")

    selected_relations = _selected_relations(snapshot, selected)
    serialized_tests = [_serialize_test(entry) for entry in tests]
    serialized_clues = [_serialize_clue(entry) for entry in clues]

    report = {
        "schema_version": AFFINITY_SCHEMA_VERSION,
        "mode": AFFINITY_MODE,
        "format": AFFINITY_FORMAT,
        "date": day_text,
        "warehouse": {
            "format": meta["format"],
            "format_version": meta["format_version"],
            "build_start": meta["build_start"],
            "build_end": meta["build_end"],
            "source_facts_sha256": meta["source_facts_sha256"],
            "taxonomy_sha256": meta["taxonomy_sha256"],
        },
        "plan": {
            "schema_version": plan["schema_version"],
            "selected_topics": [
                {"topic_id": topic, "label": labels[topic]}
                for topic in selected],
            "alpha": {
                "raw": plan["alpha_raw"],
                "exact": fraction_text(plan["alpha"]),
            },
            "min_shared": plan["min_shared"],
        },
        "family": {
            "m": m,
            "pair_ordering": "unicode_code_point",
            "definition": "all unordered selected-topic pairs that are not "
                          "ancestor-related in wh_topic_closure; fixed before "
                          "any co-occurrence, lift, p or min_shared check",
        },
        "excluded_pairs": excluded,
        "tests": serialized_tests,
        "clues": serialized_clues,
        "population": {
            "N": n_population,
            "active_versions": n_population,
            "known_deleted_versions": known_deleted,
            "known_versions": len(snapshot["versions"]),
        },
        "diagnostics": diagnostics,
        "record_evidence": evidence,
        "selected_relations": selected_relations,
        "interpretation": [
            "p is a diagnostic one-sided Fisher exact-tail probability under "
            "a fixed-margin hypergeometric model, not a population-level "
            "significance guarantee.",
            "q is the Benjamini-Hochberg adjustment for exactly this fixed "
            "testing family; no FDR guarantee is made under arbitrary "
            "dependence between topic pairs, and exclusions do not imply the "
            "remaining tests are independent.",
            "The catalog is an incomplete, non-random administrative sample; "
            "lift and enrichment must not be read as universal population "
            "patterns or causal effects.",
            "A clue is a review lead for manual inspection, not an automatic "
            "classification decision or a proven interdisciplinary link.",
            "page_summary cites the warehouse page digest only; the original "
            "XML text is not embedded in this report.",
        ],
    }
    report["digests"] = {
        "plan_bytes_sha256": plan_digest,
        "metadata_sha256": metadata_digest(report),
        "selected_relations_sha256": relations_digest(report),
        "record_evidence_sha256": evidence_digest(report),
    }
    return report


def _selected_relations(snapshot, selected):
    selected_set = set(selected)
    edges = sorted(
        [child, parent] for child, parent in snapshot["edges"]
        if child in selected_set and parent in selected_set)
    closure = sorted(
        [descendant, ancestor, distance]
        for (descendant, ancestor), distance in snapshot["closure"].items()
        if descendant in selected_set and ancestor in selected_set)
    return {"edges": edges, "closure": closure}


def _serialize_metric(value):
    return fraction_text(value) if value is not None else None


def _serialize_test(entry):
    fisher = entry["fisher"]
    metrics = entry["metrics"]
    return {
        "pair": entry["pair"],
        "table": entry["table"],
        "marginals": entry["marginals"],
        "metrics": {
            "a_over_n": _serialize_metric(metrics["a_over_n"]),
            "confidence_a_to_b": _serialize_metric(
                metrics["confidence_a_to_b"]),
            "confidence_b_to_a": _serialize_metric(
                metrics["confidence_b_to_a"]),
            "lift": _serialize_metric(metrics["lift"]),
        },
        "fisher": {
            "rank": fisher["rank"],
            "p": fraction_text(fisher["p"]),
            "q": fraction_text(fisher["q"]),
            "family_size": fisher["family_size"],
        },
        "selection": entry["selection"],
    }


def _serialize_clue(entry):
    serialized = _serialize_test(entry)
    return {
        "pair": entry["pair"],
        "table": serialized["table"],
        "metrics": serialized["metrics"],
        "fisher": serialized["fisher"],
        "selection": serialized["selection"],
    }


# ---------------------------------------------------------------------------
# Logical digests, rebuildable from the report alone (warehouse removed)
# ---------------------------------------------------------------------------

def metadata_digest(report) -> str:
    payload = {
        "format": "catalog_harvest_affinity_metadata",
        "format_version": _DIGEST_FORMAT_VERSION,
        "warehouse_format": report["warehouse"]["format"],
        "warehouse_format_version": report["warehouse"]["format_version"],
        "build_start": report["warehouse"]["build_start"],
        "build_end": report["warehouse"]["build_end"],
        "date": report["date"],
        "source_facts_sha256": report["warehouse"]["source_facts_sha256"],
        "taxonomy_sha256": report["warehouse"]["taxonomy_sha256"],
    }
    return sha256_hex(stable_json(payload).encode("utf-8"))


def relations_digest(report) -> str:
    payload = {
        "format": "catalog_harvest_affinity_selected_relations",
        "format_version": _DIGEST_FORMAT_VERSION,
        "selected_topics": report["plan"]["selected_topics"],
        "edges": report["selected_relations"]["edges"],
        "closure": report["selected_relations"]["closure"],
    }
    return sha256_hex(stable_json(payload).encode("utf-8"))


def evidence_digest(report) -> str:
    payload = {
        "format": "catalog_harvest_affinity_record_evidence",
        "format_version": _DIGEST_FORMAT_VERSION,
        "date": report["date"],
        "records": report["record_evidence"],
    }
    return sha256_hex(stable_json(payload).encode("utf-8"))


# ---------------------------------------------------------------------------
# Atomic single-writer publication and alias protection
# ---------------------------------------------------------------------------

def _ensure_distinct_paths(*labeled_paths):
    """Reject same-file aliases (symlinks/hard links) between paths."""
    resolved = {}
    stats = {}
    for label, candidate in labeled_paths:
        path = Path(candidate)
        real = os.path.realpath(path)
        if real in resolved:
            raise AffinityError(
                f"{label} must not be the same file as {resolved[real]} "
                f"(resolved path {real})")
        resolved[real] = label
        if path.exists():
            stats[(label, real)] = os.stat(path)
    seen_inodes = {}
    for (label, real), stat_result in stats.items():
        inode = (stat_result.st_dev, stat_result.st_ino)
        if inode in seen_inodes:
            raise AffinityError(
                f"{label} is a hard-link alias of {seen_inodes[inode]}")
        seen_inodes[inode] = label


def _publish_atomic(output, payload):
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    handle = None
    try:
        from tempfile import NamedTemporaryFile
        with NamedTemporaryFile(
                prefix=".affinity-", suffix=".tmp", dir=output.parent,
                delete=False) as handle:
            tmp_path = Path(handle.name)
        if os.environ.get(ENV_WRITE_FAIL):
            raise AffinityError("injected write failure (test hook)")
        with open(tmp_path, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if os.environ.get(ENV_REPLACE_FAIL):
            raise AffinityError("injected replace failure (test hook)")
        os.replace(tmp_path, output)
        tmp_path = None
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


# ---------------------------------------------------------------------------
# Command line: python -m catalog_harvest.affinity
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m catalog_harvest.affinity",
        description="Cross-topic co-occurrence affinity report over a "
                    "historical subject warehouse")
    parser.add_argument("--database", required=True,
                        help="existing catalog_harvest warehouse SQLite file")
    parser.add_argument("--date", required=True, dest="day",
                        help="analysis day YYYY-MM-DD (warehouse [start,end))")
    parser.add_argument("--plan", required=True,
                        help="strict UTF-8 JSON analysis plan")
    parser.add_argument("--output", required=True,
                        help="destination JSON report (atomic replacement)")
    args = parser.parse_args(argv)
    try:
        report = analyze(
            args.database, args.plan, args.day,
            snapshot_hook=os.environ.get(ENV_SNAPSHOT_HOOK))
        report_digest = publish_report(
            args.database, args.plan, args.output, report)
    except (AffinityError, OSError, sqlite3.DatabaseError) as exc:
        parser.exit(2, f"affinity analysis failed: {exc}\n")
    summary = {
        "status": "published",
        "date": report["date"],
        "N": report["population"]["N"],
        "known_deleted_versions":
            report["population"]["known_deleted_versions"],
        "family_size": report["family"]["m"],
        "excluded_pairs": len(report["excluded_pairs"]),
        "clues": len(report["clues"]),
        "diagnostics": report["diagnostics"],
        "report_sha256": report_digest,
    }
    print(json.dumps(summary, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
