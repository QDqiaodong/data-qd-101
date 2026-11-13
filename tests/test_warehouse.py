"""Independent tests for the offline historical subject warehouse.

The reference oracle in :class:`WarehouseOracleTests` deliberately re-derives
point-in-time state day by day (latest datestamp per identifier, explicit
set-based ancestor walks) without calling any production closure or
aggregation routine, and then compares every node and catalog count against
both the as-of reader and the materialized sparse interval tables.

All data is original synthetic content; no HTTP, real archive data or
third-party dependency is involved.
"""
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from xml.sax.saxutils import escape

from catalog_harvest.harvest import DDL as FULL_DDL, harvest
from catalog_harvest.transport import page_fetcher
from catalog_harvest.demo import fixture_server
from catalog_harvest.sync import SYNC_DDL
from catalog_harvest.warehouse import (
    build_warehouse, as_of, parse_taxonomy, read_formal_facts,
    source_facts_digest, WarehouseError, WAREHOUSE_FORMAT,
    ENV_WRITE_FAIL, ENV_REPLACE_FAIL)
from catalog_harvest.protocol import canonical

DATA = Path(__file__).resolve().parents[1] / "data"
ROOT = Path(__file__).resolve().parents[1]

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DCO_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"


# ---------------------------------------------------------------------------
# Synthetic source-database and taxonomy builders
# ---------------------------------------------------------------------------

def record_xml(rec):
    identifier, stamp = rec["identifier"], rec["datestamp"]
    if rec["deleted"]:
        return (f'<record><header status="deleted"><identifier>{escape(identifier)}'
                f'</identifier><datestamp>{stamp}</datestamp></header></record>')
    subjects = "".join(
        f"<dc:subject>{escape(s)}</dc:subject>" for s in rec["subjects"])
    return (
        "<record><header>"
        f"<identifier>{escape(identifier)}</identifier>"
        f"<datestamp>{stamp}</datestamp></header>"
        f'<metadata><oai_dc:dc xmlns:oai_dc="{DCO_NS}" xmlns:dc="{DC_NS}">'
        f"<dc:title>{escape(rec['title'])}</dc:title>{subjects}"
        "</oai_dc:dc></metadata></record>")


def page_xml(records):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<OAI-PMH xmlns="{OAI_NS}" xmlns:oai_dc="{DCO_NS}" '
        f'xmlns:dc="{DC_NS}"><ListRecords>'
        f'{"".join(record_xml(r) for r in records)}'
        "</ListRecords></OAI-PMH>").encode("utf-8")


def make_record(identifier, stamp, deleted=False, title=None, subjects=()):
    return {"identifier": identifier, "datestamp": stamp, "deleted": deleted,
            "title": title, "subjects": sorted(subjects)}


def write_source(path, revisions, sync_staging=False):
    """Write a harvest-shaped source DB; optionally add managed sync tables."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(FULL_DDL)
        if sync_staging:
            db.executescript(SYNC_DDL)
            db.execute("INSERT INTO sync_meta VALUES (?,?)",
                       ("endpoint", "http://127.0.0.1:9/oai"))
            db.execute(
                "INSERT INTO sync_window (state, stage, endpoint, window_from, "
                "window_until, overlap_days, max_pages, traversal, "
                "restarts_used, pages_confirmed, records_confirmed, "
                "next_token, last_error, created_at, updated_at) "
                "VALUES ('active','fetching',?,?,?,?,?,1,0,0,0,NULL,NULL,'t','t')",
                ("http://127.0.0.1:9/oai", "2026-01-01", "2026-01-03", 2, 20))
        staged_identities = set()
        for index, rec in enumerate(revisions):
            raw = page_xml([rec])
            digest = hashlib.sha256(raw).hexdigest()
            db.execute("INSERT OR IGNORE INTO pages VALUES (?,?)", (digest, raw))
            db.execute(
                "INSERT INTO revisions VALUES (?,?,?,?,?,?,?)",
                (rec["identifier"], rec["datestamp"], int(rec["deleted"]),
                 rec["title"], json.dumps(rec["subjects"]), canonical(rec),
                 digest))
            if sync_staging:
                # One staging row per formal identity; staging additionally
                # carries a ghost identifier never promoted to formal facts,
                # whose page is intentionally invalid XML.
                identity = (rec["identifier"], rec["datestamp"])
                if identity not in staged_identities:
                    staged_identities.add(identity)
                    db.execute(
                        "INSERT INTO sync_stage_revisions VALUES "
                        "(?,?,?,?,?,?,?,?,?,?)",
                        (1, 1, rec["identifier"], rec["datestamp"],
                         int(rec["deleted"]), rec["title"],
                         json.dumps(rec["subjects"]), canonical(rec),
                         digest, index + 1))
                db.execute(
                    "INSERT INTO sync_stage_pages VALUES (?,?,?,?,?,?,?,?)",
                    (1, 1, index + 1,
                     hashlib.sha256(b"<broken>").hexdigest(),
                     b"<broken>", 1, "first", None))
        if sync_staging:
            ghost = make_record("oai:demo:GHOST", "2026-01-02", title="Ghost")
            db.execute(
                "INSERT INTO sync_stage_revisions VALUES (?,?,?,?,?,?,?,?,?,?)",
                (1, 1, ghost["identifier"], ghost["datestamp"], 0,
                 ghost["title"], json.dumps(ghost["subjects"]),
                 canonical(ghost),
                 hashlib.sha256(b"<broken>").hexdigest(), 999))


def scenario_taxonomy():
    return {
        "schema_version": 1,
        "nodes": [
            {"topic_id": "leaf", "label": "Leaf"},
            {"topic_id": "left", "label": "Left"},
            {"topic_id": "right", "label": "Right"},
            {"topic_id": "root", "label": "Root"},
            {"topic_id": "unused", "label": "Unused"}],
        "edges": [
            {"child": "leaf", "parent": "left"},
            {"child": "leaf", "parent": "right"},
            {"child": "left", "parent": "root"},
            {"child": "right", "parent": "root"}],
        "bindings": [
            {"subject": "hydro", "topic_id": "leaf"},
            {"subject": "water", "topic_id": "leaf"}]}


def scenario_revisions():
    return [
        make_record("oai:demo:A", "2026-01-01", title="River flow",
                    subjects=["hydro", "water"]),
        make_record("oai:demo:B", "2026-01-02", title="Survey B",
                    subjects=["water", "mystery"]),
        make_record("oai:demo:A", "2026-01-03", deleted=True),
        make_record("oai:demo:B", "2026-01-04", title="Survey B revised",
                    subjects=["mystery"]),
        make_record("oai:demo:A", "2026-01-05", title="River flow reborn")]


def write_taxonomy(path, document):
    path = Path(path)
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return path


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    return db


def daterange(start_text, end_text):
    current = date.fromisoformat(start_text)
    end = date.fromisoformat(end_text)
    while current < end:
        yield current
        current += timedelta(days=1)


class WarehouseTestBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.source = self.dir / "source.sqlite"
        self.taxonomy = self.dir / "taxonomy.json"
        self.warehouse = self.dir / "warehouse.sqlite"

    def build(self, revisions=None, taxonomy=None, start="2026-01-01",
              end="2026-01-07", sync_staging=False):
        write_source(self.source,
                     scenario_revisions() if revisions is None else revisions,
                     sync_staging=sync_staging)
        write_taxonomy(self.taxonomy,
                       scenario_taxonomy() if taxonomy is None else taxonomy)
        return build_warehouse(self.source, self.taxonomy, start, end,
                               self.warehouse)


# ---------------------------------------------------------------------------
# Acceptance 1: original full-harvest fixture baseline
# ---------------------------------------------------------------------------

class FullHarvestBaselineTests(WarehouseTestBase):
    def test_original_fixture_baseline_then_warehouse_build(self):
        with fixture_server(DATA) as endpoint:
            first = harvest(page_fetcher(endpoint), self.source)
            second = harvest(page_fetcher(endpoint), self.source)
        self.assertEqual(first, {
            "pages": 2, "records_seen": 4, "inserted_revisions": 3,
            "active_records": 1, "deleted_records": 1})
        self.assertEqual(second["inserted_revisions"], 0)
        with connect(self.source) as db:
            self.assertEqual(
                db.execute("SELECT count(*) FROM pages").fetchone()[0], 2)
            self.assertEqual(
                db.execute("SELECT count(*) FROM revisions").fetchone()[0], 3)

        write_taxonomy(self.taxonomy, {
            "schema_version": 1,
            "nodes": [{"topic_id": "hydro", "label": "Hydrology"}],
            "edges": [],
            "bindings": [{"subject": "hydrology", "topic_id": "hydro"}]})
        summary = build_warehouse(
            self.source, self.taxonomy, "2026-01-01", "2026-01-10",
            self.warehouse)
        # All 3 formal revisions and both formal pages are preserved in the
        # self-contained product; no sync staging exists in this source.
        self.assertEqual(summary["source_pages"], 2)
        self.assertEqual(summary["source_revisions"], 3)
        with connect(self.warehouse) as db:
            self.assertEqual(
                db.execute("SELECT count(*) FROM wh_source_pages")
                .fetchone()[0], 2)
            self.assertEqual(
                db.execute("SELECT count(*) FROM wh_source_revisions")
                .fetchone()[0], 3)
            self.assertEqual(
                db.execute("SELECT value FROM wh_meta WHERE key='format'")
                .fetchone()[0], WAREHOUSE_FORMAT)


# ---------------------------------------------------------------------------
# Acceptance 2: canonical diamond scenario, closure and sparse topic counts
# ---------------------------------------------------------------------------

class DiamondScenarioTests(WarehouseTestBase):
    def setUp(self):
        super().setUp()
        self.build()

    def test_root_daily_counts_record_days_and_four_segments(self):
        expected_daily = [1, 2, 1, 0, 0, 0]
        for day, expected in zip(
                daterange("2026-01-01", "2026-01-07"), expected_daily):
            result = as_of(self.warehouse, day)
            self.assertEqual(
                result["topic_record_counts"]["root"], expected,
                f"root count wrong on {day}")
        with connect(self.warehouse) as db:
            segments = db.execute(
                "SELECT valid_from, valid_to, record_count "
                "FROM wh_topic_intervals WHERE topic_id='root' "
                "ORDER BY valid_from").fetchall()
            self.assertEqual([tuple(r) for r in segments], [
                ("2026-01-01", "2026-01-02", 1),
                ("2026-01-02", "2026-01-03", 2),
                ("2026-01-03", "2026-01-04", 1),
                ("2026-01-04", "2026-01-07", 0)])
            days = db.execute(
                "SELECT record_days FROM wh_topic_record_days "
                "WHERE topic_id='root'").fetchone()[0]
            self.assertEqual(days, 4)
            self.assertIsInstance(days, int)

    def test_leaf_branch_and_unused_record_days(self):
        with connect(self.warehouse) as db:
            row_days = dict(db.execute(
                "SELECT topic_id, record_days FROM wh_topic_record_days")
                .fetchall())
        self.assertEqual(row_days, {
            "leaf": 4, "left": 4, "right": 4, "root": 4, "unused": 0})
        for value in row_days.values():
            self.assertIsInstance(value, int)

    def test_leaf_to_root_shortest_depth_single_row(self):
        with connect(self.warehouse) as db:
            rows = db.execute(
                "SELECT distance FROM wh_topic_closure "
                "WHERE descendant='leaf' AND ancestor='root'").fetchall()
            self.assertEqual([r[0] for r in rows], [2])
            # One closure row per pair even though two paths reach root.
            count = db.execute(
                "SELECT count(*) FROM wh_topic_closure").fetchone()[0]
            # 5 self rows + leaf{left,right,root}=3 + left{root}=1
            # + right{root}=1 + unused self already counted = 10.
            self.assertEqual(count, 10)

    def test_multiple_subjects_and_diamond_do_not_double_vote(self):
        with connect(self.warehouse) as db:
            # A/Jan1 has both hydro and water bound to leaf: direct bridge one
            # row, and root ancestor bridge one row.
            a_jan1 = db.execute(
                "SELECT version_id FROM wh_versions "
                "WHERE identifier='oai:demo:A' AND datestamp='2026-01-01'"
            ).fetchone()[0]
            direct = [r[0] for r in db.execute(
                "SELECT topic_id FROM wh_version_topics WHERE version_id=?",
                (a_jan1,))]
            self.assertEqual(direct, ["leaf"])
            ancestors = [r[0] for r in db.execute(
                "SELECT topic_id FROM wh_version_ancestors WHERE version_id=? "
                "ORDER BY topic_id", (a_jan1,))]
            self.assertEqual(ancestors, ["leaf", "left", "right", "root"])
            # Both original subjects remain traceable.
            subjects = [r[0] for r in db.execute(
                "SELECT subject FROM wh_version_subjects WHERE version_id=? "
                "ORDER BY subject", (a_jan1,))]
            self.assertEqual(subjects, ["hydro", "water"])
            mappings = [tuple(r) for r in db.execute(
                "SELECT subject, topic_id FROM wh_version_subject_topics "
                "WHERE version_id=? ORDER BY subject", (a_jan1,))]
            self.assertEqual(mappings, [("hydro", "leaf"), ("water", "leaf")])

    def test_deleted_versions_have_no_topic_bridges(self):
        with connect(self.warehouse) as db:
            deleted_id = db.execute(
                "SELECT version_id FROM wh_versions "
                "WHERE identifier='oai:demo:A' AND deleted=1").fetchone()[0]
            for table in ("wh_version_topics", "wh_version_ancestors",
                          "wh_version_subjects", "wh_version_subject_topics"):
                self.assertEqual(db.execute(
                    f"SELECT count(*) FROM {table} WHERE version_id=?",
                    (deleted_id,)).fetchone()[0], 0)

    def test_adjacent_versions_are_never_merged(self):
        # Two revisions with identical payloads but different dates must remain
        # separate version identities.
        revisions = [
            make_record("oai:demo:C", "2026-01-02", title="Same",
                        subjects=["water"]),
            make_record("oai:demo:C", "2026-01-04", title="Same",
                        subjects=["water"])]
        write_source(self.source, revisions)
        write_taxonomy(self.taxonomy, scenario_taxonomy())
        build_warehouse(self.source, self.taxonomy, "2026-01-01",
                        "2026-01-07", self.warehouse)
        with connect(self.warehouse) as db:
            rows = db.execute(
                "SELECT datestamp, valid_from, valid_to FROM wh_versions "
                "WHERE identifier='oai:demo:C' ORDER BY datestamp").fetchall()
            self.assertEqual([tuple(r) for r in rows], [
                ("2026-01-02", "2026-01-02", "2026-01-04"),
                ("2026-01-04", "2026-01-04", "2026-01-07")])


# ---------------------------------------------------------------------------
# Acceptance 3: catalog-wide sparse metrics and unmapped/no-subject states
# ---------------------------------------------------------------------------

class CatalogMetricTests(WarehouseTestBase):
    def setUp(self):
        super().setUp()
        self.build()

    def test_daily_catalog_counts(self):
        expected = {
            "active": [1, 2, 1, 1, 2, 2],
            "deleted": [0, 0, 1, 1, 0, 0],
            "active_no_subject": [0, 0, 0, 0, 1, 1],
            "active_with_unmapped": [0, 1, 1, 1, 1, 1]}
        for day in daterange("2026-01-01", "2026-01-07"):
            result = as_of(self.warehouse, day)
            for metric, values in expected.items():
                index = (day - date(2026, 1, 1)).days
                self.assertEqual(
                    result["counts"][metric], values[index],
                    f"{metric} wrong on {day}")

    def test_metric_intervals_are_sparse_and_overlap_is_allowed(self):
        with connect(self.warehouse) as db:
            no_subject = [tuple(r) for r in db.execute(
                "SELECT valid_from, valid_to, record_count "
                "FROM wh_catalog_intervals WHERE metric='active_no_subject' "
                "ORDER BY valid_from")]
            unmapped = [tuple(r) for r in db.execute(
                "SELECT valid_from, valid_to, record_count "
                "FROM wh_catalog_intervals "
                "WHERE metric='active_with_unmapped' ORDER BY valid_from")]
        self.assertEqual(no_subject, [
            ("2026-01-01", "2026-01-05", 0),
            ("2026-01-05", "2026-01-07", 1)])
        self.assertEqual(unmapped, [
            ("2026-01-01", "2026-01-02", 0),
            ("2026-01-02", "2026-01-07", 1)])
        # On Jan5/Jan6 both flags are 1: overlapping, not mutually exclusive.
        result = as_of(self.warehouse, "2026-01-05")
        self.assertEqual(result["counts"]["active_no_subject"], 1)
        self.assertEqual(result["counts"]["active_with_unmapped"], 1)

    def test_jan4_b_is_active_but_unmapped_not_subjectless(self):
        result = as_of(self.warehouse, "2026-01-04")
        b = next(v for v in result["versions"]
                 if v["identifier"] == "oai:demo:B")
        self.assertFalse(b["deleted"])
        self.assertEqual(b["subjects"], ["mystery"])
        self.assertEqual(b["unmapped_subjects"], ["mystery"])
        self.assertEqual(b["direct_topics"], [])
        self.assertEqual(b["ancestor_topics"], [])
        self.assertEqual(result["counts"]["active_no_subject"], 0)
        self.assertEqual(result["counts"]["active_with_unmapped"], 1)

    def test_jan5_a_is_active_again_not_deleted(self):
        result = as_of(self.warehouse, "2026-01-05")
        a = next(v for v in result["versions"]
                 if v["identifier"] == "oai:demo:A")
        self.assertFalse(a["deleted"])
        self.assertEqual(a["subjects"], [])
        self.assertEqual(result["counts"]["deleted"], 0)
        self.assertEqual(result["counts"]["active"], 2)

    def test_no_subject_differs_from_only_unmapped(self):
        with connect(self.warehouse) as db:
            # A/Jan5: no subjects at all.
            a5 = db.execute(
                "SELECT has_subjects, has_unmapped FROM wh_versions "
                "WHERE identifier='oai:demo:A' AND datestamp='2026-01-05'"
            ).fetchone()
            self.assertEqual(tuple(a5), (0, 0))
            # B/Jan4: has an unmapped subject.
            b4 = db.execute(
                "SELECT has_subjects, has_unmapped FROM wh_versions "
                "WHERE identifier='oai:demo:B' AND datestamp='2026-01-04'"
            ).fetchone()
            self.assertEqual(tuple(b4), (1, 1))


# ---------------------------------------------------------------------------
# Acceptance 4: boundary, pre-start, leap day and empty-input semantics
# ---------------------------------------------------------------------------

class BoundaryAndEmptyTests(WarehouseTestBase):
    def test_pre_start_active_and_deleted_determine_start_state(self):
        revisions = [
            make_record("oai:demo:ACTIVE", "2025-12-20", title="Old active",
                        subjects=["water"]),
            make_record("oai:demo:DEAD", "2025-12-25", deleted=True),
            make_record("oai:demo:LATE", "2026-01-04", title="Appears later")]
        self.build(revisions=revisions)
        first = as_of(self.warehouse, "2026-01-01")
        by_id = {v["identifier"]: v for v in first["versions"]}
        self.assertEqual(set(by_id), {"oai:demo:ACTIVE", "oai:demo:DEAD"})
        self.assertFalse(by_id["oai:demo:ACTIVE"]["deleted"])
        self.assertTrue(by_id["oai:demo:DEAD"]["deleted"])
        # Unknown identifier is neither active nor counted as deleted.
        self.assertEqual(first["counts"]["active"], 1)
        self.assertEqual(first["counts"]["deleted"], 1)
        # No version interval may begin before the build start.
        with connect(self.warehouse) as db:
            self.assertFalse(db.execute(
                "SELECT 1 FROM wh_versions WHERE valid_from < '2026-01-01'"
            ).fetchall())
            # Out-of-range revisions are still retained at the source layer.
            stamps = [r[0] for r in db.execute(
                "SELECT datestamp FROM wh_source_revisions ORDER BY datestamp")]
            self.assertIn("2025-12-20", stamps)
            self.assertIn("2025-12-25", stamps)
        # Once LATE appears it is active; DEAD stays deleted.
        later = as_of(self.warehouse, "2026-01-04")
        self.assertEqual(later["counts"]["active"], 2)
        self.assertEqual(later["counts"]["deleted"], 1)

    def test_revision_exactly_at_end_is_excluded_but_retained_at_source(self):
        revisions = [
            make_record("oai:demo:A", "2026-01-01", title="A"),
            make_record("oai:demo:A", "2026-01-07", title="At end")]
        self.build(revisions=revisions)
        with connect(self.warehouse) as db:
            clipped = [tuple(r) for r in db.execute(
                "SELECT datestamp, valid_from, valid_to FROM wh_versions "
                "ORDER BY datestamp")]
            self.assertEqual(clipped, [
                ("2026-01-01", "2026-01-01", "2026-01-07")])
            self.assertEqual(db.execute(
                "SELECT count(*) FROM wh_source_revisions").fetchone()[0], 2)

    def test_revision_exactly_at_start_is_included(self):
        revisions = [make_record("oai:demo:A", "2026-01-01", title="Start")]
        self.build(revisions=revisions)
        result = as_of(self.warehouse, "2026-01-01")
        self.assertEqual(result["counts"]["active"], 1)

    def test_first_revision_later_than_start_is_unknown_beforehand(self):
        revisions = [make_record("oai:demo:A", "2026-01-03", title="Later")]
        self.build(revisions=revisions)
        self.assertEqual(as_of(self.warehouse, "2026-01-01")["counts"],
                         {"active": 0, "deleted": 0,
                          "active_no_subject": 0, "active_with_unmapped": 0})
        self.assertEqual(as_of(self.warehouse, "2026-01-03")["counts"]["active"],
                         1)

    def test_leap_day_is_handled(self):
        revisions = [
            make_record("oai:demo:A", "2028-02-28", title="Before leap",
                        subjects=["water"]),
            make_record("oai:demo:A", "2028-02-29", title="On leap day",
                        subjects=["water"])]
        write_source(self.source, revisions)
        write_taxonomy(self.taxonomy, scenario_taxonomy())
        build_warehouse(self.source, self.taxonomy, "2028-02-28",
                        "2028-03-02", self.warehouse)
        days = list(daterange("2028-02-28", "2028-03-02"))
        self.assertEqual([d.isoformat() for d in days],
                         ["2028-02-28", "2028-02-29", "2028-03-01"])
        self.assertEqual(as_of(self.warehouse, "2028-02-29")["versions"][0]
                         ["title"], "On leap day")
        with connect(self.warehouse) as db:
            days_count = db.execute(
                "SELECT record_days FROM wh_topic_record_days "
                "WHERE topic_id='leaf'").fetchone()[0]
            self.assertEqual(days_count, 3)

    def test_empty_formal_catalog(self):
        self.build(revisions=[])
        with connect(self.warehouse) as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM wh_versions").fetchone()[0], 0)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM wh_source_revisions").fetchone()[0], 0)
            # Every node still gets one full-range zero interval.
            coverage = [tuple(r) for r in db.execute(
                "SELECT topic_id, valid_from, valid_to, record_count "
                "FROM wh_topic_intervals ORDER BY topic_id")]
            self.assertTrue(coverage)
            for topic_id, seg_from, seg_to, count in coverage:
                self.assertEqual(
                    (seg_from, seg_to, count),
                    ("2026-01-01", "2026-01-07", 0))
        result = as_of(self.warehouse, "2026-01-02")
        self.assertEqual(result["versions"], [])
        self.assertEqual(result["counts"], {
            "active": 0, "deleted": 0, "active_no_subject": 0,
            "active_with_unmapped": 0})

    def test_empty_taxonomy_leaves_every_subject_unmapped(self):
        empty = {"schema_version": 1, "nodes": [], "edges": [], "bindings": []}
        self.build(taxonomy=empty)
        with connect(self.warehouse) as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM wh_topic_closure").fetchone()[0], 0)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM wh_topic_intervals").fetchone()[0], 0)
        result = as_of(self.warehouse, "2026-01-01")
        a = next(v for v in result["versions"] if v["identifier"]
                 == "oai:demo:A")
        self.assertEqual(a["unmapped_subjects"], ["hydro", "water"])
        self.assertEqual(a["direct_topics"], [])
        self.assertEqual(result["counts"]["active_with_unmapped"], 1)

    def test_long_range_sparse_output_does_not_expand_by_day(self):
        revisions = [
            make_record("oai:demo:A", "2026-01-01", title="A",
                        subjects=["water"]),
            make_record("oai:demo:A", "2026-02-15", deleted=True),
            make_record("oai:demo:A", "2027-06-01", title="A back",
                        subjects=["water"])]
        write_source(self.source, revisions)
        write_taxonomy(self.taxonomy, scenario_taxonomy())
        build_warehouse(self.source, self.taxonomy, "2026-01-01",
                        "2028-01-01", self.warehouse)
        with connect(self.warehouse) as db:
            interval_rows = db.execute(
                "SELECT count(*) FROM wh_topic_intervals").fetchone()[0]
            version_rows = db.execute(
                "SELECT count(*) FROM wh_versions").fetchone()[0]
        # Bounded by the few revision boundaries, never by the ~730 day span.
        self.assertEqual(version_rows, 3)
        self.assertLessEqual(interval_rows, 5 * 4)
        result = as_of(self.warehouse, "2027-06-01")
        self.assertEqual(result["counts"]["active"], 1)


# ---------------------------------------------------------------------------
# Acceptance 5: sync staging isolation and hard source/taxonomy rejections
# ---------------------------------------------------------------------------

class StagingAndRejectionTests(WarehouseTestBase):
    def test_sync_staging_is_never_exported_and_source_unchanged(self):
        self.build(sync_staging=True)
        before = self.source.read_bytes()
        facts = read_formal_facts(self.source)
        identifiers = {r["identifier"] for r in facts["revisions"]}
        self.assertNotIn("oai:demo:GHOST", identifiers)
        self.assertEqual(len(facts["revisions"]), 5)
        with connect(self.warehouse) as db:
            warehouse_tables = {
                r[0] for r in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue(
                all(not name.startswith("sync_") for name in warehouse_tables))
            self.assertFalse(db.execute(
                "SELECT 1 FROM wh_source_revisions "
                "WHERE identifier='oai:demo:GHOST'").fetchall())
        # The build must not mutate the source at all.
        self.assertEqual(self.source.read_bytes(), before)

    def test_staging_changes_leave_logical_digest_unchanged(self):
        write_source(self.source, scenario_revisions(), sync_staging=True)
        write_taxonomy(self.taxonomy, scenario_taxonomy())
        digest_a = source_facts_digest(read_formal_facts(self.source))
        # Add another staging-only page/revision; formal facts stay identical.
        with sqlite3.connect(self.source) as db:
            db.execute(
                "INSERT INTO sync_stage_pages VALUES (?,?,?,?,?,?,?,?)",
                (1, 1, 99, hashlib.sha256(b"<also-broken>").hexdigest(),
                 b"<also-broken>", 1, "resume", None))
        digest_b = source_facts_digest(read_formal_facts(self.source))
        self.assertEqual(digest_a, digest_b)

    def _build_good_target_first(self):
        self.build()
        return self.warehouse.read_bytes()

    def test_corrupted_formal_page_digest_rejected_and_target_kept(self):
        original = self._build_good_target_first()
        write_source(self.source, scenario_revisions())
        with sqlite3.connect(self.source) as db:
            digest = db.execute("SELECT sha256 FROM pages LIMIT 1").fetchone()[0]
            db.execute("UPDATE pages SET xml_body=? WHERE sha256=?",
                       (b"<?xml version='1.0'?><tampered/>", digest))
        with self.assertRaises(WarehouseError):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)
        self.assertEqual(self.warehouse.read_bytes(), original)

    def test_wrong_page_attribution_rejected(self):
        original = self._build_good_target_first()
        # Build a source whose revision points at a page not carrying it.
        self.source.unlink()
        other_page = page_xml([make_record("oai:demo:OTHER", "2026-01-01",
                                           title="Other")])
        other_digest = hashlib.sha256(other_page).hexdigest()
        rec = make_record("oai:demo:A", "2026-01-01", title="A",
                          subjects=["water"])
        raw = page_xml([rec])
        with sqlite3.connect(self.source) as db:
            db.executescript(FULL_DDL)
            db.execute("INSERT OR IGNORE INTO pages VALUES (?,?)",
                       (hashlib.sha256(raw).hexdigest(), raw))
            db.execute("INSERT OR IGNORE INTO pages VALUES (?,?)",
                       (other_digest, other_page))
            db.execute(
                "INSERT INTO revisions VALUES (?,?,?,?,?,?,?)",
                (rec["identifier"], rec["datestamp"], 0, rec["title"],
                 json.dumps(rec["subjects"]), canonical(rec), other_digest))
        with self.assertRaisesRegex(WarehouseError, "not present on cited page"):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)
        self.assertEqual(self.warehouse.read_bytes(), original)

    def test_canonical_inconsistency_rejected(self):
        original = self._build_good_target_first()
        write_source(self.source, scenario_revisions())
        with sqlite3.connect(self.source) as db:
            db.execute(
                "UPDATE revisions SET canonical_json=? WHERE identifier=? "
                "AND datestamp=?",
                ('{"different":true}', "oai:demo:A", "2026-01-01"))
        with self.assertRaisesRegex(WarehouseError, "canonical_json"):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)
        self.assertEqual(self.warehouse.read_bytes(), original)

    def test_unknown_binding_reference_rejected(self):
        original = self._build_good_target_first()
        bad = scenario_taxonomy()
        bad["bindings"].append({"subject": "water", "topic_id": "ghost"})
        write_taxonomy(self.taxonomy, bad)
        with self.assertRaisesRegex(WarehouseError, "unknown topic"):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)
        self.assertEqual(self.warehouse.read_bytes(), original)

    def test_unrelated_component_cycle_rejected(self):
        original = self._build_good_target_first()
        bad = scenario_taxonomy()
        bad["nodes"].extend([
            {"topic_id": "x1", "label": "X1"},
            {"topic_id": "x2", "label": "X2"}])
        bad["edges"].extend([
            {"child": "x1", "parent": "x2"},
            {"child": "x2", "parent": "x1"}])
        write_taxonomy(self.taxonomy, bad)
        with self.assertRaisesRegex(WarehouseError, "cycle"):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)
        self.assertEqual(self.warehouse.read_bytes(), original)

    def test_duplicate_binding_rejected_even_when_identical(self):
        original = self._build_good_target_first()
        bad = scenario_taxonomy()
        bad["bindings"].append({"subject": "water", "topic_id": "leaf"})
        write_taxonomy(self.taxonomy, bad)
        with self.assertRaisesRegex(WarehouseError, "bound more than once"):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)
        self.assertEqual(self.warehouse.read_bytes(), original)

    def test_missing_source_is_not_created(self):
        missing = self.dir / "absent.sqlite"
        with self.assertRaises(WarehouseError):
            build_warehouse(missing, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)
        self.assertFalse(missing.exists())
        self.assertFalse(self.warehouse.exists())

    def test_output_alias_of_source_rejected(self):
        write_source(self.source, scenario_revisions())
        write_taxonomy(self.taxonomy, scenario_taxonomy())
        link = self.dir / "link-to-source.sqlite"
        os.symlink(self.source, link)
        with self.assertRaises(WarehouseError):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", link)


# ---------------------------------------------------------------------------
# Acceptance 6: independent day-by-day oracle, no production aggregation calls
# ---------------------------------------------------------------------------

class Oracle:
    """Standalone reference; uses only taxonomy docs and raw revisions."""

    def __init__(self, revisions, taxonomy, start, end):
        self.revisions = revisions
        self.taxonomy = taxonomy
        self.start = date.fromisoformat(start)
        self.end = date.fromisoformat(end)
        self.nodes = {n["topic_id"]: n["label"] for n in taxonomy["nodes"]}
        self.parent_map = defaultdict(list)
        for edge in taxonomy["edges"]:
            self.parent_map[edge["child"]].append(edge["parent"])
        self.binding = {b["subject"]: b["topic_id"]
                        for b in taxonomy["bindings"]}

    def effective(self, day):
        latest = {}
        for rec in self.revisions:
            stamp = date.fromisoformat(rec["datestamp"])
            if stamp <= day:
                if (rec["identifier"] not in latest
                        or stamp > date.fromisoformat(
                            latest[rec["identifier"]]["datestamp"])):
                    latest[rec["identifier"]] = rec
        return latest

    def ancestors_of(self, topic):
        # Explicit set walk; does not call the production closure builder.
        seen = {topic}
        queue = [topic]
        while queue:
            current = queue.pop(0)
            for parent in self.parent_map.get(current, ()):
                if parent not in seen:
                    seen.add(parent)
                    queue.append(parent)
        return seen

    def day_counts(self, day):
        effective = self.effective(day)
        topic_counts = {topic: 0 for topic in self.nodes}
        active = deleted = no_subject = with_unmapped = 0
        for rec in effective.values():
            if rec["deleted"]:
                deleted += 1
                continue
            active += 1
            subjects = rec["subjects"]
            if not subjects:
                no_subject += 1
            if any(s not in self.binding for s in subjects):
                with_unmapped += 1
            topics = {self.binding[s] for s in subjects if s in self.binding}
            ancestors = set()
            for topic in topics:
                ancestors |= self.ancestors_of(topic)
            for ancestor in ancestors:
                topic_counts[ancestor] += 1
        return {
            "active": active, "deleted": deleted,
            "active_no_subject": no_subject,
            "active_with_unmapped": with_unmapped}, topic_counts, effective


class WarehouseOracleTests(WarehouseTestBase):
    def _oracle_scenario(self, seed):
        taxonomy = {
            "schema_version": 1,
            "nodes": [
                {"topic_id": "n0", "label": "N0"},
                {"topic_id": "n1", "label": "N1"},
                {"topic_id": "n2", "label": "N2"},
                {"topic_id": "n3", "label": "N3"},
                {"topic_id": "n4", "label": "N4"}],
            "edges": [
                {"child": "n0", "parent": "n1"},
                {"child": "n0", "parent": "n2"},
                {"child": "n1", "parent": "n3"},
                {"child": "n2", "parent": "n3"},
                {"child": "n3", "parent": "n4"}],
            "bindings": [
                {"subject": "s0", "topic_id": "n0"},
                {"subject": "s1", "topic_id": "n1"},
                {"subject": "s2", "topic_id": "n2"}]}
        subjects_pool = ["s0", "s1", "s2", "unmapped-a", "unmapped-b"]
        revisions = []
        stamps = ["2026-01-01", "2026-01-03", "2026-01-04", "2026-01-06",
                  "2026-01-08"]
        for index in range(seed):
            identifier = f"oai:demo:R{index}"
            for step, stamp in enumerate(stamps):
                # Deterministic but varied timeline: some deletions, some
                # subjectless re-appearances, otherwise rotating subjects.
                phase = (index * 2 + step) % 5
                if phase == 0:
                    revisions.append(make_record(identifier, stamp, deleted=True))
                elif phase == 3:
                    revisions.append(make_record(
                        identifier, stamp,
                        title=f"Title {index}-{step}"))
                else:
                    chosen = [subjects_pool[(index + step + k) % 5]
                              for k in range(1 + (index + step) % 3)]
                    revisions.append(make_record(
                        identifier, stamp,
                        title=f"Title {index}-{step}", subjects=chosen))
        # Keep a deterministic ordering for the source writer.
        revisions.sort(key=lambda r: (r["identifier"], r["datestamp"]))
        write_source(self.source, revisions)
        write_taxonomy(self.taxonomy, taxonomy)
        build_warehouse(self.source, self.taxonomy, "2026-01-01",
                        "2026-01-10", self.warehouse)
        return Oracle(revisions, taxonomy, "2026-01-01", "2026-01-10")

    def test_oracle_matches_reader_and_interval_tables_every_day(self):
        for seed in (1, 2, 5, 8):
            with self.subTest(seed=seed):
                oracle = self._oracle_scenario(seed)
                with connect(self.warehouse) as db:
                    for day in daterange("2026-01-01", "2026-01-10"):
                        text = day.isoformat()
                        counts, topic_counts, effective = oracle.day_counts(day)
                        result = as_of(self.warehouse, text)
                        self.assertEqual(result["counts"], counts, text)
                        self.assertEqual(
                            result["topic_record_counts"], topic_counts, text)
                        self.assertEqual(
                            result["identifiers"], len(effective), text)
                        # Re-derive every count from the sparse interval tables
                        # to prove the tables are consistent with the reader.
                        for metric, expected in counts.items():
                            row = db.execute(
                                "SELECT record_count FROM wh_catalog_intervals "
                                "WHERE metric=? AND valid_from<=? AND ?<valid_to",
                                (metric, text, text)).fetchone()
                            self.assertEqual(row[0], expected, (metric, text))
                        for topic, expected in topic_counts.items():
                            row = db.execute(
                                "SELECT record_count FROM wh_topic_intervals "
                                "WHERE topic_id=? AND valid_from<=? AND ?<valid_to",
                                (topic, text, text)).fetchone()
                            self.assertEqual(row[0], expected, (topic, text))

    def test_record_days_recomputes_from_intervals(self):
        oracle = self._oracle_scenario(6)
        expected_days = defaultdict(int)
        for day in daterange("2026-01-01", "2026-01-10"):
            _counts, topic_counts, _eff = oracle.day_counts(day)
            for topic, count in topic_counts.items():
                expected_days[topic] += count
        with connect(self.warehouse) as db:
            stored = dict(db.execute(
                "SELECT topic_id, record_days FROM wh_topic_record_days")
                .fetchall())
        self.assertEqual(stored, dict(expected_days))


# ---------------------------------------------------------------------------
# Acceptance 7: real CLI end-to-end, self-containment, failure/rebuild
# ---------------------------------------------------------------------------

def run_wh_cli(*args, env=None, expect_code=0):
    clean_env = {key: value for key, value in os.environ.items()
                 if key.upper() != "PYTHONPATH"}
    clean_env["PYTHONPATH"] = str(ROOT)
    if env:
        clean_env.update(env)
    proc = subprocess.run(
        [sys.executable, "-E", "-m", "catalog_harvest.warehouse", *map(str, args)],
        cwd=ROOT, capture_output=True, text=True, env=clean_env)
    if proc.returncode != expect_code:
        raise AssertionError(
            f"warehouse CLI exited {proc.returncode}, expected {expect_code}\n"
            f"stdout={proc.stdout}\nstderr={proc.stderr}")
    return proc


class CliEndToEndTests(WarehouseTestBase):
    def test_real_cli_build_and_self_contained_as_of(self):
        with fixture_server(DATA) as endpoint:
            harvest(page_fetcher(endpoint), self.source)
        taxonomy_doc = {
            "schema_version": 1,
            "nodes": [{"topic_id": "hydro", "label": "Hydrology"}],
            "edges": [],
            "bindings": [{"subject": "hydrology", "topic_id": "hydro"}]}
        write_taxonomy(self.taxonomy, taxonomy_doc)
        proc = run_wh_cli(
            "build", "--source-db", self.source, "--taxonomy", self.taxonomy,
            "--start", "2026-01-01", "--end", "2026-01-10",
            "--database", self.warehouse)
        summary = json.loads(proc.stdout)
        self.assertEqual(summary["source_revisions"], 3)

        # Query the product directly in SQL, like an external analyst would.
        with connect(self.warehouse) as db:
            versions = db.execute(
                "SELECT identifier, datestamp, valid_from, valid_to, deleted "
                "FROM wh_versions ORDER BY identifier, datestamp").fetchall()
            self.assertTrue(versions)
            closure = db.execute(
                "SELECT descendant, ancestor, distance FROM wh_topic_closure"
            ).fetchall()
            self.assertEqual([tuple(r) for r in closure],
                             [("hydro", "hydro", 0)])
            self.assertTrue(db.execute(
                "SELECT 1 FROM wh_taxonomy_provenance").fetchall())
            self.assertTrue(db.execute(
                "SELECT 1 FROM wh_catalog_intervals").fetchall())

        # Move source and taxonomy away: the product stays fully usable.
        hide = self.dir / "hidden"
        hide.mkdir()
        moved_source = hide / "source.sqlite"
        moved_tax = hide / "taxonomy.json"
        shutil.move(self.source, moved_source)
        shutil.move(self.taxonomy, moved_tax)
        proc = run_wh_cli(
            "as-of", "--database", self.warehouse, "--date", "2026-01-02")
        result = json.loads(proc.stdout)
        self.assertEqual(result["date"], "2026-01-02")
        self.assertEqual(result["identifiers"], 2)
        self.assertTrue(result["versions"])

    def test_write_and_replace_failures_keep_old_target_and_clean_temp(self):
        self.build()
        original = self.warehouse.read_bytes()
        temp_files_before = {p.name for p in self.dir.glob(".whbuild-*")}
        # Write failure happens before replacement.
        proc = run_wh_cli(
            "build", "--source-db", self.source, "--taxonomy", self.taxonomy,
            "--start", "2026-01-01", "--end", "2026-01-07",
            "--database", self.warehouse,
            env={ENV_WRITE_FAIL: "1"}, expect_code=2)
        self.assertIn("injected write failure", proc.stderr)
        self.assertEqual(self.warehouse.read_bytes(), original)
        # Replace failure happens after a complete validated temp database.
        proc = run_wh_cli(
            "build", "--source-db", self.source, "--taxonomy", self.taxonomy,
            "--start", "2026-01-01", "--end", "2026-01-07",
            "--database", self.warehouse,
            env={ENV_REPLACE_FAIL: "1"}, expect_code=2)
        self.assertIn("injected replace failure", proc.stderr)
        self.assertEqual(self.warehouse.read_bytes(), original)
        temp_files_after = {p.name for p in self.dir.glob(".whbuild-*")}
        self.assertEqual(temp_files_after, temp_files_before)
        # Corrected rerun succeeds without manual cleanup.
        run_wh_cli(
            "build", "--source-db", self.source, "--taxonomy", self.taxonomy,
            "--start", "2026-01-01", "--end", "2026-01-07",
            "--database", self.warehouse)
        self.assertTrue(self.warehouse.read_bytes())
        as_of(self.warehouse, "2026-01-02")

    def test_deterministic_summary_and_business_tables(self):
        self.build()
        first = build_warehouse(self.source, self.taxonomy, "2026-01-01",
                                "2026-01-07", self.warehouse)
        second = build_warehouse(self.source, self.taxonomy, "2026-01-01",
                                 "2026-01-07", self.warehouse)
        self.assertEqual(first, second)

    def test_taxonomy_array_reorder_changes_digest_not_business_sets(self):
        self.build()
        before_digest = self.taxonomy.read_bytes()
        with connect(self.warehouse) as db:
            closure_before = db.execute(
                "SELECT descendant, ancestor, distance FROM wh_topic_closure "
                "ORDER BY descendant, ancestor").fetchall()
            topics_before = db.execute(
                "SELECT * FROM wh_topic_intervals ORDER BY topic_id, valid_from"
            ).fetchall()
            catalog_before = db.execute(
                "SELECT * FROM wh_catalog_intervals ORDER BY metric, valid_from"
            ).fetchall()
        reordered = scenario_taxonomy()
        reordered["nodes"] = list(reversed(reordered["nodes"]))
        reordered["edges"] = list(reversed(reordered["edges"]))
        reordered["bindings"] = list(reversed(reordered["bindings"]))
        write_taxonomy(self.taxonomy, reordered)
        build_warehouse(self.source, self.taxonomy, "2026-01-01",
                        "2026-01-07", self.warehouse)
        with connect(self.warehouse) as db:
            closure_after = db.execute(
                "SELECT descendant, ancestor, distance FROM wh_topic_closure "
                "ORDER BY descendant, ancestor").fetchall()
            topics_after = db.execute(
                "SELECT * FROM wh_topic_intervals ORDER BY topic_id, valid_from"
            ).fetchall()
            catalog_after = db.execute(
                "SELECT * FROM wh_catalog_intervals ORDER BY metric, valid_from"
            ).fetchall()
            new_digest = db.execute(
                "SELECT value FROM wh_meta WHERE key='taxonomy_sha256'"
            ).fetchone()[0]
        self.assertEqual(closure_before, closure_after)
        self.assertEqual([tuple(r) for r in topics_before],
                         [tuple(r) for r in topics_after])
        self.assertEqual([tuple(r) for r in catalog_before],
                         [tuple(r) for r in catalog_after])
        self.assertEqual(
            new_digest, hashlib.sha256(self.taxonomy.read_bytes()).hexdigest())
        self.assertNotEqual(hashlib.sha256(before_digest).hexdigest(),
                            new_digest)


# ---------------------------------------------------------------------------
# Strict taxonomy validation and reader boundaries
# ---------------------------------------------------------------------------

class TaxonomyValidationTests(WarehouseTestBase):
    def _reject(self, document, needle):
        write_source(self.source, scenario_revisions())
        write_taxonomy(self.taxonomy, document)
        with self.assertRaisesRegex(WarehouseError, needle):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)

    def test_schema_version_must_be_integer_one(self):
        doc = scenario_taxonomy()
        doc["schema_version"] = True
        self._reject(doc, "integer 1")
        doc["schema_version"] = "1"
        self._reject(doc, "integer 1")
        doc["schema_version"] = 2
        self._reject(doc, "integer 1")

    def test_extract_fields_and_wrong_types_rejected(self):
        doc = scenario_taxonomy()
        doc["nodes"][0]["extra"] = 1
        self._reject(doc, "exactly")
        doc = scenario_taxonomy()
        doc["extra"] = 1
        self._reject(doc, "exactly")
        doc = scenario_taxonomy()
        doc["nodes"] = {"topic_id": "root", "label": "Root"}
        self._reject(doc, "arrays")
        doc = scenario_taxonomy()
        doc["nodes"][0]["label"] = "  spaced  "
        self._reject(doc, "label")

    def test_duplicate_edge_self_edge_and_unknown_node_rejected(self):
        doc = scenario_taxonomy()
        doc["edges"].append({"child": "leaf", "parent": "left"})
        self._reject(doc, "duplicate edge")
        doc = scenario_taxonomy()
        doc["edges"].append({"child": "root", "parent": "root"})
        self._reject(doc, "self edge")
        doc = scenario_taxonomy()
        doc["edges"].append({"child": "leaf", "parent": "ghost"})
        self._reject(doc, "unknown node")

    def test_binding_subject_must_be_normalized_and_distinct(self):
        doc = scenario_taxonomy()
        doc["bindings"][0]["subject"] = " hydro "
        self._reject(doc, "whitespace-normalized")
        doc = scenario_taxonomy()
        doc["bindings"] = [
            {"subject": "water", "topic_id": "leaf"},
            {"subject": "water", "topic_id": "root"}]
        self._reject(doc, "bound more than once")

    def test_empty_document_rejected(self):
        self.taxonomy.write_text("", encoding="utf-8")
        write_source(self.source, scenario_revisions())
        with self.assertRaises(WarehouseError):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-07", self.warehouse)


class AsOfBoundaryTests(WarehouseTestBase):
    def test_as_of_rejects_dates_outside_build_window(self):
        self.build()
        for bad in ("2025-12-31", "2026-01-07", "2026-02-01"):
            with self.assertRaisesRegex(WarehouseError, "must satisfy"):
                as_of(self.warehouse, bad)
        # Endpoints: start accepted, end excluded.
        self.assertTrue(as_of(self.warehouse, "2026-01-01"))
        with self.assertRaises(WarehouseError):
            as_of(self.warehouse, "2026-01-07")

    def test_non_warehouse_database_rejected(self):
        write_source(self.source, scenario_revisions())
        with self.assertRaisesRegex(WarehouseError, "not a catalog_harvest"):
            as_of(self.source, "2026-01-02")

    def test_invalid_date_range_rejected(self):
        write_source(self.source, scenario_revisions())
        write_taxonomy(self.taxonomy, scenario_taxonomy())
        with self.assertRaises(WarehouseError):
            build_warehouse(self.source, self.taxonomy, "2026-01-07",
                            "2026-01-01", self.warehouse)
        with self.assertRaises(WarehouseError):
            build_warehouse(self.source, self.taxonomy, "2026-01-01",
                            "2026-01-01", self.warehouse)


if __name__ == "__main__":
    unittest.main()
