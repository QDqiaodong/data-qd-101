"""Independent tests for the cross-topic co-occurrence affinity analysis.

The numeric oracles here deliberately re-derive every statistic without
calling any production statistics routine:

* :func:`ref_fisher_tail` enumerates every feasible fixed-margin 2x2 table
  with :mod:`math.comb` and :class:`fractions.Fraction` and sums the exact
  hypergeometric tail itself;
* :func:`ref_bh` re-implements the Benjamini--Hochberg reverse cumulative
  minimum from an explicit hand-built p sequence;
* :func:`recompute_from_evidence` rebuilds every contingency table, metric,
  tail, adjustment and selection from the per-record membership sets carried
  in the report's own evidence section.

Controlled scenarios are collected through the *real* loopback OAI-PMH
harvester (not hand-built analysis objects), built with the real warehouse
entry point and only then analyzed. All data is original synthetic content.
"""
import hashlib
import json
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
from fractions import Fraction
from itertools import combinations
from pathlib import Path
from xml.sax.saxutils import escape

from catalog_harvest.demo import fixture_server
from catalog_harvest.harvest import harvest
from catalog_harvest.transport import page_fetcher
from catalog_harvest.warehouse import build_warehouse
from catalog_harvest import affinity
from catalog_harvest.affinity import (
    AffinityError, analyze, publish_report, parse_plan,
    fisher_one_sided_tail, benjamini_hochberg, fraction_text,
    serialize_report, metadata_digest, relations_digest, evidence_digest,
    ENV_SNAPSHOT_HOOK, ENV_WRITE_FAIL, ENV_REPLACE_FAIL)

ROOT = Path(__file__).resolve().parents[1]

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DCO_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"


# ---------------------------------------------------------------------------
# Original synthetic fixtures and the real harvest / warehouse entry points
# ---------------------------------------------------------------------------

def make_record(identifier, stamp, deleted=False, title=None, subjects=()):
    return {"identifier": identifier, "datestamp": stamp, "deleted": deleted,
            "title": title, "subjects": sorted(subjects)}


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
    # Single page, no resumption token -> the harvester stops after one GET.
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<OAI-PMH xmlns="{OAI_NS}" xmlns:oai_dc="{DCO_NS}" '
        f'xmlns:dc="{DC_NS}"><ListRecords>'
        f'{"".join(record_xml(r) for r in records)}'
        "</ListRecords></OAI-PMH>").encode("utf-8")


def harvest_records(source, records):
    """Collect ``records`` through the real loopback HTTP harvester.

    Any earlier source file is removed first so each scenario starts from a
    clean harvest rather than accumulating revisions across re-builds.
    """
    source = Path(source)
    for stale in (source, Path(str(source) + "-wal"),
                  Path(str(source) + "-shm")):
        if stale.exists():
            stale.unlink()
    with tempfile.TemporaryDirectory() as fixture_dir:
        directory = Path(fixture_dir)
        (directory / "page1.xml").write_bytes(page_xml(records))
        with fixture_server(directory) as endpoint:
            return harvest(page_fetcher(endpoint), source)


def write_taxonomy(path, document):
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2),
                    encoding="utf-8")


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    return db


def flat_taxonomy():
    """Four mutually non-ancestral nodes A, B, C, U (U unused)."""
    return {
        "schema_version": 1,
        "nodes": [
            {"topic_id": "A", "label": "Topic A"},
            {"topic_id": "B", "label": "Topic B"},
            {"topic_id": "C", "label": "Topic C"},
            {"topic_id": "U", "label": "Unused node"}],
        "edges": [],
        "bindings": [
            {"subject": "a-topic", "topic_id": "A"},
            {"subject": "b-topic", "topic_id": "B"},
            {"subject": "c-topic", "topic_id": "C"}],
    }


def relation_taxonomy():
    """Diamond graph root -> {left,right} -> leaf plus an independent solo."""
    return {
        "schema_version": 1,
        "nodes": [
            {"topic_id": "leaf", "label": "Leaf"},
            {"topic_id": "left", "label": "Left"},
            {"topic_id": "right", "label": "Right"},
            {"topic_id": "root", "label": "Root"},
            {"topic_id": "solo", "label": "Solo"}],
        "edges": [
            {"child": "leaf", "parent": "left"},
            {"child": "leaf", "parent": "right"},
            {"child": "left", "parent": "root"},
            {"child": "right", "parent": "root"}],
        "bindings": [
            {"subject": "hydro", "topic_id": "leaf"},
            {"subject": "water", "topic_id": "leaf"},
            {"subject": "leftness", "topic_id": "left"},
            {"subject": "rightness", "topic_id": "right"}],
    }


def six_records():
    """1/2/3 carry A+B; 4/5/6 carry C only."""
    return [
        make_record("oai:demo:1", "2026-01-01", title="One",
                    subjects=["a-topic", "b-topic"]),
        make_record("oai:demo:2", "2026-01-02", title="Two",
                    subjects=["a-topic", "b-topic"]),
        make_record("oai:demo:3", "2026-01-03", title="Three",
                    subjects=["a-topic", "b-topic"]),
        make_record("oai:demo:4", "2026-01-04", title="Four",
                    subjects=["c-topic"]),
        make_record("oai:demo:5", "2026-01-05", title="Five",
                    subjects=["c-topic"]),
        make_record("oai:demo:6", "2026-01-06", title="Six",
                    subjects=["c-topic"]),
    ]


def write_plan(path, topics, alpha="0.3", min_shared=3,
               schema_version=1):
    path.write_text(json.dumps({
        "schema_version": schema_version, "topics": topics,
        "alpha": alpha, "min_shared": min_shared}), encoding="utf-8")
    return path


class AffinityTestBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.source = self.dir / "source.sqlite"
        self.taxonomy = self.dir / "taxonomy.json"
        self.warehouse = self.dir / "warehouse.sqlite"
        self.plan = self.dir / "plan.json"
        self.output = self.dir / "report.json"

    def build_warehouse(self, records, taxonomy=None,
                        start="2026-01-01", end="2026-01-10"):
        harvest_records(self.source, records)
        write_taxonomy(
            self.taxonomy,
            taxonomy if taxonomy is not None else flat_taxonomy())
        build_warehouse(self.source, self.taxonomy, start, end,
                        self.warehouse)

    def run_analysis(self, day="2026-01-06", topics=None, alpha="0.3",
                     min_shared=3, hook=None):
        write_plan(self.plan,
                   topics if topics is not None else ["A", "B", "C", "U"],
                   alpha=alpha, min_shared=min_shared)
        return analyze(self.warehouse, self.plan, day, snapshot_hook=hook)

    def find_test(self, report, pair):
        return next(t for t in report["tests"] if t["pair"] == list(pair))


# ---------------------------------------------------------------------------
# Independent numeric oracles (no production statistics calls)
# ---------------------------------------------------------------------------

def ref_fisher_tail(n, n_a, n_b, a):
    """Enumerate every feasible fixed-margin table and sum the exact tail."""
    lower = max(0, n_a + n_b - n)
    upper = min(n_a, n_b)
    denominator = math.comb(n, n_b)
    total = Fraction(0)
    for k in range(lower, upper + 1):
        probability = Fraction(
            math.comb(n_a, k) * math.comb(n - n_a, n_b - k), denominator)
        if k >= a:
            total += probability
    return total


def ref_bh(pairs_p):
    """Reference BH adjustment with explicit reverse cumulative minimum."""
    m = len(pairs_p)
    order = sorted(pairs_p, key=lambda item: (item[1], item[0]))
    raw = []
    for rank, (pair, p_value) in enumerate(order, start=1):
        raw.append((pair, rank, p_value,
                    min(Fraction(1), Fraction(m) * p_value / rank)))
    adjusted = {}
    running = Fraction(1)
    for index in range(m - 1, -1, -1):
        pair, rank, p_value, candidate = raw[index]
        running = min(running, candidate)
        adjusted[pair] = (rank, p_value, running)
    return adjusted


def recompute_from_evidence(report, alpha, min_shared):
    """Rebuild the entire analysis from the report's per-record evidence."""
    memberships = [
        set(rec["selected_topics"])
        for rec in report["record_evidence"] if not rec["deleted"]]
    n_population = len(memberships)
    topics = sorted(t["topic_id"] for t in report["plan"]["selected_topics"])
    excluded = {tuple(e["pair"]) for e in report["excluded_pairs"]}
    family = [tuple(sorted(pair)) for pair in combinations(topics, 2)
              if tuple(sorted(pair)) not in excluded]
    p_values = []
    rebuilt = {}
    for pair in family:
        first, second = pair
        a = b = c = d = 0
        for members in memberships:
            has_first = first in members
            has_second = second in members
            if has_first and has_second:
                a += 1
            elif has_first:
                b += 1
            elif has_second:
                c += 1
            else:
                d += 1
        n_a, n_b = a + b, a + c
        lift = (Fraction(a * n_population, n_a * n_b)
                if n_a and n_b else None)
        if n_population > 0:
            p_value = ref_fisher_tail(n_population, n_a, n_b, a)
            p_values.append((pair, p_value))
        else:
            p_value = None
        rebuilt[pair] = {
            "table": (a, b, c, d, n_population), "n_a": n_a, "n_b": n_b,
            "lift": lift, "p": p_value}
    adjusted = ref_bh(p_values) if n_population > 0 else {}
    for pair in family:
        if n_population > 0:
            _rank, _p, q_value = adjusted[pair]
            entry = rebuilt[pair]
            entry["q"] = q_value
            entry["selected"] = (
                entry["table"][0] >= min_shared
                and entry["lift"] is not None and entry["lift"] > 1
                and q_value <= alpha)
        else:
            entry["q"] = None
            entry["selected"] = False
    return n_population, family, rebuilt, adjusted


# ---------------------------------------------------------------------------
# Acceptance 1: headline six-record family, exact p/q/lift, alpha boundary
# ---------------------------------------------------------------------------

class HeadlineScenarioTests(AffinityTestBase):
    def setUp(self):
        super().setUp()
        self.build_warehouse(six_records())

    def test_six_record_family_and_exact_ab_statistics(self):
        report = self.run_analysis()
        self.assertEqual(report["population"], {
            "N": 6, "active_versions": 6, "known_deleted_versions": 0,
            "known_versions": 6})
        self.assertEqual(report["family"]["m"], 6)
        self.assertEqual(report["excluded_pairs"], [])
        self.assertEqual(report["diagnostics"], [])
        all_pairs = [tuple(sorted(p)) for p in
                     combinations(["A", "B", "C", "U"], 2)]
        self.assertEqual(
            sorted(tuple(t["pair"]) for t in report["tests"]),
            sorted(all_pairs))

        ab = self.find_test(report, ("A", "B"))
        self.assertEqual(ab["table"],
                         {"a": 3, "b": 0, "c": 0, "d": 3, "N": 6})
        self.assertEqual(ab["marginals"],
                         {"n_A": 3, "n_B": 3, "neither": 3})
        self.assertEqual(ab["metrics"]["a_over_n"], "1/2")
        self.assertEqual(ab["metrics"]["confidence_a_to_b"], "1/1")
        self.assertEqual(ab["metrics"]["confidence_b_to_a"], "1/1")
        self.assertEqual(ab["metrics"]["lift"], "2/1")
        self.assertEqual(ab["fisher"], {
            "rank": 1, "p": "1/20", "q": "3/10", "family_size": 6})
        self.assertTrue(ab["selection"]["selected"])

        # The other five pairs are fully tested and all land at p=q=1/1.
        for pair in (p for p in all_pairs if p != ("A", "B")):
            entry = self.find_test(report, pair)
            self.assertEqual(entry["fisher"]["p"], "1/1", pair)
            self.assertEqual(entry["fisher"]["q"], "1/1", pair)
            self.assertFalse(entry["selection"]["selected"], pair)
        self.assertEqual([c["pair"] for c in report["clues"]],
                         [["A", "B"]])
        # Ranks are a stable 1..6 permutation.
        self.assertEqual(
            sorted(t["fisher"]["rank"] for t in report["tests"]),
            [1, 2, 3, 4, 5, 6])

    def test_per_record_evidence_recomputes_the_tables(self):
        report = self.run_analysis()
        evidence = report["record_evidence"]
        self.assertEqual(len(evidence), 6)
        by_id = {row["identifier"]: row for row in evidence}
        for identifier in ("oai:demo:1", "oai:demo:2", "oai:demo:3"):
            self.assertEqual(by_id[identifier]["selected_topics"], ["A", "B"])
            self.assertFalse(by_id[identifier]["deleted"])
            self.assertTrue(by_id[identifier]["page_summary"]["page_sha256"])
        for identifier in ("oai:demo:4", "oai:demo:5", "oai:demo:6"):
            self.assertEqual(by_id[identifier]["selected_topics"], ["C"])
        # Independently rebuild AB straight from the evidence memberships.
        a = b = c = d = 0
        for row in evidence:
            hits = set(row["selected_topics"])
            has_a, has_b = "A" in hits, "B" in hits
            if has_a and has_b:
                a += 1
            elif has_a:
                b += 1
            elif has_b:
                c += 1
            else:
                d += 1
        self.assertEqual((a, b, c, d), (3, 0, 0, 3))

    def test_alpha_boundary_0_3_selects_but_0_299999_does_not(self):
        selected = self.run_analysis(alpha="0.3")
        self.assertTrue(self.find_test(selected, ("A", "B"))
                        ["selection"]["selected"])
        rejected = self.run_analysis(alpha="0.299999")
        ab = self.find_test(rejected, ("A", "B"))
        self.assertFalse(ab["selection"]["selected"])
        reason = ab["selection"]["reasons"]["q_le_alpha"]
        self.assertFalse(reason["passed"])
        self.assertEqual(reason["threshold_raw"], "0.299999")
        # Exact comparison, never a float rounding: 3/10 > 299999/1000000.
        self.assertTrue(Fraction("3/10") > Fraction("299999/1000000"))
        # Adjustment and family are alpha-independent.
        self.assertEqual(ab["fisher"]["p"], "1/20")
        self.assertEqual(ab["fisher"]["q"], "3/10")
        self.assertEqual(rejected["family"]["m"], 6)
        self.assertEqual(rejected["clues"], [])

    def test_larger_min_shared_changes_selection_not_p_q_or_family(self):
        report = self.run_analysis(min_shared=4)
        self.assertEqual(report["family"]["m"], 6)
        ab = self.find_test(report, ("A", "B"))
        self.assertEqual(ab["fisher"]["p"], "1/20")
        self.assertEqual(ab["fisher"]["q"], "3/10")
        self.assertFalse(ab["selection"]["selected"])
        self.assertFalse(
            ab["selection"]["reasons"]["a_ge_min_shared"]["passed"])
        self.assertTrue(ab["selection"]["reasons"]["lift_gt_1"]["passed"])
        self.assertTrue(ab["selection"]["reasons"]["q_le_alpha"]["passed"])
        self.assertEqual(report["clues"], [])


# ---------------------------------------------------------------------------
# Acceptance 2: denominator includes topicless/unmapped records; deletions
# ---------------------------------------------------------------------------

class DenominatorAndDeletionTests(AffinityTestBase):
    def _seven_records(self, seventh):
        records = six_records() + [seventh]
        self.build_warehouse(records)

    def test_topicless_record_enters_N_and_raises_d_to_four(self):
        self._seven_records(make_record(
            "oai:demo:7", "2026-01-07", title="No subjects at all"))
        report = self.run_analysis(day="2026-01-07")
        self.assertEqual(report["population"]["N"], 7)
        ab = self.find_test(report, ("A", "B"))
        self.assertEqual(ab["table"],
                         {"a": 3, "b": 0, "c": 0, "d": 4, "N": 7})
        self.assertEqual(ab["fisher"]["p"], "1/35")
        self.assertEqual(ab["fisher"]["q"], "6/35")
        seventh = next(r for r in report["record_evidence"]
                       if r["identifier"] == "oai:demo:7")
        self.assertEqual(seventh["selected_topics"], [])
        self.assertFalse(seventh["deleted"])

    def test_only_unmapped_subject_also_enters_N(self):
        self._seven_records(make_record(
            "oai:demo:7", "2026-01-07", title="Mystery",
            subjects=["unmapped-subject"]))
        report = self.run_analysis(day="2026-01-07")
        self.assertEqual(report["population"]["N"], 7)
        ab = self.find_test(report, ("A", "B"))
        self.assertEqual(ab["table"]["d"], 4)
        self.assertEqual(ab["fisher"]["p"], "1/35")
        seventh = next(r for r in report["record_evidence"]
                       if r["identifier"] == "oai:demo:7")
        self.assertEqual(seventh["selected_topics"], [])

    def test_deleted_version_is_evidence_but_never_in_N(self):
        records = six_records() + [
            make_record("oai:demo:8", "2026-01-01", title="Later deleted",
                        subjects=["a-topic"]),
            make_record("oai:demo:8", "2026-01-05", deleted=True)]
        self.build_warehouse(records)
        report = self.run_analysis(day="2026-01-06")
        self.assertEqual(report["population"]["N"], 6)
        self.assertEqual(report["population"]["known_deleted_versions"], 1)
        self.assertEqual(report["population"]["known_versions"], 7)
        tombstone = next(r for r in report["record_evidence"]
                         if r["identifier"] == "oai:demo:8")
        self.assertTrue(tombstone["deleted"])
        self.assertIsNone(tombstone["title"])
        self.assertEqual(tombstone["selected_topics"], [])
        # The active A/B counts are untouched by the deleted identifier.
        ab = self.find_test(report, ("A", "B"))
        self.assertEqual(ab["table"],
                         {"a": 3, "b": 0, "c": 0, "d": 3, "N": 6})

    def test_revision_switch_deletion_revival_and_end_exclusion(self):
        records = [
            make_record("oai:demo:X", "2026-01-02", title="X active A",
                        subjects=["a-topic"]),
            make_record("oai:demo:X", "2026-01-04", deleted=True),
            make_record("oai:demo:X", "2026-01-05", title="X revived B",
                        subjects=["b-topic"]),
            make_record("oai:demo:END", "2026-01-10", title="At end")]
        self.build_warehouse(records)
        # Before the first revision the identifier is simply unknown.
        before = self.run_analysis(day="2026-01-01", topics=["A", "B"])
        self.assertEqual(before["record_evidence"], [])
        # Active with A; interval carries to the deletion boundary.
        active_a = self.run_analysis(day="2026-01-03", topics=["A", "B"])
        row = active_a["record_evidence"][0]
        self.assertEqual(row["identifier"], "oai:demo:X")
        self.assertEqual(row["selected_topics"], ["A"])
        # Deletion day: tombstone, never a fallback to the old active version.
        deleted = self.run_analysis(day="2026-01-04", topics=["A", "B"])
        self.assertTrue(deleted["record_evidence"][0]["deleted"])
        self.assertEqual(deleted["population"]["N"], 0)
        # Revival uses the new B payload, not the historic A one.
        revived = self.run_analysis(day="2026-01-05", topics=["A", "B"])
        self.assertEqual(
            revived["record_evidence"][0]["selected_topics"], ["B"])
        # A revision dated exactly at the exclusive end is never effective.
        at_nine = self.run_analysis(day="2026-01-09", topics=["A", "B"])
        self.assertEqual(
            [r["identifier"] for r in at_nine["record_evidence"]],
            ["oai:demo:X"])
        with self.assertRaisesRegex(AffinityError, "must satisfy"):
            self.run_analysis(day="2026-01-10", topics=["A", "B"])


# ---------------------------------------------------------------------------
# Acceptance 3: de-duplication, ancestry exclusions, empty/zero diagnostics
# ---------------------------------------------------------------------------

class DeduplicationAndFamilyTests(AffinityTestBase):
    def setUp(self):
        super().setUp()
        self.build_warehouse([
            make_record("oai:demo:diamond", "2026-01-02",
                        title="Diamond record",
                        subjects=["hydro", "water", "leftness"])],
            taxonomy=relation_taxonomy())

    def test_synonyms_and_diamond_vote_once_per_topic(self):
        report = self.run_analysis(
            day="2026-01-02",
            topics=["leaf", "left", "right", "root", "solo"])
        row = report["record_evidence"][0]
        # hydro+water are synonyms bound to leaf; leaf reaches root through
        # two diamond paths. The de-duplicated selected set still has four.
        self.assertEqual(row["selected_topics"],
                         ["leaf", "left", "right", "root"])
        # solo is independent and unused: every pair involving it is tested.
        self.assertEqual(report["family"]["m"], 5)
        excluded = {tuple(e["pair"]) for e in report["excluded_pairs"]}
        self.assertEqual(excluded, {
            ("leaf", "left"), ("leaf", "right"), ("leaf", "root"),
            ("left", "root"), ("right", "root")})
        # left/right share descendant leaf but are mutually non-ancestral, so
        # they remain a testable pair.
        self.assertIn(
            ("left", "right"),
            [tuple(t["pair"]) for t in report["tests"]])
        left_right = self.find_test(report, ("left", "right"))
        self.assertEqual(left_right["table"]["a"], 1)

    def test_excluded_pair_carries_descendant_ancestor_and_distance(self):
        report = self.run_analysis(
            day="2026-01-02",
            topics=["leaf", "left", "right", "root", "solo"])
        by_pair = {tuple(e["pair"]): e for e in report["excluded_pairs"]}
        direct = by_pair[("leaf", "left")]
        self.assertEqual(direct["descendant"], "leaf")
        self.assertEqual(direct["ancestor"], "left")
        self.assertEqual(direct["shortest_distance"], 1)
        transitive = by_pair[("leaf", "root")]
        self.assertEqual(transitive["descendant"], "leaf")
        self.assertEqual(transitive["ancestor"], "root")
        # Shortest of the two diamond paths, never a duplicated row.
        self.assertEqual(transitive["shortest_distance"], 2)
        self.assertEqual(by_pair[("left", "root")]["shortest_distance"], 1)
        # Excluded pairs never appear in the testing family or its ranks.
        family_pairs = {tuple(t["pair"]) for t in report["tests"]}
        self.assertNotIn(("leaf", "root"), family_pairs)
        for test in report["tests"]:
            self.assertIsNotNone(test["fisher"]["rank"])

    def test_multiple_direct_topics_vote_once_each_in_tables(self):
        taxonomy = relation_taxonomy()
        records = [
            make_record("oai:demo:r1", "2026-01-02", title="R1",
                        subjects=["leftness", "rightness"]),
            make_record("oai:demo:r2", "2026-01-02", title="R2",
                        subjects=["leftness"])]
        self.build_warehouse(records, taxonomy=taxonomy)
        report = self.run_analysis(
            day="2026-01-02",
            topics=["leaf", "left", "right", "root", "solo"])
        # left/right are mutually non-ancestral; r1 hits both, r2 only left.
        left_right = self.find_test(report, ("left", "right"))
        self.assertEqual(left_right["table"],
                         {"a": 1, "b": 1, "c": 0, "d": 0, "N": 2})
        # Both records reach root through left, so the leaf/root-style
        # ancestor pair stays excluded and root counts both records once.
        evidence_by_id = {r["identifier"]: r["selected_topics"]
                          for r in report["record_evidence"]}
        self.assertEqual(evidence_by_id["oai:demo:r1"],
                         ["left", "right", "root"])
        self.assertEqual(evidence_by_id["oai:demo:r2"],
                         ["left", "root"])

    def test_all_ancestor_related_family_is_empty(self):
        report = self.run_analysis(
            day="2026-01-02", topics=["root", "left", "leaf"])
        self.assertEqual(report["family"]["m"], 0)
        self.assertIn("no_eligible_pairs", report["diagnostics"])
        self.assertEqual(report["tests"], [])
        self.assertEqual(report["clues"], [])
        self.assertEqual(len(report["excluded_pairs"]), 3)
        for excluded in report["excluded_pairs"]:
            self.assertEqual(excluded["status"], "ancestor_related")
            self.assertGreaterEqual(excluded["shortest_distance"], 1)
            self.assertEqual(excluded["evidence"], "wh_topic_closure")

    def test_zero_margin_pair_has_null_lift_but_unit_tail(self):
        self.build_warehouse(six_records())
        report = self.run_analysis()
        a_u = self.find_test(report, ("A", "U"))
        # U has a zero margin: lift/confidence are null, tail is exactly 1.
        self.assertIsNone(a_u["metrics"]["lift"])
        self.assertIsNone(a_u["metrics"]["confidence_b_to_a"])
        self.assertEqual(a_u["fisher"]["p"], "1/1")
        self.assertEqual(a_u["fisher"]["q"], "1/1")

    def test_zero_cooccurrence_pair_is_still_tested(self):
        self.build_warehouse(six_records())
        report = self.run_analysis()
        a_c = self.find_test(report, ("A", "C"))
        self.assertEqual(a_c["table"]["a"], 0)
        self.assertEqual(a_c["metrics"]["lift"], "0/1")
        self.assertEqual(a_c["fisher"]["p"], "1/1")
        self.assertFalse(a_c["selection"]["selected"])

    def test_empty_active_population_keeps_full_family_and_null_stats(self):
        # The only identifier is deleted on the analysis day.
        self.build_warehouse([
            make_record("oai:demo:gone", "2026-01-01", title="Gone",
                        subjects=["a-topic"]),
            make_record("oai:demo:gone", "2026-01-03", deleted=True)],
            end="2026-01-12")
        report = self.run_analysis(day="2026-01-04")
        self.assertIn("no_active_population", report["diagnostics"])
        self.assertEqual(report["population"]["N"], 0)
        self.assertEqual(report["family"]["m"], 6)
        self.assertEqual(len(report["tests"]), 6)
        for entry in report["tests"]:
            self.assertEqual(entry["table"],
                             {"a": 0, "b": 0, "c": 0, "d": 0, "N": 0})
            self.assertIsNone(entry["metrics"]["lift"])
            self.assertIsNone(entry["metrics"]["a_over_n"])
            self.assertIsNone(entry["fisher"]["p"])
            self.assertIsNone(entry["fisher"]["q"])
            self.assertIsNone(entry["fisher"]["rank"])
            self.assertFalse(entry["selection"]["selected"])
        self.assertEqual(report["clues"], [])
        # The tombstone still appears in the evidence.
        self.assertEqual(len(report["record_evidence"]), 1)
        self.assertTrue(report["record_evidence"][0]["deleted"])


# ---------------------------------------------------------------------------
# Acceptance 4: independent Fisher enumeration and BH reference
# ---------------------------------------------------------------------------

class FisherOracleTests(unittest.TestCase):
    def test_exhaustive_small_tables_match_enumeration(self):
        for n in range(1, 9):
            for n_a in range(0, n + 1):
                for n_b in range(0, n + 1):
                    lower = max(0, n_a + n_b - n)
                    upper = min(n_a, n_b)
                    for a in range(lower, upper + 1):
                        expected = ref_fisher_tail(n, n_a, n_b, a)
                        actual = fisher_one_sided_tail(n, n_a, n_b, a)
                        self.assertEqual(actual, expected,
                                         (n, n_a, n_b, a))
                        self.assertIsInstance(actual, Fraction)

    def test_tail_probabilities_sum_to_one_and_zero_margin_is_unit(self):
        n, n_a, n_b = 8, 5, 4
        lower = max(0, n_a + n_b - n)
        upper = min(n_a, n_b)
        denominator = math.comb(n, n_b)
        total = sum((
            Fraction(math.comb(n_a, k) * math.comb(n - n_a, n_b - k),
                     denominator)
            for k in range(lower, upper + 1)), Fraction(0))
        self.assertEqual(total, Fraction(1))
        # Zero-margin legal tables always yield 1/1, never NaN.
        self.assertEqual(fisher_one_sided_tail(6, 0, 0, 0), Fraction(1))
        self.assertEqual(fisher_one_sided_tail(6, 6, 0, 0), Fraction(1))

    def test_infeasible_tables_are_rejected(self):
        with self.assertRaises(AffinityError):
            fisher_one_sided_tail(5, 2, 2, 3)
        with self.assertRaises(AffinityError):
            fisher_one_sided_tail(5, 2, 2, -1)


class BenjaminiHochbergReferenceTests(unittest.TestCase):
    def test_explicit_sequence_reverse_minimum_and_ties(self):
        p_values = [
            ("t1", Fraction(1, 100)),
            ("t2", Fraction(1, 50)),
            ("t3", Fraction(1, 20)),
            ("t4", Fraction(1, 10)),
            ("t5", Fraction(3, 10)),
            ("t6", Fraction(1, 1)),
            ("t7", Fraction(1, 1))]
        m = len(p_values)
        expected = ref_bh(p_values)
        actual = benjamini_hochberg(p_values, m)
        self.assertEqual(actual, expected)
        # At rank 1 the candidate m*p/1 = 7/100 and nothing later is smaller,
        # so the reverse minimum leaves q_1 at 7/100.
        self.assertEqual(actual["t1"][2], Fraction(7, 100))
        # The rank-5 entry inherits the reverse minimum from the tail.
        self.assertEqual(actual["t5"][2], Fraction(21, 50))
        ranks = {pair: value[0] for pair, value in actual.items()}
        # Equal p=1 ties resolve by stable pair order, never by input luck.
        self.assertLess(ranks["t6"], ranks["t7"])

    def test_input_permutation_cannot_change_adjustment(self):
        p_values = [
            ("b", Fraction(1, 4)), ("a", Fraction(1, 4)),
            ("c", Fraction(1, 2)), ("d", Fraction(1, 2))]
        first = benjamini_hochberg(list(p_values), 4)
        shuffled = [p_values[2], p_values[0], p_values[3], p_values[1]]
        second = benjamini_hochberg(shuffled, 4)
        self.assertEqual(first, second)
        self.assertEqual(ref_bh(p_values), first)


class FullRecomputeTests(AffinityTestBase):
    def test_nontrivial_dataset_rebuilt_from_per_record_sets(self):
        # Five topics, ten records with deliberately overlapping memberships
        # so several pairs have non-trivial a values and mixed decisions.
        taxonomy = {
            "schema_version": 1,
            "nodes": [{"topic_id": t, "label": t}
                      for t in ("A", "B", "C", "D", "E")],
            "edges": [],
            "bindings": [
                {"subject": "sa", "topic_id": "A"},
                {"subject": "sb", "topic_id": "B"},
                {"subject": "sc", "topic_id": "C"},
                {"subject": "sd", "topic_id": "D"},
                {"subject": "se", "topic_id": "E"}],
        }
        memberships = [
            ["sa", "sb"], ["sa", "sb"], ["sa", "sb", "sc"],
            ["sc", "sd"], ["sd", "se"], ["sa", "se"],
            ["sb", "sc"], ["sa"], ["se"], []]
        records = [
            make_record(f"oai:demo:R{i}", f"2026-01-{i + 1:02d}",
                        title=f"Record {i}", subjects=subjects)
            for i, subjects in enumerate(memberships)]
        self.build_warehouse(records, taxonomy=taxonomy,
                             end="2026-01-12")
        write_plan(self.plan, ["A", "B", "C", "D", "E"],
                   alpha="0.25", min_shared=2)
        report = analyze(self.warehouse, self.plan, "2026-01-10")
        n_population, family, rebuilt, _adjusted = recompute_from_evidence(
            report, Fraction(1, 4), 2)

        self.assertEqual(n_population, 10)
        self.assertEqual(len(family), report["family"]["m"])
        self.assertEqual(
            sorted(family),
            sorted(tuple(t["pair"]) for t in report["tests"]))
        for pair in family:
            entry = self.find_test(report, pair)
            oracle = rebuilt[pair]
            self.assertEqual(
                tuple(entry["table"][k] for k in ("a", "b", "c", "d", "N")),
                oracle["table"], pair)
            self.assertEqual(entry["marginals"]["n_A"], oracle["n_a"], pair)
            self.assertEqual(entry["marginals"]["n_B"], oracle["n_b"], pair)
            self.assertEqual(entry["metrics"]["lift"],
                             fraction_text(oracle["lift"]), pair)
            self.assertEqual(entry["fisher"]["p"],
                             fraction_text(oracle["p"]), pair)
            self.assertEqual(entry["fisher"]["q"],
                             fraction_text(oracle["q"]), pair)
            self.assertEqual(entry["selection"]["selected"],
                             oracle["selected"], pair)
        # The clue board equals exactly the independently selected pairs.
        expected_clues = sorted(
            pair for pair in family if rebuilt[pair]["selected"])
        self.assertEqual(
            [tuple(c["pair"]) for c in report["clues"]], expected_clues)


# ---------------------------------------------------------------------------
# Acceptance 5: single read snapshot under a concurrent committing connection
# ---------------------------------------------------------------------------

def enable_wal(path):
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=5000")


class SnapshotTests(AffinityTestBase):
    def setUp(self):
        super().setUp()
        self.build_warehouse(six_records())
        enable_wal(self.warehouse)

    def _existing_page_digest(self):
        with connect(self.warehouse) as db:
            return db.execute(
                "SELECT page_sha256 FROM wh_versions LIMIT 1").fetchone()[0]

    def test_concurrent_commit_between_reads_uses_one_snapshot(self):
        baseline = analyze(
            self.warehouse,
            write_plan(self.plan, ["A", "B", "C", "U"]),
            "2026-01-06")
        self.assertEqual(baseline["population"]["N"], 6)

        def hook(path, day_text):
            with sqlite3.connect(path) as writer:
                writer.execute("PRAGMA busy_timeout=5000")
                writer.execute(
                    "INSERT INTO wh_versions "
                    "(version_id, identifier, datestamp, deleted, title, "
                    "valid_from, valid_to, has_subjects, has_unmapped, "
                    "page_sha256, canonical_json) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (999, "oai:demo:HOOK", "2026-01-02", 0, "Hook row",
                     "2026-01-02", "2026-01-10", 0, 0,
                     self._existing_page_digest(), '{"hook":true}'))
                writer.commit()

        hooked = analyze(
            self.warehouse, self.plan, "2026-01-06", snapshot_hook=hook)
        # The report must come from the snapshot established at BEGIN: the
        # concurrently committed row is invisible to this analysis.
        self.assertEqual(hooked["population"]["N"], 6)
        self.assertNotIn(
            "oai:demo:HOOK",
            [r["identifier"] for r in hooked["record_evidence"]])
        # A fresh read after the commit does observe the writer's row.
        after = analyze(self.warehouse, self.plan, "2026-01-06")
        self.assertEqual(after["population"]["N"], 7)
        self.assertIn(
            "oai:demo:HOOK",
            [r["identifier"] for r in after["record_evidence"]])

    def test_unrelated_committed_table_leaves_report_unchanged(self):
        write_plan(self.plan, ["A", "B", "C", "U"])
        before = analyze(self.warehouse, self.plan, "2026-01-06")
        before_bytes = serialize_report(before)

        def hook(path, day_text):
            with sqlite3.connect(path) as writer:
                writer.execute("PRAGMA busy_timeout=5000")
                writer.execute(
                    "CREATE TABLE IF NOT EXISTS affinity_probe (x INTEGER)")
                writer.execute("INSERT INTO affinity_probe VALUES (1),(2)")
                writer.commit()

        hooked = analyze(
            self.warehouse, self.plan, "2026-01-06", snapshot_hook=hook)
        self.assertEqual(serialize_report(hooked), before_bytes)


# ---------------------------------------------------------------------------
# Acceptance 6: digests rebuild from the report, determinism, no local state
# ---------------------------------------------------------------------------

class DigestAndDeterminismTests(AffinityTestBase):
    def setUp(self):
        super().setUp()
        self.build_warehouse(six_records())
        write_plan(self.plan, ["A", "B", "C", "U"])

    def test_digests_rebuild_after_warehouse_is_moved_away(self):
        report = analyze(self.warehouse, self.plan, "2026-01-06")
        hidden = self.dir / "hidden-warehouse.sqlite"
        shutil.move(self.warehouse, hidden)
        # Every logical digest is a pure function of report contents.
        self.assertEqual(metadata_digest(report),
                         report["digests"]["metadata_sha256"])
        self.assertEqual(relations_digest(report),
                         report["digests"]["selected_relations_sha256"])
        self.assertEqual(evidence_digest(report),
                         report["digests"]["record_evidence_sha256"])
        self.assertEqual(
            report["digests"]["plan_bytes_sha256"],
            hashlib.sha256(self.plan.read_bytes()).hexdigest())

    def test_repeated_runs_are_byte_identical_and_carry_no_local_state(self):
        first = serialize_report(
            analyze(self.warehouse, self.plan, "2026-01-06"))
        second = serialize_report(
            analyze(self.warehouse, self.plan, "2026-01-06"))
        self.assertEqual(first, second)
        text = first.decode("utf-8")
        self.assertNotIn("rowid", text)
        self.assertNotIn(str(self.dir), text)
        self.assertNotIn("/tmp", text)
        self.assertNotIn(".affinity-", text)

    def test_plan_array_reorder_changes_plan_digest_only(self):
        first = analyze(self.warehouse, self.plan, "2026-01-06")
        reordered = self.dir / "plan-reordered.json"
        write_plan(reordered, ["U", "C", "B", "A"])
        second = analyze(self.warehouse, reordered, "2026-01-06")
        self.assertNotEqual(
            first["digests"]["plan_bytes_sha256"],
            second["digests"]["plan_bytes_sha256"])
        # Business facts are independent of plan array order.
        self.assertEqual(first["family"], second["family"])
        self.assertEqual(
            [(t["pair"], t["table"], t["fisher"]) for t in first["tests"]],
            [(t["pair"], t["table"], t["fisher"]) for t in second["tests"]])
        self.assertEqual(first["clues"], second["clues"])
        self.assertEqual(
            first["digests"]["record_evidence_sha256"],
            second["digests"]["record_evidence_sha256"])

    def test_unrelated_table_change_does_not_move_logical_digests(self):
        before = analyze(self.warehouse, self.plan, "2026-01-06")
        with sqlite3.connect(self.warehouse) as db:
            db.execute("CREATE TABLE affinity_probe (note TEXT)")
            db.execute("INSERT INTO affinity_probe VALUES (?)",
                       ("irrelevant",))
            db.commit()
        after = analyze(self.warehouse, self.plan, "2026-01-06")
        self.assertEqual(
            serialize_report(after), serialize_report(before))


# ---------------------------------------------------------------------------
# Acceptance 7: strict plan, date and warehouse validation
# ---------------------------------------------------------------------------

class PlanValidationTests(AffinityTestBase):
    def setUp(self):
        super().setUp()
        self.build_warehouse(six_records())

    def _analyze_raw_plan(self, raw_text):
        self.plan.write_text(raw_text, encoding="utf-8")
        return analyze(self.warehouse, self.plan, "2026-01-06")

    def test_duplicate_json_keys_are_rejected(self):
        with self.assertRaisesRegex(AffinityError, "duplicate JSON object"):
            self._analyze_raw_plan(
                '{"schema_version":1,"topics":["A","B"],'
                '"alpha":"0.3","alpha":"0.2","min_shared":1}')

    def test_schema_version_must_be_integer_one(self):
        for bad in ("true", '"1"', "2", "1.0", "null"):
            with self.assertRaisesRegex(AffinityError, "integer 1"):
                self._analyze_raw_plan(
                    f'{{"schema_version":{bad},"topics":["A","B"],'
                    f'"alpha":"0.3","min_shared":1}}')

    def test_topic_array_rules(self):
        base = ('{{"schema_version":1,"topics":{topics},"alpha":"0.3",'
                '"min_shared":1}}')
        with self.assertRaises(AffinityError):
            self._analyze_raw_plan(base.format(topics='["A"]'))
        with self.assertRaises(AffinityError):
            self._analyze_raw_plan(base.format(
                topics="[" + ",".join(f'"T{i}"' for i in range(51)) + "]"))
        with self.assertRaises(AffinityError):
            self._analyze_raw_plan(base.format(topics='["A","A"]'))
        with self.assertRaises(AffinityError):
            self._analyze_raw_plan(base.format(topics='[" A","B"]'))
        with self.assertRaises(AffinityError):
            self._analyze_raw_plan(base.format(topics='["","B"]'))
        with self.assertRaises(AffinityError):
            self._analyze_raw_plan(base.format(topics='[1,2]'))

    def test_unknown_topic_rejected(self):
        with self.assertRaisesRegex(AffinityError, "not registered"):
            self._analyze_raw_plan(
                '{"schema_version":1,"topics":["A","GHOST"],'
                '"alpha":"0.3","min_shared":1}')

    def test_alpha_decimal_rules(self):
        def with_alpha(value):
            return ('{"schema_version":1,"topics":["A","B"],'
                    f'"alpha":{value},"min_shared":1}}')
        for bad in ('0.3', '"0.3 "', '"3e-1"', '"+0.3"', '".3"',
                    '"1.0000001"', '"0"', '"NaN"', '"Infinity"',
                    '"-0.1"', '"1.000001"', '"0x1"'):
            with self.assertRaises(AffinityError, msg=bad):
                self._analyze_raw_plan(with_alpha(bad))
        for good in ('"0.000001"', '"1"', '"1.0"', '"0.300000"', '"0.3"'):
            report = self._analyze_raw_plan(with_alpha(good))
            self.assertEqual(
                report["plan"]["alpha"]["raw"], json.loads(good))

    def test_min_shared_rules(self):
        def with_min_shared(value):
            return ('{"schema_version":1,"topics":["A","B"],'
                    f'"alpha":"0.3","min_shared":{value}}}')
        for bad in ("0", "1000000001", "true", '"3"', "1.5", "null"):
            with self.assertRaises(AffinityError, msg=bad):
                self._analyze_raw_plan(with_min_shared(bad))

    def test_top_level_must_have_exactly_four_keys(self):
        with self.assertRaisesRegex(AffinityError, "exactly"):
            self._analyze_raw_plan(
                '{"schema_version":1,"topics":["A","B"],'
                '"alpha":"0.3","min_shared":1,"extra":true}')
        with self.assertRaisesRegex(AffinityError, "exactly"):
            self._analyze_raw_plan(
                '{"schema_version":1,"topics":["A","B"],"alpha":"0.3"}')

    def test_parse_plan_directly(self):
        plan = parse_plan(
            b'{"schema_version":1,"topics":["A","B"],'
            b'"alpha":"0.3","min_shared":2}')
        self.assertEqual(plan["alpha"], Fraction(3, 10))
        self.assertEqual(plan["min_shared"], 2)
        with self.assertRaises(AffinityError):
            parse_plan(b"")
        with self.assertRaises(AffinityError):
            parse_plan("{invalid".encode("utf-8"))


class WarehouseAndDateValidationTests(AffinityTestBase):
    def setUp(self):
        super().setUp()
        self.build_warehouse(six_records())
        write_plan(self.plan, ["A", "B", "C", "U"])

    def test_missing_database_is_not_created(self):
        missing = self.dir / "absent.sqlite"
        with self.assertRaises(AffinityError):
            analyze(missing, self.plan, "2026-01-06")
        self.assertFalse(missing.exists())

    def test_date_must_be_strict_and_inside_window(self):
        for bad in ("2026-13-01", "2026-01-06T00:00", "20260106",
                    "2025-12-31", "2026-01-10"):
            with self.assertRaises(AffinityError, msg=bad):
                analyze(self.warehouse, self.plan, bad)

    def test_non_warehouse_database_rejected(self):
        with self.assertRaisesRegex(AffinityError, "warehouse"):
            analyze(self.source, self.plan, "2026-01-06")

    def test_unsupported_format_version_rejected(self):
        with sqlite3.connect(self.warehouse) as db:
            db.execute("UPDATE wh_meta SET value='999' "
                       "WHERE key='format_version'")
            db.commit()
        with self.assertRaisesRegex(AffinityError, "format version"):
            analyze(self.warehouse, self.plan, "2026-01-06")

    def test_missing_required_table_rejected(self):
        with sqlite3.connect(self.warehouse) as db:
            db.execute("DROP TABLE wh_version_ancestors")
            db.commit()
        with self.assertRaisesRegex(AffinityError, "missing required tables"):
            analyze(self.warehouse, self.plan, "2026-01-06")

    def _tampered_copy(self):
        target = self.dir / "tampered.sqlite"
        shutil.copy(self.warehouse, target)
        return target

    def test_duplicate_effective_version_names_identifier(self):
        target = self._tampered_copy()
        with sqlite3.connect(target) as db:
            digest = db.execute(
                "SELECT page_sha256 FROM wh_versions LIMIT 1").fetchone()[0]
            db.execute(
                "INSERT INTO wh_versions "
                "(version_id, identifier, datestamp, deleted, title, "
                "valid_from, valid_to, has_subjects, has_unmapped, "
                "page_sha256, canonical_json) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (998, "oai:demo:1", "2026-01-02", 0, "Duplicate",
                 "2026-01-02", "2026-01-10", 0, 0, digest, '{"x":1}'))
            db.commit()
        with self.assertRaisesRegex(AffinityError, "oai:demo:1"):
            analyze(target, self.plan, "2026-01-06")

    def test_bridge_to_unknown_topic_rejected(self):
        target = self._tampered_copy()
        with sqlite3.connect(target) as db:
            version_id = db.execute(
                "SELECT version_id FROM wh_versions LIMIT 1").fetchone()[0]
            db.execute(
                "INSERT INTO wh_version_ancestors VALUES (?,?)",
                (version_id, "GHOST"))
            db.commit()
        with self.assertRaisesRegex(AffinityError, "unknown topic"):
            analyze(target, self.plan, "2026-01-06")

    def test_edge_without_closure_rejected(self):
        target = self._tampered_copy()
        with sqlite3.connect(target) as db:
            db.execute(
                "INSERT INTO wh_taxonomy_edges VALUES (?,?)", ("U", "A"))
            db.commit()
        with self.assertRaisesRegex(AffinityError, "no matching closure row"):
            analyze(target, self.plan, "2026-01-06")

    def test_deleted_version_with_bridge_rejected(self):
        target = self._tampered_copy()
        with sqlite3.connect(target) as db:
            deleted_id = db.execute(
                "SELECT version_id FROM wh_versions WHERE deleted=1 "
                "LIMIT 1").fetchone()
            # The headline fixture has no deletion; add one first.
            if deleted_id is None:
                digest = db.execute(
                    "SELECT page_sha256 FROM wh_versions LIMIT 1"
                ).fetchone()[0]
                db.execute(
                    "INSERT INTO wh_versions "
                    "(version_id, identifier, datestamp, deleted, title, "
                    "valid_from, valid_to, has_subjects, has_unmapped, "
                    "page_sha256, canonical_json) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (997, "oai:demo:DEL", "2026-01-05", 1, None,
                     "2026-01-05", "2026-01-10", 0, 0, digest, '{"d":1}'))
                deleted_id = (997,)
            db.execute(
                "INSERT INTO wh_version_ancestors VALUES (?,?)",
                (deleted_id[0], "A"))
            db.commit()
        with self.assertRaisesRegex(AffinityError, "deleted version"):
            analyze(target, self.plan, "2026-01-06")


# ---------------------------------------------------------------------------
# Acceptance 8: atomic publication, failure injection, aliases, real CLI
# ---------------------------------------------------------------------------

def run_cli(*args, env=None, expect_code=0):
    clean_env = {key: value for key, value in os.environ.items()
                 if key.upper() != "PYTHONPATH"}
    clean_env["PYTHONPATH"] = str(ROOT)
    if env:
        clean_env.update(env)
    proc = subprocess.run(
        [sys.executable, "-E", "-m", "catalog_harvest.affinity",
         *map(str, args)],
        cwd=ROOT, capture_output=True, text=True, env=clean_env)
    if proc.returncode != expect_code:
        raise AssertionError(
            f"affinity CLI exited {proc.returncode}, expected {expect_code}\n"
            f"stdout={proc.stdout}\nstderr={proc.stderr}")
    return proc


class PublicationTests(AffinityTestBase):
    def setUp(self):
        super().setUp()
        self.build_warehouse(six_records())
        write_plan(self.plan, ["A", "B", "C", "U"])
        self.report = analyze(self.warehouse, self.plan, "2026-01-06")

    def test_successful_publish_is_deterministic(self):
        digest = publish_report(
            self.warehouse, self.plan, self.output, self.report)
        on_disk = self.output.read_bytes()
        self.assertEqual(hashlib.sha256(on_disk).hexdigest(), digest)
        self.assertEqual(on_disk, serialize_report(self.report))
        # Re-publishing overwrites with identical bytes.
        publish_report(self.warehouse, self.plan, self.output, self.report)
        self.assertEqual(self.output.read_bytes(), on_disk)

    def test_write_and_replace_failures_keep_old_report_and_clean_temp(self):
        publish_report(
            self.warehouse, self.plan, self.output, self.report)
        original = self.output.read_bytes()
        for hook in (ENV_WRITE_FAIL, ENV_REPLACE_FAIL):
            with unittest.mock.patch.dict(os.environ, {hook: "1"}):
                with self.assertRaisesRegex(AffinityError, "injected"):
                    publish_report(
                        self.warehouse, self.plan, self.output, self.report)
            self.assertEqual(self.output.read_bytes(), original)
            self.assertEqual(list(self.dir.glob(".affinity-*")), [])
        # Corrected rerun succeeds without manual cleanup.
        publish_report(
            self.warehouse, self.plan, self.output, self.report)
        self.assertEqual(self.output.read_bytes(), original)

    def test_output_cannot_alias_database_or_plan(self):
        db_link = self.dir / "db-link.json"
        os.symlink(self.warehouse, db_link)
        with self.assertRaises(AffinityError):
            publish_report(
                self.warehouse, self.plan, db_link, self.report)
        hard_link = self.dir / "db-hard.json"
        os.link(self.warehouse, hard_link)
        with self.assertRaises(AffinityError):
            publish_report(
                self.warehouse, self.plan, hard_link, self.report)
        with self.assertRaises(AffinityError):
            publish_report(
                self.warehouse, self.plan, self.plan, self.report)

    def test_source_inputs_are_not_modified_by_publish(self):
        warehouse_bytes = self.warehouse.read_bytes()
        plan_bytes = self.plan.read_bytes()
        publish_report(
            self.warehouse, self.plan, self.output, self.report)
        self.assertEqual(self.warehouse.read_bytes(), warehouse_bytes)
        self.assertEqual(self.plan.read_bytes(), plan_bytes)


class CliEndToEndTests(AffinityTestBase):
    def test_real_cli_harvest_build_analyze_and_independent_check(self):
        # 1. Real loopback OAI-PMH collection through the shipping harvester.
        harvest_records(self.source, six_records())
        write_taxonomy(self.taxonomy, flat_taxonomy())
        # 2. Real warehouse build entry point.
        build_warehouse(self.source, self.taxonomy, "2026-01-01",
                        "2026-01-10", self.warehouse)
        write_plan(self.plan, ["A", "B", "C", "U"])
        # 3. Real affinity CLI subprocess, atomic publication.
        proc = run_cli(
            "--database", self.warehouse, "--date", "2026-01-06",
            "--plan", self.plan, "--output", self.output)
        summary = json.loads(proc.stdout)
        self.assertEqual(summary["status"], "published")
        self.assertEqual(summary["N"], 6)
        self.assertEqual(summary["family_size"], 6)
        self.assertEqual(summary["clues"], 1)
        report = json.loads(self.output.read_text(encoding="utf-8"))
        self.assertEqual(report["schema_version"],
                         affinity.AFFINITY_SCHEMA_VERSION)
        self.assertEqual(report["mode"], affinity.AFFINITY_MODE)
        # 4. Independent recomputation straight from the emitted evidence.
        n_population, family, rebuilt, _adjusted = recompute_from_evidence(
            report, Fraction(3, 10), 3)
        self.assertEqual(n_population, 6)
        ab = rebuilt[("A", "B")]
        self.assertEqual(ab["table"], (3, 0, 0, 3, 6))
        self.assertEqual(ab["p"], Fraction(1, 20))
        self.assertEqual(ab["q"], Fraction(3, 10))
        self.assertEqual(ab["lift"], Fraction(2, 1))
        self.assertTrue(ab["selected"])
        self.assertTrue(
            all(not rebuilt[p]["selected"] for p in family if p != ("A", "B")))

    def test_cli_failure_is_nonzero_and_leaves_no_output_or_temp(self):
        self.build_warehouse(six_records())
        self.plan.write_text(
            '{"schema_version":1,"topics":["A","B"],"alpha":"bad",'
            '"min_shared":1}', encoding="utf-8")
        run_cli("--database", self.warehouse, "--date", "2026-01-06",
                "--plan", self.plan, "--output", self.output,
                expect_code=2)
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.dir.glob(".affinity-*")), [])

    def test_cli_string_snapshot_hook_keeps_single_snapshot(self):
        self.build_warehouse(six_records())
        enable_wal(self.warehouse)
        write_plan(self.plan, ["A", "B", "C", "U"])
        hook_script = self.dir / "hook.py"
        hook_script.write_text(
            "import sqlite3, sys\n"
            "path, day = sys.argv[1], sys.argv[2]\n"
            "db = sqlite3.connect(path)\n"
            "db.execute('PRAGMA busy_timeout=5000')\n"
            "db.execute('CREATE TABLE IF NOT EXISTS affinity_probe (x INT)')\n"
            "db.execute('INSERT INTO affinity_probe VALUES (7)')\n"
            "db.commit()\n"
            "db.close()\n",
            encoding="utf-8")
        baseline = serialize_report(
            analyze(self.warehouse, self.plan, "2026-01-06"))
        proc = run_cli(
            "--database", self.warehouse, "--date", "2026-01-06",
            "--plan", self.plan, "--output", self.output,
            env={ENV_SNAPSHOT_HOOK: str(hook_script)})
        self.assertEqual(json.loads(proc.stdout)["N"], 6)
        self.assertEqual(self.output.read_bytes(), baseline)


if __name__ == "__main__":
    unittest.main()
