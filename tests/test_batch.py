"""End-to-end tests for the persistent three-stage batch orchestrator.

Every scenario uses a real loopback OAI-PMH fixture (a subprocess with a
request log), the real ``python -m catalog_harvest.batch`` CLI for
cross-process recovery, deterministic process-exit barriers placed exactly in
the commit/ledger gap, and original synthetic records. No timing-based sleeps
decide any outcome.
"""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from catalog_harvest.batch import (
    BatchError, run_batch, status_batch, parse_plan, plan_stage_names,
    ENV_BARRIER, ENV_FAIL_STAGE)
from catalog_harvest.affinity import evidence_digest
from tests.oai_fixtures import (FixtureProcess, batch_cli_json, run_batch_cli,
                                record_xml, page_xml)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def dated_match(from_date, until_date):
    return {"verb": ["ListRecords"], "metadataPrefix": ["oai_dc"],
            "from": [from_date], "until": [until_date]}


def token_match(token):
    return {"verb": ["ListRecords"], "resumptionToken": [token]}


def window_pages():
    """Two pages in one frozen [2026-01-01, 2026-01-05] window."""
    first = page_xml([
        record_xml("oai:demo:A", "2026-01-01",
                   title="River flow & sediment", subjects=["hydro"]),
        record_xml("oai:demo:B", "2026-01-02", title="Water survey",
                   subjects=["water"]),
    ], token="page/two")
    second = page_xml([
        record_xml("oai:demo:C", "2026-01-03", title="Catchment review",
                   subjects=["hydro", "water"]),
        record_xml("oai:demo:B", "2026-01-04", deleted=True),
    ])
    return [
        {"match": dated_match("2026-01-01", "2026-01-05"),
         "body": first.decode()},
        {"match": token_match("page/two"), "body": second.decode()},
    ]


class BatchTestBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.workdir = self.dir / "batches"
        self.run_id = "batch-one"
        self.plan_path = self.dir / "plan.json"
        self.affinity_plan = self.dir / "affinity.json"
        self.affinity_plan.write_bytes(
            (DATA / "affinity_plan_example.json").read_bytes())
        self.taxonomy = self.dir / "taxonomy.json"
        self.taxonomy.write_bytes((DATA / "taxonomy_demo.json").read_bytes())

    def fixture(self, pages):
        proc = FixtureProcess(self.dir, {"pages": pages,
                                         "fallback": {"status": 400,
                                                      "body": "unexpected"}})
        self.addCleanup(proc.shutdown)
        return proc

    def batch_dir(self):
        return self.workdir / self.run_id

    def write_plan(self, endpoint, reports=("2026-01-03", "2026-01-04"),
                   until="2026-01-05", start="2026-01-01",
                   wh_end="2026-01-08", extra=None):
        plan = {
            "schema_version": 1,
            "sync": {"endpoint": endpoint, "start_date": start,
                     "until": until, "overlap_days": 2, "max_pages": 20},
            "warehouse": {"start": "2026-01-01", "end": wh_end,
                          "taxonomy_file": str(self.taxonomy)},
            "reports": list(reports),
            "affinity": {"plan_file": str(self.affinity_plan)},
        }
        if extra:
            plan.update(extra)
        self.plan_path.write_text(json.dumps(plan), encoding="utf-8")
        return plan

    def stage_states(self):
        info = status_batch(self.workdir, self.run_id)
        return {stage["name"]: stage["state"] for stage in info["stages"]}

    def stage_evidence(self, name):
        info = status_batch(self.workdir, self.run_id)
        return next(stage for stage in info["stages"]
                    if stage["name"] == name)["artifact"]

    def _artifact_digests(self):
        # state.lock carries the current holder's pid, so it is not an artifact.
        return {path.relative_to(self.batch_dir()).as_posix():
                hashlib.sha256(path.read_bytes()).hexdigest()
                for path in self.batch_dir().rglob("*")
                if path.is_file() and path.name != "state.lock"}

    def recovery_events(self):
        info = status_batch(self.workdir, self.run_id)
        return [(event["stage"], event["action"])
                for event in info["recovery_events"]]

    def ledger_rows(self):
        db = sqlite3.connect(self.batch_dir() / "state.sqlite")
        db.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in db.execute(
                "SELECT * FROM attempt ORDER BY id")]
        finally:
            db.close()


class AcceptanceTests(BatchTestBase):
    def test_full_batch_uses_real_loopback_source_and_one_request_each(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            requests = fx.requests()
        self.assertTrue(result["complete"])
        self.assertEqual(result["result"], "complete")
        self.assertEqual(
            [stage["name"] for stage in result["stages"]],
            ["sync", "warehouse", "report:2026-01-03",
             "report:2026-01-04", "manifest"])
        self.assertTrue(all(stage["state"] == "succeeded"
                            for stage in result["stages"]))
        # Exactly the two frozen-window requests, with the opaque token intact.
        self.assertEqual(requests, [dated_match("2026-01-01", "2026-01-05"),
                                    token_match("page/two")])
        info = status_batch(self.workdir, self.run_id)
        self.assertTrue(info["complete"])
        self.assertTrue(info["delivery_manifest"]["valid"])
        manifest = json.loads(
            (self.batch_dir() / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["run_id"], self.run_id)
        self.assertEqual(manifest["sync"]["watermark"], "2026-01-05")
        self.assertEqual(manifest["sync"]["pages"], 2)
        self.assertEqual(manifest["sync"]["records_seen"], 4)
        self.assertEqual(manifest["warehouse"]["versions"], 4)
        self.assertEqual([report["date"] for report in manifest["reports"]],
                         ["2026-01-03", "2026-01-04"])
        for report in manifest["reports"]:
            path = self.batch_dir() / report["path"]
            self.assertEqual(
                hashlib.sha256(path.read_bytes()).hexdigest(),
                report["sha256"])
            published = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(published["date"], report["date"])
            self.assertEqual(published["digests"]["record_evidence_sha256"],
                             evidence_digest(published))

    def test_frozen_inputs_are_private_byte_copies_with_recorded_digests(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
        metadata = json.loads(
            (self.batch_dir() / "batch.json").read_text(encoding="utf-8"))
        expected = {"plan": self.plan_path,
                    "taxonomy": self.taxonomy,
                    "affinity": self.affinity_plan}
        for kind, source in expected.items():
            record = metadata["inputs"][kind]
            frozen = self.batch_dir() / record["path"]
            self.assertEqual(frozen.read_bytes(), source.read_bytes())
            self.assertEqual(hashlib.sha256(frozen.read_bytes()).hexdigest(),
                             record["sha256"])
        # The batch directory holds no copy of the machine plan path layout.
        self.assertEqual(
            sorted(path.name for path in (self.batch_dir() / "inputs").iterdir()),
            ["affinity_plan.json", "plan.json", "taxonomy.json"])

    def test_complete_batch_replay_skips_every_stage_and_keeps_artifacts(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
            before = self._artifact_digests()
            requests_before = len(fx.requests())
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
        self.assertTrue(result["complete"])
        self.assertTrue(all(stage["attempts"] == 1
                            for stage in result["stages"]))
        after = self._artifact_digests()
        # Warehouse/report bytes are deterministic; the manifest embeds the
        # same attempts, so every artifact is byte-identical after a replay.
        self.assertEqual(before, after)


class InterruptionTests(BatchTestBase):
    def _barrier_run(self, endpoint, barrier, expect_code=75):
        proc = run_batch_cli(
            "run", "--plan", self.plan_path, "--workdir", self.workdir,
            "--run-id", self.run_id, "--json",
            env={ENV_BARRIER: barrier}, expect_code=expect_code)
        events = [json.loads(line) for line in proc.stdout.splitlines()
                  if line.strip()]
        self.assertEqual(events[-1], {"barrier": barrier})

    def test_sync_commit_gap_is_reattached_without_reopening_the_window(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            # Sync publishes and exits before its ledger row is updated.
            self._barrier_run(fx.endpoint, "after-sync")
            self.assertEqual(self.stage_states()["sync"], "running")
            self.assertEqual(len(fx.requests()), 2)
            # The published window is proven to be this batch's, reattached
            # without any new HTTP request.
            result = batch_cli_json(
                "run", "--plan", self.plan_path, "--workdir", self.workdir,
                "--run-id", self.run_id, "--json")
            self.assertEqual(len(fx.requests()), 2)
        self.assertTrue(result["complete"])
        states = self.stage_states()
        self.assertEqual(states["sync"], "succeeded")
        self.assertIn({"stage": "sync", "action": "reattached"},
                      result["recovery"])
        self.assertIn(("sync", "reattached"), self.recovery_events())
        self.assertEqual(
            [row["stage"] for row in self.ledger_rows()
             if row["outcome"] == "succeeded"],
            ["warehouse", "report:2026-01-03", "report:2026-01-04",
             "manifest"])
        self.assertEqual(
            [row["outcome"] for row in self.ledger_rows()
             if row["stage"] == "sync"], ["interrupted"])

    def test_warehouse_publish_gap_is_reattached_not_rebuilt(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            self._barrier_run(fx.endpoint, "after-warehouse")
            warehouse = self.batch_dir() / "warehouse.sqlite"
            digest = hashlib.sha256(warehouse.read_bytes()).hexdigest()
            self.assertEqual(self.stage_states()["warehouse"], "running")
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(hashlib.sha256(warehouse.read_bytes()).hexdigest(),
                             digest)
            states = self.stage_states()
        self.assertTrue(result["complete"])
        self.assertIn({"stage": "warehouse", "action": "reattached"},
                      result["recovery"])
        self.assertEqual(states["warehouse"], "succeeded")
        self.assertEqual(states["sync"], "succeeded")
        self.assertIn(("warehouse", "reattached"), self.recovery_events())
        warehouse_attempts = [row for row in self.ledger_rows()
                              if row["stage"] == "warehouse"]
        self.assertEqual([row["outcome"] for row in warehouse_attempts],
                         ["interrupted"])

    def test_report_publish_gap_retries_only_that_report(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            self._barrier_run(fx.endpoint, "after-report:2026-01-03")
            self.assertEqual(
                self.stage_states()["report:2026-01-03"], "running")
            self.assertEqual(
                self.stage_states()["report:2026-01-04"], "pending")
            report = self.batch_dir() / "reports" / "2026-01-03.json"
            digest = hashlib.sha256(report.read_bytes()).hexdigest()
            requests_before = len(fx.requests())
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
        self.assertTrue(result["complete"])
        self.assertEqual(hashlib.sha256(report.read_bytes()).hexdigest(),
                         digest)
        self.assertIn({"stage": "report:2026-01-03", "action": "reattached"},
                      result["recovery"])
        attempts = {row["stage"]: row["outcome"]
                    for row in self.ledger_rows()}
        self.assertEqual(attempts, {
            "sync": "succeeded",
            "warehouse": "succeeded",
            "report:2026-01-03": "interrupted",
            "report:2026-01-04": "succeeded",
            "manifest": "succeeded",
        })
        self.assertIn(("report:2026-01-03", "reattached"),
                      self.recovery_events())

    def test_sync_checkpoint_interruption_resumes_without_refetching_page(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            # The sync machine exits right after page 1's checkpoint commits;
            # the batch ledger row is left 'running' with no evidence.
            proc = subprocess.run(
                [sys.executable, "-E", "-m", "catalog_harvest.batch", "run",
                 "--plan", str(self.plan_path), "--workdir", str(self.workdir),
                 "--run-id", self.run_id, "--json"],
                cwd=ROOT, capture_output=True, text=True,
                env={**os.environ, "PYTHONPATH": str(ROOT),
                     "CATALOG_HARVEST_EXIT_AFTER_PAGE": "1"})
            self.assertEqual(proc.returncode, 7, proc.stderr)
            self.assertEqual(len(fx.requests()), 1)
            self.assertEqual(self.stage_states()["sync"], "running")
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            # Only page 2 is requested; page 1's checkpoint is reused.
            self.assertEqual(fx.requests(), [
                dated_match("2026-01-01", "2026-01-05"),
                token_match("page/two")])
        self.assertTrue(result["complete"])
        self.assertIn({"stage": "sync", "action": "resumed"},
                      result["recovery"])
        self.assertEqual(
            [row["outcome"] for row in self.ledger_rows()
             if row["stage"] == "sync"], ["interrupted", "succeeded"])

    def test_barrier_before_a_stage_leaves_it_pending_and_resumable(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            self._barrier_run(fx.endpoint, "before-warehouse")
            states = self.stage_states()
            self.assertEqual(states["sync"], "succeeded")
            self.assertEqual(states["warehouse"], "running")
            self.assertEqual(states["report:2026-01-03"], "pending")
            self.assertFalse((self.batch_dir() / "warehouse.sqlite").exists())
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), 2)
        self.assertTrue(result["complete"])
        self.assertIn({"stage": "warehouse", "action": "resumed"},
                      result["recovery"])


    def test_pending_row_with_committed_window_is_reattached(self):
        """A commit that landed before the ledger row turned even 'running'."""
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            self._barrier_run(fx.endpoint, "after-sync")
            db = sqlite3.connect(self.batch_dir() / "state.sqlite")
            try:
                with db:
                    db.execute("UPDATE stage SET state='pending', "
                               "evidence_json=NULL WHERE name='sync'")
                    db.execute("DELETE FROM attempt")
            finally:
                db.close()
            requests_before = len(fx.requests())
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
        self.assertTrue(result["complete"])
        self.assertIn({"stage": "sync", "action": "reattached"},
                      result["recovery"])
        self.assertEqual(self.stage_states()["sync"], "succeeded")

    def test_foreign_warehouse_is_preserved_and_blocks_reports(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            self._barrier_run(fx.endpoint, "before-warehouse")
            foreign = self.batch_dir() / "warehouse.sqlite"
            self.assertFalse(foreign.exists())
            # A different, valid warehouse (other taxonomy) is planted.
            other_taxonomy = self.dir / "other-taxonomy.json"
            other_taxonomy.write_text(json.dumps(
                {"schema_version": 1,
                 "nodes": [{"topic_id": "x", "label": "X"}],
                 "edges": [], "bindings": []}), encoding="utf-8")
            from catalog_harvest.warehouse import build_warehouse
            build_warehouse(self.batch_dir() / "catalog.sqlite",
                            other_taxonomy, "2026-01-01", "2026-01-08",
                            foreign)
            planted = foreign.read_bytes()
            with self.assertRaisesRegex(BatchError, "does not match"):
                run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(foreign.read_bytes(), planted)
            self.assertEqual(self.stage_states()["warehouse"], "failed")
            self.assertFalse((self.batch_dir() / "manifest.json").exists())
            self.assertFalse(
                (self.batch_dir() / "reports" / "2026-01-03.json").exists())

    def test_foreign_catalog_database_is_preserved_not_overwritten(self):
        """A full-harvest DB in the batch path is not this batch's artifact."""
        from catalog_harvest.harvest import harvest as full_harvest
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            # Create the batch directory through an interrupted sync run,
            # then plant a foreign full-harvest database where sync writes.
            run_batch_cli(
                "run", "--plan", self.plan_path, "--workdir", self.workdir,
                "--run-id", self.run_id, "--json",
                env={ENV_BARRIER: "before-sync"}, expect_code=75)
            foreign = self.batch_dir() / "catalog.sqlite"
            foreign.unlink(missing_ok=True)
            with tempfile.TemporaryDirectory() as other:
                data_dir = Path(other)
                for name in ("page1.xml", "page2.xml"):
                    (data_dir / name).write_bytes((DATA / name).read_bytes())
                from catalog_harvest.demo import fixture_server
                from catalog_harvest.transport import page_fetcher
                with fixture_server(data_dir) as endpoint:
                    full_harvest(page_fetcher(endpoint), foreign)
            planted = foreign.read_bytes()
            with self.assertRaises(BatchError) as caught:
                run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertIn("foreign", str(caught.exception))
            self.assertEqual(foreign.read_bytes(), planted)
            self.assertEqual(self.stage_states()["sync"], "failed")


    def test_foreign_report_for_wrong_date_fails_closed_and_is_preserved(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            # Warehouse is published, reports have not started.
            self._barrier_run(fx.endpoint, "before-report:2026-01-03")
            from catalog_harvest.affinity import analyze, publish_report
            other = self.dir / "foreign-report.json"
            report = analyze(self.batch_dir() / "warehouse.sqlite",
                             self.batch_dir() / "inputs" / "affinity_plan.json",
                             "2026-01-04")
            publish_report(self.batch_dir() / "warehouse.sqlite",
                           self.batch_dir() / "inputs" / "affinity_plan.json",
                           other, report)
            planted = (self.batch_dir() / "reports" / "2026-01-03.json")
            planted.parent.mkdir(parents=True, exist_ok=True)
            planted.write_bytes(other.read_bytes())
            with self.assertRaises(BatchError) as caught:
                run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertIn("report identity", str(caught.exception))
            self.assertEqual(planted.read_bytes(), other.read_bytes())
            self.assertEqual(
                self.stage_states()["report:2026-01-03"], "failed")
            self.assertFalse((self.batch_dir() / "manifest.json").exists())


    def test_manifest_publish_gap_is_reattached_without_rewrite(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            self._barrier_run(fx.endpoint, "after-manifest")
            manifest = self.batch_dir() / "manifest.json"
            digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
            self.assertEqual(self.stage_states()["manifest"], "running")
            requests_before = len(fx.requests())
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
        self.assertTrue(result["complete"])
        self.assertIn({"stage": "manifest", "action": "reattached"},
                      result["recovery"])
        self.assertEqual(hashlib.sha256(manifest.read_bytes()).hexdigest(),
                         digest)
        self.assertEqual(self.recovery_events()[-1],
                         ("manifest", "reattached"))

    def test_foreign_manifest_bytes_are_preserved_and_not_adopted(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            self._barrier_run(fx.endpoint, "after-manifest")
            manifest = self.batch_dir() / "manifest.json"
            original = manifest.read_bytes()
            planted = original.replace(b'"clues"', b' "clues"', 1)
            self.assertNotEqual(planted, original)
            try:
                manifest.write_bytes(planted)
                with self.assertRaisesRegex(BatchError, "preserved unchanged"):
                    run_batch(self.plan_path, self.workdir, self.run_id)
                self.assertEqual(manifest.read_bytes(), planted)
            finally:
                manifest.write_bytes(original)
            result = run_batch(self.plan_path, self.workdir, self.run_id)
        self.assertTrue(result["complete"])
        self.assertIn(("manifest", "reattached"), self.recovery_events())


class FailureTests(BatchTestBase):
    def test_failed_report_retried_alone_and_success_not_rerun(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            failing = run_batch_cli(
                "run", "--plan", self.plan_path, "--workdir", self.workdir,
                "--run-id", self.run_id, "--json",
                env={ENV_FAIL_STAGE: "report:2026-01-04"}, expect_code=3)
            summary = json.loads(failing.stdout)
            self.assertFalse(summary["complete"])
            states = {stage["name"]: stage["state"]
                      for stage in summary["stages"]}
            self.assertEqual(states["sync"], "succeeded")
            self.assertEqual(states["warehouse"], "succeeded")
            self.assertEqual(states["report:2026-01-03"], "succeeded")
            self.assertEqual(states["report:2026-01-04"], "failed")
            self.assertEqual(states["manifest"], "blocked")
            # No complete delivery manifest can exist for a partial batch.
            self.assertFalse((self.batch_dir() / "manifest.json").exists())
            info = status_batch(self.workdir, self.run_id)
            self.assertIsNone(info["delivery_manifest"])
            first_report = self.batch_dir() / "reports" / "2026-01-03.json"
            first_digest = hashlib.sha256(first_report.read_bytes()).hexdigest()
            self.assertFalse(
                (self.batch_dir() / "reports" / "2026-01-04.json").exists())
            requests_before = len(fx.requests())

            result = run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
        self.assertTrue(result["complete"])
        self.assertEqual(hashlib.sha256(first_report.read_bytes()).hexdigest(),
                         first_digest)
        attempts = {row["stage"]: row["outcome"]
                    for row in self.ledger_rows()}
        self.assertEqual(attempts, {
            "sync": "succeeded",
            "warehouse": "succeeded",
            "report:2026-01-03": "succeeded",
            "report:2026-01-04": "succeeded",
            "manifest": "succeeded"})
        self.assertIn(("report:2026-01-04", "requeued"),
                      self.recovery_events())
        manifest = json.loads(
            (self.batch_dir() / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual([report["date"] for report in manifest["reports"]],
                         ["2026-01-03", "2026-01-04"])

    def test_corrupt_published_report_fails_closed_and_is_preserved(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
            report = self.batch_dir() / "reports" / "2026-01-04.json"
            original = report.read_bytes()
            tampered = original.replace(b'"clues"', b'"clues" ', 1)
            self.assertNotEqual(tampered, original)
            try:
                report.write_bytes(tampered)
                with self.assertRaisesRegex(BatchError, "does not match"):
                    run_batch(self.plan_path, self.workdir, self.run_id)
                self.assertEqual(report.read_bytes(), tampered)
            finally:
                report.write_bytes(original)
            requests_before = len(fx.requests())
            result = run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
        self.assertTrue(result["complete"])

    def test_sync_failure_blocks_downstream_and_no_manifest(self):
        broken = page_xml([
            record_xml("oai:demo:A", "2026-01-01", title="A"),
            record_xml("oai:demo:B", "2025-12-31", title="outside window"),
        ])
        with self.fixture([{"match": dated_match("2026-01-01", "2026-01-05"),
                            "body": broken.decode()}]) as fx:
            self.write_plan(fx.endpoint)
            with self.assertRaisesRegex(BatchError, "outside frozen window"):
                run_batch(self.plan_path, self.workdir, self.run_id)
            states = self.stage_states()
            info = status_batch(self.workdir, self.run_id)
        self.assertEqual(states["sync"], "failed")
        for name in ("warehouse", "report:2026-01-03",
                     "report:2026-01-04", "manifest"):
            self.assertEqual(states[name], "blocked")
        sync_stage = next(stage for stage in info["stages"]
                          if stage["name"] == "sync")
        self.assertTrue(sync_stage["last_error"].startswith("WindowFailed:"))
        self.assertFalse(info["complete"])
        self.assertIsNone(info["delivery_manifest"])
        self.assertFalse((self.batch_dir() / "manifest.json").exists())
        self.assertFalse((self.batch_dir() / "warehouse.sqlite").exists())

    def test_retry_of_failed_sync_does_not_bypass_sync_failure_state(self):
        broken = page_xml([record_xml("oai:demo:A", "2025-12-31", title="A")])
        with self.fixture([{"match": dated_match("2026-01-01", "2026-01-05"),
                            "body": broken.decode()}]) as fx:
            self.write_plan(fx.endpoint)
            with self.assertRaises(BatchError):
                run_batch(self.plan_path, self.workdir, self.run_id)
            requests_before = len(fx.requests())
            # A second explicit run re-queues the stage, but the sync machine
            # itself refuses the failed window before any HTTP request.
            with self.assertRaisesRegex(BatchError, "failed state"):
                run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
            self.assertEqual(self.stage_states()["sync"], "failed")
            self.assertFalse((self.batch_dir() / "warehouse.sqlite").exists())


class IdentityAndDamageTests(BatchTestBase):
    def test_different_plan_for_same_run_id_is_rejected_before_any_stage(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
            requests_before = len(fx.requests())
            changed = self.write_plan(fx.endpoint, reports=("2026-01-03",),
                                      wh_end="2026-01-09")
            with self.assertRaisesRegex(BatchError, "frozen plan bytes"):
                run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
            self.assertIsNotNone(changed)
            # The original plan still resumes and skips everything.
            self.write_plan(fx.endpoint)
            result = run_batch(self.plan_path, self.workdir, self.run_id)
        self.assertTrue(result["complete"])
        self.assertEqual(len(fx.requests()), requests_before)

    def test_changed_taxonomy_file_is_rejected_for_existing_batch(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
            self.taxonomy.write_text(json.dumps(
                {"schema_version": 1, "nodes": [], "edges": [],
                 "bindings": []}), encoding="utf-8")
            with self.assertRaisesRegex(BatchError, "changed since"):
                run_batch(self.plan_path, self.workdir, self.run_id)

    def test_corrupt_artifact_is_preserved_and_reported(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
            catalog = self.batch_dir() / "catalog.sqlite"
            original = catalog.read_bytes()
            try:
                catalog.write_bytes(b"not a sqlite database")
                with self.assertRaisesRegex(BatchError,
                                            "preserved unchanged|verif"):
                    run_batch(self.plan_path, self.workdir, self.run_id)
                self.assertEqual(catalog.read_bytes(),
                                 b"not a sqlite database")
            finally:
                catalog.write_bytes(original)
            result = run_batch(self.plan_path, self.workdir, self.run_id)
        self.assertTrue(result["complete"])

    def test_damaged_batch_metadata_is_rejected_and_untouched(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
            metadata = self.batch_dir() / "batch.json"
            original = metadata.read_bytes()
            damaged = original.replace(b'"run_id"', b'"run_idd"', 1)
            self.assertNotEqual(damaged, original)
            try:
                metadata.write_bytes(damaged)
                with self.assertRaisesRegex(BatchError, "missing keys|damaged"):
                    run_batch(self.plan_path, self.workdir, self.run_id)
                self.assertEqual(metadata.read_bytes(), damaged)
                with self.assertRaisesRegex(BatchError, "missing keys|damaged"):
                    status_batch(self.workdir, self.run_id)
            finally:
                metadata.write_bytes(original)
            self.assertTrue(
                run_batch(self.plan_path, self.workdir, self.run_id)["complete"])

    def test_incomplete_batch_directory_without_metadata_is_untouched(self):
        self.batch_dir().mkdir(parents=True)
        marker = self.batch_dir() / "foreign.txt"
        marker.write_text("keep me", encoding="utf-8")
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            with self.assertRaisesRegex(BatchError, "batch.json|left untouched"):
                run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(marker.read_text(encoding="utf-8"), "keep me")
            self.assertEqual(fx.requests(), [])

    def test_invalid_run_ids_and_plan_fields_are_rejected(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            for bad in ("../escape", "/abs", "a/b", "", "x" * 65):
                with self.subTest(run_id=bad):
                    with self.assertRaises(BatchError):
                        run_batch(self.plan_path, self.workdir, bad)
            self.assertEqual(fx.requests(), [])
            bad_plan = self.write_plan(fx.endpoint)
            bad_plan["reports"] = ["2026-02-01"]
            self.plan_path.write_text(json.dumps(bad_plan), encoding="utf-8")
            with self.assertRaisesRegex(BatchError, "warehouse start"):
                run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertFalse(self.batch_dir().exists())

    def test_plan_duplicate_keys_and_wrong_types_are_rejected(self):
        self.plan_path.write_text(
            '{"schema_version":1,"sync":{"endpoint":"x"},'
            '"sync":{},"warehouse":{},"reports":[],"affinity":{}}',
            encoding="utf-8")
        with self.assertRaisesRegex(BatchError, "duplicate"):
            parse_plan(self.plan_path.read_bytes())
        with self.assertRaises(BatchError):
            parse_plan(b'{"schema_version":true}')

    def test_plan_with_symlinked_taxonomy_reads_frozen_bytes(self):
        target = self.dir / "real-taxonomy.json"
        target.write_bytes((DATA / "taxonomy_demo.json").read_bytes())
        self.taxonomy.unlink()
        self.taxonomy.symlink_to(target)
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            result = run_batch(self.plan_path, self.workdir, self.run_id)
        self.assertTrue(result["complete"])
        frozen = (self.batch_dir() / "inputs" / "taxonomy.json").read_bytes()
        self.assertEqual(frozen, target.read_bytes())

    def test_bundled_example_plan_parses_and_matches_warehouse_range(self):
        raw = (DATA / "batch_plan_example.json").read_bytes()
        plan = parse_plan(raw)
        self.assertEqual(plan["endpoint"], "http://127.0.0.1:8000/oai")
        self.assertEqual(plan["report_dates"], ["2026-01-03", "2026-01-04"])
        self.assertEqual(plan_stage_names(plan), [
            "sync", "warehouse", "report:2026-01-03", "report:2026-01-04",
            "manifest"])

    def test_stage_names_follow_report_dates(self):
        plan = parse_plan(json.dumps({
            "schema_version": 1,
            "sync": {"endpoint": "http://127.0.0.1:1/oai",
                     "start_date": "2026-01-01", "until": "2026-01-05",
                     "overlap_days": 2, "max_pages": 20},
            "warehouse": {"start": "2026-01-01", "end": "2026-01-08",
                          "taxonomy_file": "x"},
            "reports": ["2026-01-04", "2026-01-03"],
            "affinity": {"plan_file": "y"},
        }).encode("utf-8"))
        self.assertEqual(plan_stage_names(plan),
                         ["sync", "warehouse", "report:2026-01-04",
                          "report:2026-01-03", "manifest"])


class InputChangeTests(BatchTestBase):
    def test_affinity_plan_change_after_freeze_is_rejected_before_network(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            # Publication of a partial batch: stop after sync then change the
            # external affinity plan. The frozen copy must win and the change
            # must be refused before any further request.
            run_batch_cli(
                "run", "--plan", self.plan_path, "--workdir", self.workdir,
                "--run-id", self.run_id, "--json",
                env={ENV_BARRIER: "before-warehouse"}, expect_code=75)
            self.affinity_plan.write_text(json.dumps(
                {"schema_version": 1, "topics": ["leaf", "left"],
                 "alpha": "0.5", "min_shared": 1}), encoding="utf-8")
            requests_before = len(fx.requests())
            with self.assertRaisesRegex(BatchError, "changed since"):
                run_batch(self.plan_path, self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
            # Restoring the frozen bytes lets the same batch resume to
            # completion using the originally frozen plan.
            self.affinity_plan.write_bytes(
                (self.batch_dir() / "inputs" / "affinity_plan.json").read_bytes())
            result = run_batch(self.plan_path, self.workdir, self.run_id)
        self.assertTrue(result["complete"])
        published = json.loads(
            (self.batch_dir() / "reports" / "2026-01-03.json")
            .read_text(encoding="utf-8"))
        self.assertEqual(published["plan"]["selected_topics"][0]["topic_id"]
                         if published["plan"]["selected_topics"] else None,
                         "leaf")


class CliSurfaceTests(BatchTestBase):
    def test_status_of_missing_batch_exits_two_and_creates_nothing(self):
        proc = run_batch_cli(
            "status", "--workdir", self.workdir, "--run-id", "absent",
            "--json", expect_code=2)
        self.assertIn("not found", proc.stderr)
        self.assertFalse((self.workdir / "absent").exists())

    def test_run_cli_reports_incomplete_batch_with_nonzero_exit(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            proc = run_batch_cli(
                "run", "--plan", self.plan_path, "--workdir", self.workdir,
                "--run-id", self.run_id, "--json",
                env={ENV_FAIL_STAGE: "warehouse"}, expect_code=3)
            summary = json.loads(proc.stdout)
            self.assertFalse(summary["complete"])
            self.assertIn("injected stage failure", proc.stderr)
            manifest_missing = not (
                self.batch_dir() / "manifest.json").exists()
        self.assertTrue(manifest_missing)

    def test_status_detects_manifest_without_full_success(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
            db = sqlite3.connect(self.batch_dir() / "state.sqlite")
            try:
                with db:
                    db.execute("UPDATE stage SET state='failed' "
                               "WHERE name='sync'")
            finally:
                db.close()
            with self.assertRaisesRegex(BatchError, "manifest exists"):
                status_batch(self.workdir, self.run_id)
            # The manifest bytes were not touched by the failed status read.
            self.assertTrue((self.batch_dir() / "manifest.json").exists())

    def test_status_rejects_tampered_frozen_input_copy(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch(self.plan_path, self.workdir, self.run_id)
            frozen = self.batch_dir() / "inputs" / "taxonomy.json"
            original = frozen.read_bytes()
            try:
                frozen.write_bytes(original + b" ")
                with self.assertRaisesRegex(BatchError, "frozen input"):
                    status_batch(self.workdir, self.run_id)
                with self.assertRaisesRegex(BatchError, "frozen input"):
                    run_batch(self.plan_path, self.workdir, self.run_id)
                self.assertEqual(frozen.read_bytes(), original + b" ")
            finally:
                frozen.write_bytes(original)
            self.assertTrue(
                run_batch(self.plan_path, self.workdir, self.run_id)["complete"])

    def test_demo_runs_all_three_stages_on_a_real_loopback_source(self):
        workdir = self.dir / "demo"
        proc = subprocess.run(
            [sys.executable, "-E", "-m", "catalog_harvest.batch_demo",
             "--workdir", str(workdir), "--run-id", "demo"],
            cwd=ROOT, capture_output=True, text=True,
            env=dict(os.environ, PYTHONPATH=str(ROOT)))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        batch_dir = workdir / "batches" / "demo"
        manifest = json.loads(
            (batch_dir / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["stage_order"],
                         ["sync", "warehouse", "report:2026-01-03",
                          "report:2026-01-04", "manifest"])
        self.assertEqual(manifest["sync"]["watermark"], "2026-01-05")
        self.assertEqual(manifest["sync"]["pages"], 2)
        self.assertEqual(manifest["warehouse"]["versions"], 4)


class ContentionTests(BatchTestBase):
    def _hold_lock(self):
        """Hold the batch lock of an already-created batch until released."""
        lock_path = self.batch_dir() / "state.lock"
        self.assertTrue(lock_path.exists())
        code = (
            "import fcntl,os,sys\n"
            f"fd=os.open({str(lock_path)!r}, os.O_RDWR|os.O_CREAT)\n"
            "fcntl.flock(fd, fcntl.LOCK_EX)\n"
            "sys.stdout.write('held\\n');sys.stdout.flush()\n"
            "sys.stdin.read(1)\n")
        holder = subprocess.Popen(
            [sys.executable, "-E", "-c", code], cwd=ROOT,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            env=dict(os.environ, PYTHONPATH=str(ROOT)))
        self.assertEqual(holder.stdout.readline().strip(), "held")
        return holder

    def _release(self, holder):
        holder.stdin.write("x")
        holder.stdin.flush()
        holder.wait(timeout=10)
        holder.stdin.close()
        holder.stdout.close()

    def test_concurrent_run_of_the_same_batch_is_refused_immediately(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            # First run stops before the warehouse while holding the batch
            # directory and lock-free ledger; the lock itself is then taken by
            # the helper to simulate a live competing writer.
            run_batch_cli(
                "run", "--plan", self.plan_path, "--workdir", self.workdir,
                "--run-id", self.run_id, "--json",
                env={ENV_BARRIER: "before-warehouse"}, expect_code=75)
            holder = self._hold_lock()
            try:
                requests_before = len(fx.requests())
                ledger = self.batch_dir() / "state.sqlite"
                ledger_before = hashlib.sha256(ledger.read_bytes()).hexdigest()
                proc = run_batch_cli(
                    "run", "--plan", self.plan_path, "--workdir", self.workdir,
                    "--run-id", self.run_id, "--json", expect_code=2)
                self.assertIn("lock is held", proc.stderr)
                self.assertIn("refused", proc.stderr)
                # Refused before any request or ledger change.
                self.assertEqual(len(fx.requests()), requests_before)
                self.assertEqual(
                    hashlib.sha256(ledger.read_bytes()).hexdigest(),
                    ledger_before)
            finally:
                self._release(holder)
            result = run_batch(self.plan_path, self.workdir, self.run_id)
        self.assertTrue(result["complete"])
        self.assertEqual(len(fx.requests()), 2)

    def test_competing_run_after_creation_is_also_refused(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch_cli(
                "run", "--plan", self.plan_path, "--workdir", self.workdir,
                "--run-id", self.run_id, "--json",
                env={ENV_BARRIER: "before-warehouse"}, expect_code=75)
            holder = self._hold_lock()
            try:
                requests_before = len(fx.requests())
                proc = run_batch_cli(
                    "run", "--plan", self.plan_path, "--workdir", self.workdir,
                    "--run-id", self.run_id, "--json", expect_code=2)
                self.assertIn("lock is held", proc.stderr)
                self.assertEqual(len(fx.requests()), requests_before)
            finally:
                self._release(holder)
            self.assertTrue(
                run_batch(self.plan_path, self.workdir, self.run_id)["complete"])

    def test_status_is_read_only_and_does_not_take_the_lock(self):
        with self.fixture(window_pages()) as fx:
            self.write_plan(fx.endpoint)
            run_batch_cli(
                "run", "--plan", self.plan_path, "--workdir", self.workdir,
                "--run-id", self.run_id, "--json",
                env={ENV_BARRIER: "before-warehouse"}, expect_code=75)
            ledger = self.batch_dir() / "state.sqlite"
            before = hashlib.sha256(ledger.read_bytes()).hexdigest()
            holder = self._hold_lock()
            try:
                info = status_batch(self.workdir, self.run_id)
            finally:
                self._release(holder)
            self.assertEqual(hashlib.sha256(ledger.read_bytes()).hexdigest(),
                             before)
            self.assertEqual(
                {stage["name"]: stage["state"] for stage in info["stages"]},
                {"sync": "succeeded", "warehouse": "running",
                 "report:2026-01-03": "pending",
                 "report:2026-01-04": "pending", "manifest": "pending"})
            # Read-only status created no warehouse and issued no request.
            self.assertFalse((self.batch_dir() / "warehouse.sqlite").exists())
            requests_before = len(fx.requests())
            status_batch(self.workdir, self.run_id)
            self.assertEqual(len(fx.requests()), requests_before)
            result = run_batch(self.plan_path, self.workdir, self.run_id)
        self.assertTrue(result["complete"])
        self.assertTrue(status_batch(self.workdir, self.run_id)["complete"])


if __name__ == "__main__":
    unittest.main()
