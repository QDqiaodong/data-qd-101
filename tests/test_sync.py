"""End-to-end tests for resumable incremental date-window sync.

These tests drive a real loopback HTTP fixture and, for the recovery cases,
real ``python -m catalog_harvest.sync`` CLI subprocesses. Synchronization uses
explicit test hooks (process exit after a checkpoint commit, shutdown
endpoint) rather than timing-based sleeps.
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from catalog_harvest.harvest import harvest
from catalog_harvest.transport import page_fetcher
from catalog_harvest.demo import fixture_server
from catalog_harvest.sync import (run_sync, status, discard, WindowFailed,
                                  SyncError)
from tests.oai_fixtures import (FixtureProcess, run_cli, cli_json, record_xml,
                                page_xml, error_xml)

DATA = Path(__file__).resolve().parents[1] / "data"


def config(pages, fallback=None):
    return {"pages": pages,
            "fallback": fallback or {"status": 400, "body": "unexpected request"}}


def dated_match(from_date, until_date):
    return {"verb": ["ListRecords"], "metadataPrefix": ["oai_dc"],
            "from": [from_date], "until": [until_date]}


def token_match(token):
    return {"verb": ["ListRecords"], "resumptionToken": [token]}


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.db = self.dir / "sync.sqlite"

    def fixture(self, pages, fallback=None):
        proc = FixtureProcess(self.dir, config(pages, fallback))
        self.addCleanup(proc.shutdown)
        return proc

    def query(self, sql, params=()):
        with sqlite3.connect(self.db) as db:
            return db.execute(sql, params).fetchall()

    # -- acceptance: two window synchronization -----------------------------
    def test_two_windows_watermark_overlap_and_tombstone_rules(self):
        window1_p1 = page_xml([
            record_xml("oai:demo:A", "2026-01-01", title="River flow & sediment",
                       subjects=["hydrology"]),
            record_xml("oai:demo:B", "2026-01-01", title="Retired survey")],
            token=" opaque/token 1 ")
        window1_p2 = page_xml([
            record_xml("oai:demo:A", "2026-01-01", title="River flow & sediment",
                       subjects=["hydrology"]),
            record_xml("oai:demo:B", "2026-01-02", deleted=True)])
        # Window 2: late C/Jan2, new A/Jan4, repeated B/Jan2 tombstone.
        window2_p1 = page_xml([
            record_xml("oai:demo:C", "2026-01-02", title="Late catch",
                       subjects=["water"]),
            record_xml("oai:demo:A", "2026-01-04", title="River flow & sediment",
                       subjects=["hydrology"])],
            token="opaque/token 2")
        window2_p2 = page_xml([
            record_xml("oai:demo:B", "2026-01-02", deleted=True)])
        # Window 3 deliberately omits C: overlap re-harvest must not delete it.
        window3 = page_xml([record_xml("oai:demo:A", "2026-01-04",
                                       title="River flow & sediment",
                                       subjects=["hydrology"])])
        pages = [
            {"match": dated_match("2026-01-01", "2026-01-03"), "body": window1_p1.decode()},
            {"match": token_match(" opaque/token 1 "), "body": window1_p2.decode()},
            {"match": dated_match("2026-01-02", "2026-01-05"), "body": window2_p1.decode()},
            {"match": token_match("opaque/token 2"), "body": window2_p2.decode()},
            {"match": dated_match("2026-01-04", "2026-01-06"), "body": window3.decode()},
        ]
        with self.fixture(pages) as fx:
            r1 = run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                          until="2026-01-03")
            # Watermark is the window upper bound even though max record date is Jan 2.
            self.assertEqual(r1["watermark"], "2026-01-03")
            self.assertEqual((r1["pages"], r1["records_seen"],
                              r1["inserted_revisions"]), (2, 4, 3))
            requests = fx.requests()
            self.assertEqual(requests[0], dated_match("2026-01-01", "2026-01-03"))
            # Opaque token including surrounding whitespace is preserved verbatim.
            self.assertEqual(requests[1], token_match(" opaque/token 1 "))
            self.assertNotIn("from", requests[1])
            self.assertNotIn("metadataPrefix", requests[1])

            r2 = run_sync(fx.endpoint, self.db, until="2026-01-05", overlap_days=2)
            # W=Jan3, overlap-days=2 -> lower bound Jan 2.
            self.assertEqual(fx.requests()[2], dated_match("2026-01-02", "2026-01-05"))
            self.assertEqual(fx.requests()[3], token_match("opaque/token 2"))

            self.assertEqual(r2["inserted_revisions"], 2)
            self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 5)
            current = dict(self.query("SELECT identifier, deleted FROM current_records"))
            self.assertEqual(current, {"oai:demo:A": 0, "oai:demo:C": 0,
                                       "oai:demo:B": 1})
            self.assertEqual(self.query(
                "SELECT count(*) FROM current_records WHERE deleted=0")[0][0], 2)
            self.assertEqual(self.query(
                "SELECT count(*) FROM current_records WHERE deleted=1")[0][0], 1)
            # Repeated B tombstone did not create a duplicate revision.
            self.assertEqual(self.query(
                "SELECT count(*) FROM revisions WHERE identifier='oai:demo:B'")[0][0], 2)
            # Newest tombstone still wins; B is not resurrected by the older active row.
            self.assertEqual(self.query(
                "SELECT deleted FROM current_records WHERE identifier='oai:demo:B'")[0][0], 1)
            self.assertEqual(status(self.db)["watermark"], "2026-01-05")

            # Window 3 deliberately omits C: overlap re-harvest must not delete it.
            run_sync(fx.endpoint, self.db, until="2026-01-06", overlap_days=2)
        self.assertEqual(self.query(
            "SELECT count(*) FROM revisions WHERE identifier='oai:demo:C'")[0][0], 1)
        self.assertEqual(self.query(
            "SELECT deleted FROM current_records WHERE identifier='oai:demo:C'")[0][0], 0)

    # -- cross-process recovery via real CLI subprocesses --------------------
    def test_cli_cross_process_resume_from_next_token(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                      token="resume-token-xyz")
        p2 = page_xml([record_xml("oai:demo:B", "2026-01-02", title="B1")],
                      token="resume-token-final")
        p3 = page_xml([record_xml("oai:demo:A", "2026-01-02", deleted=True)])
        pages = [
            {"match": dated_match("2026-01-01", "2026-01-03"), "body": p1.decode()},
            {"match": token_match("resume-token-xyz"), "body": p2.decode()},
            {"match": token_match("resume-token-final"), "body": p3.decode()},
        ]
        with self.fixture(pages) as fx:
            # Deterministic barrier: the process exits only after page 1's
            # checkpoint transaction has committed.
            run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                    "--start-date", "2026-01-01", "--until", "2026-01-03",
                    "--json", env={"CATALOG_HARVEST_EXIT_AFTER_PAGE": "1"},
                    expect_code=7)
            info = status(self.db)
            self.assertEqual(info["unfinished_window"]["pages_confirmed"], 1)
            self.assertEqual(info["unfinished_window"]["next_token"],
                             "resume-token-xyz")
            # Official tables remain untouched until full-window publish.
            self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 0)
            self.assertEqual(self.query("SELECT count(*) FROM pages")[0][0], 0)

            result = cli_json("run", "--endpoint", fx.endpoint, "--db", self.db,
                              "--until", "2026-01-03", "--json")
            requests = fx.requests()
        # Page 1 was never re-requested; resume started directly from the token.
        self.assertEqual([q for q in requests if "from" in q],
                         [dated_match("2026-01-01", "2026-01-03")])
        self.assertEqual([q.get("resumptionToken") for q in requests
                          if "resumptionToken" in q],
                         [["resume-token-xyz"], ["resume-token-final"]])
        self.assertEqual(result["recovered_pages"], 1)
        self.assertEqual(result["pages_fetched_this_run"], 2)
        self.assertEqual(result["pages"], 3)
        self.assertEqual(status(self.db)["watermark"], "2026-01-03")
        self.assertEqual(self.query(
            "SELECT page_sha256 FROM revisions WHERE identifier='oai:demo:B'")[0][0],
                         self.query(
                             "SELECT sha256 FROM sync_stage_pages WHERE page_seq=2")[0][0])

    def test_cli_resume_after_terminal_page_persisted_sends_no_http(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                      token="only-token")
        p2 = page_xml([record_xml("oai:demo:B", "2026-01-01", title="B1")])
        pages = [
            {"match": dated_match("2026-01-01", "2026-01-02"), "body": p1.decode()},
            {"match": token_match("only-token"), "body": p2.decode()},
        ]
        with self.fixture(pages) as fx:
            run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                    "--start-date", "2026-01-01", "--until", "2026-01-02",
                    "--json", env={"CATALOG_HARVEST_EXIT_BEFORE_PUBLISH": "1"},
                    expect_code=8)
            self.assertEqual(status(self.db)["unfinished_window"]["pages_confirmed"], 2)
            requests_before = len(fx.requests())
            result = cli_json("run", "--endpoint", fx.endpoint, "--db", self.db,
                              "--until", "2026-01-02", "--json")
            self.assertEqual(len(fx.requests()), requests_before)
        self.assertEqual(result["http_requests_this_run"], 0)
        self.assertEqual(result["recovered_pages"], 2)
        self.assertEqual(result["pages_fetched_this_run"], 0)
        self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 2)

    # -- badResumptionToken restart budget -----------------------------------
    def test_bad_token_restart_discards_abandoned_traversal(self):
        ghost = record_xml("oai:demo:GHOST", "2026-01-01", title="must disappear")
        p1 = page_xml([ghost], token="stale-token")
        bad = error_xml("badResumptionToken", "token expired")
        restart_p1 = page_xml([
            record_xml("oai:demo:A", "2026-01-01", title="A1")], token="fresh-token")
        restart_p2 = page_xml([
            record_xml("oai:demo:A", "2026-01-02", deleted=True)])
        pages = [
            # First dated request (request #0) returns the abandoned ghost page.
            {"nth": [0], "match": dated_match("2026-01-01", "2026-01-03"),
             "body": p1.decode()},
            # Replayed dated request after restart (#2) must not contain the ghost.
            {"nth": [2], "match": dated_match("2026-01-01", "2026-01-03"),
             "body": restart_p1.decode()},
            {"match": token_match("stale-token"), "body": bad.decode()},
            {"match": token_match("fresh-token"), "body": restart_p2.decode()},
        ]
        with self.fixture(pages) as fx:
            result = run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                              until="2026-01-03")
            requests = fx.requests()
        # Same frozen from/until first request is replayed after the restart.
        self.assertEqual([q for q in requests if "from" in q],
                         [dated_match("2026-01-01", "2026-01-03"),
                          dated_match("2026-01-01", "2026-01-03")])
        self.assertEqual(result["traversal_restarts"], 1)
        self.assertEqual(self.query(
            "SELECT count(*) FROM revisions WHERE identifier='oai:demo:GHOST'")[0][0], 0)
        self.assertEqual(self.query(
            "SELECT count(*) FROM pages")[0][0], 2)
        # Abandoned traversal rows are gone; only traversal 2 remains.
        self.assertEqual(self.query(
            "SELECT DISTINCT traversal FROM sync_stage_pages")[0][0], 2)
        self.assertEqual(self.query(
            "SELECT count(*) FROM sync_stage_pages")[0][0], 2)

    def test_restart_budget_is_persisted_across_processes(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                      token="stale-token")
        # After the one allowed restart the same token fails again.
        restart_p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                              token="stale-token")
        bad = error_xml("badResumptionToken", "token expired")
        pages = [
            {"nth": [0], "match": dated_match("2026-01-01", "2026-01-03"),
             "body": p1.decode()},
            {"nth": [2], "match": dated_match("2026-01-01", "2026-01-03"),
             "body": restart_p1.decode()},
            {"match": token_match("stale-token"), "body": bad.decode()},
        ]
        with self.fixture(pages) as fx:
            # Process 1 consumes the persisted restart budget, then stops after
            # the replacement traversal's first page is checkpointed.
            run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                    "--start-date", "2026-01-01", "--until", "2026-01-03",
                    "--json",
                    env={"CATALOG_HARVEST_EXIT_AFTER_RESTART_PAGE": "1"},
                    expect_code=7)
            info = status(self.db)
            self.assertEqual(info["unfinished_window"]["restarts_used"], 1)
            self.assertEqual(info["unfinished_window"]["traversal"], 2)
            self.assertEqual(info["unfinished_window"]["next_token"], "stale-token")
            # Process 2 resumes from the stored token; it fails again and the
            # budget must not have reset across the process boundary.
            proc = run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                           "--until", "2026-01-03", "--json", expect_code=2)
        self.assertIn("restart budget exhausted", proc.stderr)
        self.assertEqual(status(self.db)["unfinished_window"]["state"], "failed")
        # Further run cannot silently skip the failure (same bound endpoint;
        # the refusal happens before any HTTP request).
        before = len(fx.requests())
        with self.assertRaisesRegex(SyncError, "failed state"):
            run_sync(fx.endpoint, self.db, until="2026-01-03")
        self.assertEqual(len(fx.requests()), before)
        # discard clears it; no facts were ever published and no watermark set.
        out = discard(self.db)
        self.assertTrue(out["discarded"])
        self.assertIsNone(out["watermark_retained"])
        self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 0)

    def test_restarted_traversal_can_itself_be_interrupted_and_resumed(self):
        # Abandoned traversal: ghost page + stale token.
        ghost = page_xml([record_xml("oai:demo:GHOST", "2026-01-01",
                                     title="gone")], token="stale-token")
        bad = error_xml("badResumptionToken", "expired")
        # Replacement traversal spans two pages so it can be interrupted.
        restart_p1 = page_xml([
            record_xml("oai:demo:A", "2026-01-01", title="A1")], token="fresh")
        restart_p2 = page_xml([
            record_xml("oai:demo:B", "2026-01-02", title="B1")])
        pages = [
            {"nth": [0], "match": dated_match("2026-01-01", "2026-01-03"),
             "body": ghost.decode()},
            {"nth": [2], "match": dated_match("2026-01-01", "2026-01-03"),
             "body": restart_p1.decode()},
            {"match": token_match("stale-token"), "body": bad.decode()},
            {"match": token_match("fresh"), "body": restart_p2.decode()},
        ]
        with self.fixture(pages) as fx:
            # Process 1: restart fires, replacement page 1 checkpoints, then exit.
            run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                    "--start-date", "2026-01-01", "--until", "2026-01-03",
                    "--json",
                    env={"CATALOG_HARVEST_EXIT_AFTER_RESTART_PAGE": "1"},
                    expect_code=7)
            info = status(self.db)
            self.assertEqual((info["unfinished_window"]["traversal"],
                              info["unfinished_window"]["restarts_used"],
                              info["unfinished_window"]["pages_confirmed"],
                              info["unfinished_window"]["next_token"]),
                             (2, 1, 1, "fresh"))
            # Process 2 resumes the replacement traversal from 'fresh' without
            # re-requesting its page 1 and without spending more restart budget.
            result = cli_json("run", "--endpoint", fx.endpoint, "--db", self.db,
                              "--until", "2026-01-03", "--json")
            dated_requests = [q for q in fx.requests() if "from" in q]
        self.assertEqual(len(dated_requests), 2)  # original + one replay only
        self.assertEqual(result["recovered_pages"], 1)
        # Restart happened in process 1; its budget stays persisted in the window.
        self.assertEqual(self.query(
            "SELECT restarts_used, traversal FROM sync_window")[0], (1, 2))
        self.assertEqual(self.query(
            "SELECT count(*) FROM revisions WHERE identifier='oai:demo:GHOST'")[0][0], 0)
        self.assertEqual(status(self.db)["watermark"], "2026-01-03")

    # -- explicit OAI error branches -----------------------------------------
    def test_first_request_no_records_match_advances_watermark(self):
        empty = error_xml("noRecordsMatch", "nothing in range")
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-02"),
                 "body": empty.decode()}]) as fx:
            result = run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                              until="2026-01-02")
        self.assertTrue(result["empty_window"])
        self.assertEqual(result["watermark"], "2026-01-02")
        self.assertEqual(status(self.db)["watermark"], "2026-01-02")
        self.assertEqual(self.query("SELECT count(*) FROM sync_stage_pages")[0][0], 0)

    def test_continuation_no_records_match_fails_without_advancing(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                      token="tok")
        bad = error_xml("noRecordsMatch", "unexpected")
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-03"), "body": p1.decode()},
                {"match": token_match("tok"), "body": bad.decode()}]) as fx:
            with self.assertRaisesRegex(WindowFailed, "noRecordsMatch"):
                run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                         until="2026-01-03")
        self.assertIsNone(status(self.db)["watermark"])
        self.assertEqual(status(self.db)["unfinished_window"]["state"], "failed")

    def test_bad_token_on_first_request_fails(self):
        bad = error_xml("badResumptionToken", "nonsense")
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-03"),
                 "body": bad.decode()}]) as fx:
            with self.assertRaisesRegex(WindowFailed, "dated first request"):
                run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                         until="2026-01-03")
        self.assertEqual(status(self.db)["unfinished_window"]["state"], "failed")

    # -- hard failures never advance the watermark ---------------------------
    def _seed_failed_window(self, pages, matcher):
        with self.fixture(pages) as fx:
            with self.assertRaises(WindowFailed):
                run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                         until="2026-01-03")
            return fx

    def test_out_of_window_datestamp_fails(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-04", title="future")])
        self._seed_failed_window(
            [{"match": dated_match("2026-01-01", "2026-01-03"),
              "body": p1.decode()}], None)
        self.assertIsNone(status(self.db)["watermark"])
        self.assertIn("outside frozen window",
                      status(self.db)["unfinished_window"]["last_error"])
        self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 0)

    def test_token_cycle_fails(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                      token="cycle")
        self._seed_failed_window([
            {"match": dated_match("2026-01-01", "2026-01-03"), "body": p1.decode()},
            {"match": token_match("cycle"), "body": p1.decode()}], None)
        self.assertEqual(status(self.db)["unfinished_window"]["state"], "failed")
        self.assertIn("cycle", status(self.db)["unfinished_window"]["last_error"])

    def test_page_budget_exhausted_fails(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                      token="more")
        p2 = page_xml([record_xml("oai:demo:B", "2026-01-01", title="B1")],
                      token="more2")
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-03"),
                 "body": p1.decode()},
                {"match": token_match("more"), "body": p2.decode()}]) as fx:
            with self.assertRaisesRegex(WindowFailed, "page limit"):
                run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                         until="2026-01-03", max_pages=1)
        win = status(self.db)["unfinished_window"]
        self.assertEqual((win["state"], win["pages_confirmed"]), ("failed", 1))

    def test_same_identity_payload_conflict_fails(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="first")],
                      token="tok")
        p2 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="CONFLICT")])
        self._seed_failed_window([
            {"match": dated_match("2026-01-01", "2026-01-03"), "body": p1.decode()},
            {"match": token_match("tok"), "body": p2.decode()}], None)
        self.assertIn("conflicting revision",
                      status(self.db)["unfinished_window"]["last_error"])
        self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 0)

    def test_invalid_xml_fails_but_checkpoint_before_it_survives(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                      token="tok")
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-03"),
                 "body": p1.decode()},
                {"match": token_match("tok"), "body": "<broken"}],
                fallback={"status": 400, "body": "x"}) as fx:
            with self.assertRaises(WindowFailed):
                run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                         until="2026-01-03")
        self.assertEqual(status(self.db)["unfinished_window"]["pages_confirmed"], 1)
        self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 0)

    # -- publish transaction rollback and retry ------------------------------
    def test_publish_failure_rolls_back_together_and_can_republish(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")])
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-02"),
                 "body": p1.decode()}]) as fx:
            env = {"CATALOG_HARVEST_PUBLISH_FAIL": "1"}
            proc = run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                           "--start-date", "2026-01-01", "--until", "2026-01-02",
                           "--json", env=env, expect_code=2)
            self.assertIn("injected publish failure", proc.stderr)
            info = status(self.db)
            # Not a hard content failure: window stays resumable, not 'failed'.
            self.assertEqual(info["unfinished_window"]["state"], "active")
            self.assertIsNone(info["watermark"])
            self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 0)
            # Staging is intact and a new process republishes without HTTP.
            result = cli_json("run", "--endpoint", fx.endpoint, "--db", self.db,
                              "--until", "2026-01-02", "--json")
        self.assertEqual(result["watermark"], "2026-01-02")
        self.assertEqual(result["http_requests_this_run"], 0)
        self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 1)

    # -- binding and parameter rejection rules -------------------------------
    def test_nonempty_legacy_database_is_rejected_before_http(self):
        legacy = self.dir / "legacy.sqlite"
        with fixture_server(DATA) as endpoint:
            harvest(page_fetcher(endpoint), legacy)
        with self.fixture([]) as fx:
            with self.assertRaisesRegex(SyncError, "full-harvest pages/revisions"):
                run_sync(fx.endpoint, legacy, start_date="2026-01-01",
                         until="2026-01-03")
            self.assertEqual(fx.requests(), [])
        with sqlite3.connect(legacy) as db:
            # No binding metadata was written; legacy facts are untouched.
            self.assertIsNone(db.execute(
                "SELECT value FROM sync_meta WHERE key='endpoint'").fetchone())
            self.assertEqual(db.execute("SELECT count(*) FROM revisions").fetchone()[0], 3)

    def test_endpoint_and_start_date_are_fixed(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")])
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-02"),
                 "body": p1.decode()}]) as fx:
            run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                     until="2026-01-02")
            with self.assertRaisesRegex(SyncError, "bound to endpoint"):
                run_sync("http://127.0.0.1:9/oai", self.db, until="2026-01-03")
            with self.assertRaisesRegex(SyncError, "fixed start date"):
                run_sync(fx.endpoint, self.db, start_date="2026-01-02",
                         until="2026-01-03")

    def test_frozen_parameters_must_match_on_resume(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")],
                      token="tok")
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-03"),
                 "body": p1.decode()}]) as fx:
            run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                    "--start-date", "2026-01-01", "--until", "2026-01-03",
                    "--json", env={"CATALOG_HARVEST_EXIT_AFTER_PAGE": "1"},
                    expect_code=7)
            # Different until while an unfinished window exists: rejected pre-HTTP.
            before = len(fx.requests())
            proc = run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                           "--until", "2026-01-04", "--json", expect_code=2)
            self.assertEqual(len(fx.requests()), before)
        self.assertIn("frozen parameters", proc.stderr)

    def test_full_harvest_rejects_managed_database(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")])
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-02"),
                 "body": p1.decode()}]) as fx:
            run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                     until="2026-01-02")
            with self.assertRaisesRegex(ValueError, "incremental sync"):
                harvest(lambda marker: b"", self.db)

    def test_until_validation_and_equal_until_reharvest(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")])
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-02"),
                 "body": p1.decode()}]) as fx:
            with self.assertRaises(SyncError):
                run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                         until="01/02/2026")
            run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                     until="2026-01-02")
            with self.assertRaisesRegex(SyncError, "earlier than the existing"):
                run_sync(fx.endpoint, self.db, until="2026-01-01")
            # until == W is allowed and re-harvests the overlap window.
            again = run_sync(fx.endpoint, self.db, until="2026-01-02")
        self.assertEqual(again["inserted_revisions"], 0)
        self.assertEqual(again["watermark"], "2026-01-02")

    def test_discard_keeps_official_facts_and_watermark(self):
        p1 = page_xml([record_xml("oai:demo:A", "2026-01-01", title="A1")])
        p2 = page_xml([record_xml("oai:demo:B", "2026-01-03", title="B1")])
        p2_dedup = page_xml([
            record_xml("oai:demo:A", "2026-01-01", title="A1"),
            record_xml("oai:demo:B", "2026-01-03", title="B1")])
        with self.fixture([
                {"match": dated_match("2026-01-01", "2026-01-03"),
                 "body": p1.decode()},
                {"match": dated_match("2026-01-02", "2026-01-04"),
                 "body": p2.decode()},
                {"nth": [2], "match": dated_match("2026-01-02", "2026-01-04"),
                 "body": p2_dedup.decode()}]) as fx:
            run_sync(fx.endpoint, self.db, start_date="2026-01-01",
                     until="2026-01-03")
            # Open a second window then discard it.
            run_cli("run", "--endpoint", fx.endpoint, "--db", self.db,
                    "--until", "2026-01-04", "--json",
                    env={"CATALOG_HARVEST_EXIT_AFTER_FINAL_PAGE": "1"},
                    expect_code=9)
            out = discard(self.db)
            self.assertTrue(out["discarded"])
            self.assertEqual(out["watermark_retained"], "2026-01-03")
            self.assertEqual(status(self.db)["watermark"], "2026-01-03")
            self.assertEqual(self.query("SELECT identifier FROM revisions"),
                             [("oai:demo:A",)])
            self.assertEqual(self.query("SELECT count(*) FROM sync_window")[0][0], 1)
            # Re-collect after discard; old facts remain and A de-duplicates.
            result = run_sync(fx.endpoint, self.db, until="2026-01-04")
        self.assertEqual(result["inserted_revisions"], 1)
        self.assertEqual(self.query("SELECT count(*) FROM revisions")[0][0], 2)


if __name__ == "__main__":
    unittest.main()
