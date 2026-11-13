"""Deterministic loopback demonstration of the three-stage batch pipeline.

Starts an in-process synthetic OAI-PMH source on 127.0.0.1, freezes a batch
plan, then runs sync -> warehouse -> two dated affinity reports -> delivery
manifest through :mod:`catalog_harvest.batch`. No external service is
contacted and every record is original synthetic data.

    python -m catalog_harvest.batch_demo --workdir run/batch-demo
"""
import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape

from .batch import run_batch, status_batch

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DCO_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"


def record(identifier, datestamp, deleted=False, title=None, subjects=()):
    attr = ' status="deleted"' if deleted else ""
    header = (f"<header{attr}><identifier>{escape(identifier)}</identifier>"
              f"<datestamp>{datestamp}</datestamp></header>")
    if deleted:
        return f"<record>{header}</record>"
    subjects_xml = "".join(
        f"<dc:subject>{escape(s)}</dc:subject>" for s in subjects)
    metadata = (f'<metadata><oai_dc:dc xmlns:oai_dc="{DCO_NS}" '
                f'xmlns:dc="{DC_NS}"><dc:title>{escape(title)}</dc:title>'
                f"{subjects_xml}</oai_dc:dc></metadata>")
    return f"<record>{header}{metadata}</record>"


def page(records, token=None):
    token_xml = (f"<resumptionToken>{escape(token)}</resumptionToken>"
                 if token is not None else "")
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            f'<OAI-PMH xmlns="{OAI_NS}" xmlns:oai_dc="{DCO_NS}" '
            f'xmlns:dc="{DC_NS}"><ListRecords>{"".join(records)}'
            f"{token_xml}</ListRecords></OAI-PMH>")


def dated(from_date, until_date):
    return {"verb": ["ListRecords"], "metadataPrefix": ["oai_dc"],
            "from": [from_date], "until": [until_date]}


def with_token(token):
    return {"verb": ["ListRecords"], "resumptionToken": [token]}


def scenario_pages():
    """Two pages in one frozen window; subjects match data/taxonomy_demo.json."""
    first = page([
        record("oai:demo:A", "2026-01-01", title="River flow & sediment",
               subjects=["hydro"]),
        record("oai:demo:B", "2026-01-02", title="Water survey",
               subjects=["water"]),
    ], token=" page/two ")
    second = page([
        record("oai:demo:C", "2026-01-03", title="Catchment review",
               subjects=["hydro", "water"]),
        record("oai:demo:B", "2026-01-04", deleted=True),
    ])
    return [
        {"match": dated("2026-01-01", "2026-01-05"), "body": first},
        {"match": with_token(" page/two "), "body": second},
    ]


class _Source:
    def __init__(self, pages):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                query = parse_qs(urlsplit(self.path).query,
                                 keep_blank_values=True)
                answer = next(
                    (item for item in pages if query == item["match"]), None)
                if answer is None:
                    self.send_error(400, "unexpected request")
                    return
                body = answer["body"].encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/xml; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.worker = threading.Thread(
            target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server.server_port}/oai"

    def __enter__(self):
        self.worker.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.worker.join()
        self.server.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", default="run/batch-demo")
    parser.add_argument("--run-id", default="demo")
    args = parser.parse_args()
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    existing = workdir / "batches" / args.run_id
    if existing.exists():
        parser.exit(
            2, f"batch {args.run_id!r} already exists in {workdir}; this "
               "demo freezes the loopback endpoint into its plan, so use a "
               "new --workdir for a clean replay\n")
    data = Path(__file__).resolve().parents[1] / "data"
    plan = {
        "schema_version": 1,
        "sync": {"endpoint": None, "start_date": "2026-01-01",
                 "until": "2026-01-05", "overlap_days": 2, "max_pages": 20},
        "warehouse": {"start": "2026-01-01", "end": "2026-01-08",
                      "taxonomy_file": str(data / "taxonomy_demo.json")},
        "reports": ["2026-01-03", "2026-01-04"],
        "affinity": {"plan_file": str(data / "affinity_plan_example.json")},
    }
    with _Source(scenario_pages()) as source:
        plan["sync"]["endpoint"] = source.endpoint
        plan_path = workdir / "plan.json"
        plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")
        summary = run_batch(plan_path, workdir / "batches", args.run_id)
    print("RUN SUMMARY")
    print(json.dumps(summary, sort_keys=True, indent=2))
    print("\nSTATUS")
    info = status_batch(workdir / "batches", args.run_id)
    print(json.dumps(
        {"complete": info["complete"],
         "delivery_manifest": info["delivery_manifest"]},
        sort_keys=True, indent=2))
    print("\nMANIFEST")
    print((workdir / "batches" / args.run_id / "manifest.json").read_text(
        encoding="utf-8"))


if __name__ == "__main__":
    main()
