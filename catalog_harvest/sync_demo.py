"""Deterministic loopback demo of resumable incremental date-window sync.

Runs an in-process synthetic OAI-PMH source on 127.0.0.1 and drives two
windows through ``catalog_harvest.sync`` with real HTTP requests. No external
service is contacted and every record is original synthetic data.

    python -m catalog_harvest.sync_demo --db run/inc.sqlite
"""
import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape

from .sync import run_sync, status

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DCO_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"


def record(identifier, datestamp, deleted=False, title=None, subjects=()):
    attr = ' status="deleted"' if deleted else ""
    header = (f"<header{attr}><identifier>{escape(identifier)}</identifier>"
              f"<datestamp>{datestamp}</datestamp></header>")
    if deleted:
        return f"<record>{header}</record>"
    subjects_xml = "".join(f"<dc:subject>{escape(s)}</dc:subject>" for s in subjects)
    metadata = (f'<metadata><oai_dc:dc xmlns:oai_dc="{DCO_NS}" xmlns:dc="{DC_NS}">'
                f"<dc:title>{escape(title)}</dc:title>{subjects_xml}</oai_dc:dc></metadata>")
    return f"<record>{header}{metadata}</record>"


def page(records, token=None):
    token_xml = f"<resumptionToken>{escape(token)}</resumptionToken>" if token is not None else ""
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            f'<OAI-PMH xmlns="{OAI_NS}" xmlns:oai_dc="{DCO_NS}" xmlns:dc="{DC_NS}">'
            f'<ListRecords>{"".join(records)}{token_xml}</ListRecords></OAI-PMH>')


def dated(from_date, until_date):
    return {"verb": ["ListRecords"], "metadataPrefix": ["oai_dc"],
            "from": [from_date], "until": [until_date]}


def with_token(token):
    return {"verb": ["ListRecords"], "resumptionToken": [token]}


def scenario_pages():
    # Window 1 [2026-01-01, 2026-01-03]: A active, B active then B tombstone.
    w1_p1 = page([
        record("oai:demo:A", "2026-01-01", title="River flow & sediment",
               subjects=["hydrology"]),
        record("oai:demo:B", "2026-01-01", title="Retired survey")],
        token=" opaque/token 1 ")
    w1_p2 = page([
        record("oai:demo:A", "2026-01-01", title="River flow & sediment",
               subjects=["hydrology"]),
        record("oai:demo:B", "2026-01-02", deleted=True)])
    # Window 2 [2026-01-02, 2026-01-05]: late C/Jan2, new A/Jan4, repeated B
    # tombstone (same identity/payload, must de-duplicate).
    w2_p1 = page([
        record("oai:demo:C", "2026-01-02", title="Late catch", subjects=["water"]),
        record("oai:demo:A", "2026-01-04", title="River flow & sediment",
               subjects=["hydrology"])],
        token="opaque/token 2")
    w2_p2 = page([record("oai:demo:B", "2026-01-02", deleted=True)])
    return [
        {"match": dated("2026-01-01", "2026-01-03"), "body": w1_p1},
        {"match": with_token(" opaque/token 1 "), "body": w1_p2},
        {"match": dated("2026-01-02", "2026-01-05"), "body": w2_p1},
        {"match": with_token("opaque/token 2"), "body": w2_p2},
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="run/inc.sqlite")
    args = parser.parse_args()
    pages = scenario_pages()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            answer = next((p for p in pages if query == p["match"]), None)
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

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    endpoint = f"http://127.0.0.1:{server.server_port}/oai"
    try:
        first = run_sync(endpoint, args.db, start_date="2026-01-01",
                         until="2026-01-03")
        second = run_sync(endpoint, args.db, until="2026-01-05", overlap_days=2)
        print(json.dumps({"window_1": first, "window_2": second},
                         sort_keys=True, ensure_ascii=False, indent=2))
        print(json.dumps(status(args.db), sort_keys=True, ensure_ascii=False))
    finally:
        server.shutdown()
        worker.join()
        server.server_close()


if __name__ == "__main__":
    main()
