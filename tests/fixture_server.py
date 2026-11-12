"""Scriptable loopback OAI-PMH fixture for incremental sync tests.

Run as a subprocess::

    python -I tests/fixture_server.py CONFIG.json LOG.json

The config is an original synthetic scenario (never real archive data):

{
  "port": 0,                       // 0 -> ephemeral port, printed on stdout
  "pages": [ {"match": {...query...}, "status": 200, "body": "<xml>"} ... ],
  "sequence": [ {"match": {...query...}, "body": "<xml>"}, ... ],
  "fallback": {"status": 400, "body": "..."}
}

A page may carry ``"nth": [i, j]`` so it only answers the i-th/j-th matching
request (0-based), which lets one config serve an abandoned traversal and its
restart through the identical dated request. ``sequence`` entries are consumed
in order when their query matches. Every GET query is appended to the log
file as one JSON object. A GET to ``/shutdown`` stops the server
deterministically -- no sleeps in tests.
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


def _match(query, spec):
    if set(query) != set(spec):
        return False
    for key, value in spec.items():
        wanted = value if isinstance(value, list) else [value]
        if query.get(key) != wanted:
            return False
    return True


def main(config_path, log_path):
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    log = Path(log_path)

    class Handler(BaseHTTPRequestHandler):
        request_index = 0

        def do_GET(self):
            parts = urlsplit(self.path)
            query = parse_qs(parts.query, keep_blank_values=True)
            if parts.path == "/shutdown":
                self.send_response(204)
                self.end_headers()
                server.shutdown()
                return
            with log.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(query, sort_keys=True) + "\n")
            answer = config.get("fallback", {"status": 400, "body": "unmatched request"})
            idx = Handler.request_index
            Handler.request_index += 1
            for page in config["pages"]:
                if "nth" in page and idx not in page["nth"]:
                    continue
                if _match(query, page["match"]):
                    answer = page
                    break
            body = answer["body"].encode("utf-8")
            self.send_response(answer.get("status", 200))
            self.send_header("Content-Type", "text/xml; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", config.get("port", 0)), Handler)
    print(f"http://127.0.0.1:{server.server_port}/oai", flush=True)
    server.serve_forever()
    server.server_close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
