"""Short-lived deterministic local OAI-PMH source, never a public endpoint."""
import argparse
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs,urlsplit
from .transport import page_fetcher
from .harvest import harvest


@contextmanager
def fixture_server(directory):
    directory = Path(directory)
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)
            if query == {"verb":["ListRecords"],"metadataPrefix":["oai_dc"]}:
                filename = "page1.xml"
            elif query == {"verb":["ListRecords"],"resumptionToken":["page two/+?"]}:
                filename = "page2.xml"
            else:
                self.send_error(400)
                return
            self.send_response(200)
            self.send_header("Content-Type","text/xml; charset=utf-8")
            self.end_headers()
            self.wfile.write((directory/filename).read_bytes())
        def log_message(self,*args):
            pass
    with ThreadingHTTPServer(("127.0.0.1",0),Handler) as server:
        worker=threading.Thread(target=server.serve_forever,daemon=True)
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/oai"
        finally:
            server.shutdown()
            worker.join()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--fixtures",type=Path,default=Path("data"))
    parser.add_argument("--db",type=Path,default=Path("run/catalog.sqlite"))
    args=parser.parse_args()
    with fixture_server(args.fixtures) as endpoint:
        print(json.dumps(harvest(page_fetcher(endpoint),args.db),sort_keys=True))


if __name__=="__main__":
    main()
