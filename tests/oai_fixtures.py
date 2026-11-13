"""Helpers for synthetic, deterministic OAI-PMH loopback test scenarios."""
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path
from xml.sax.saxutils import escape

OAI_NS = 'http://www.openarchives.org/OAI/2.0/'
DCO_NS = 'http://www.openarchives.org/OAI/2.0/oai_dc/'
DC_NS = 'http://purl.org/dc/elements/1.1/'

ERROR_TEMPLATE = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    f'<OAI-PMH xmlns="{OAI_NS}"><error code="{{code}}">{{message}}</error></OAI-PMH>')


def record_xml(identifier, datestamp, deleted=False, title=None, subjects=()):
    deleted_attr = ' status="deleted"' if deleted else ""
    header = (f'<header{deleted_attr}>'
              f'<identifier>{escape(identifier)}</identifier>'
              f'<datestamp>{datestamp}</datestamp></header>')
    if deleted:
        return f"<record>{header}</record>"
    subject_xml = "".join(f"<dc:subject>{escape(s)}</dc:subject>" for s in subjects)
    metadata = (f'<metadata><oai_dc:dc xmlns:oai_dc="{DCO_NS}" xmlns:dc="{DC_NS}">'
                f'<dc:title>{escape(title)}</dc:title>{subject_xml}</oai_dc:dc></metadata>')
    return f"<record>{header}{metadata}</record>"


def page_xml(records, token=None):
    token_xml = ""
    if token is not None:
        token_xml = f"<resumptionToken>{escape(token)}</resumptionToken>"
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            f'<OAI-PMH xmlns="{OAI_NS}" xmlns:oai_dc="{DCO_NS}" xmlns:dc="{DC_NS}">'
            f'<ListRecords>{"".join(records)}{token_xml}</ListRecords></OAI-PMH>').encode()


def error_xml(code, message=""):
    return ERROR_TEMPLATE.format(code=code, message=escape(message)).encode()


ROOT = Path(__file__).resolve().parents[1]


def _isolated_env(extra=None):
    # -E plus an explicit PYTHONPATH keeps user site-packages and any planted
    # modules outside the project out of the subprocess while allowing imports
    # of catalog_harvest from the checked-out repository.
    env = {key: value for key, value in os.environ.items()
           if key.upper() != "PYTHONPATH"}
    env["PYTHONPATH"] = str(ROOT)
    if extra:
        env.update(extra)
    return env


class FixtureProcess:
    def __init__(self, directory, config):
        self.directory = Path(directory)
        self.config_path = self.directory / "config.json"
        self.log_path = self.directory / "requests.log"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")
        self.process = subprocess.Popen(
            [sys.executable, "-E", str(Path(__file__).with_name("fixture_server.py")),
             str(self.config_path), str(self.log_path)],
            cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=_isolated_env())
        self.endpoint = self.process.stdout.readline().strip()
        if not self.endpoint:
            raise RuntimeError("fixture server failed to start: "
                               + self.process.stderr.read())
        self.process.stdout.close()
        # stderr stays attached for diagnostics but is drained lazily; closing
        # the read end now would let a verbose child fail on write.
        self._stderr = self.process.stderr

    def requests(self):
        if not self.log_path.exists():
            return []
        return [json.loads(line) for line in
                self.log_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def shutdown(self):
        try:
            urllib.request.urlopen(self.endpoint.replace("/oai", "/shutdown"), timeout=5)
        except OSError:
            pass
        self.process.wait(timeout=10)
        if self.process.stdout:
            self.process.stdout.close()
        if self.process.stderr:
            self.process.stderr.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.shutdown()


def run_cli(*args, env=None, expect_code=0):
    """Run the sync CLI in a fresh process; return CompletedProcess."""
    proc = subprocess.run(
        [sys.executable, "-E", "-m", "catalog_harvest.sync", *map(str, args)],
        cwd=ROOT,
        capture_output=True, text=True, env=_isolated_env(env))
    if proc.returncode != expect_code:
        raise AssertionError(
            f"CLI exited {proc.returncode}, expected {expect_code}\n"
            f"stdout={proc.stdout}\nstderr={proc.stderr}")
    return proc


def cli_json(*args, env=None):
    proc = run_cli(*args, env=env)
    return json.loads(proc.stdout)


def run_batch_cli(*args, env=None, expect_code=0):
    """Run the batch CLI in a fresh process; return CompletedProcess."""
    proc = subprocess.run(
        [sys.executable, "-E", "-m", "catalog_harvest.batch", *map(str, args)],
        cwd=ROOT,
        capture_output=True, text=True, env=_isolated_env(env))
    if expect_code == 75:
        # The process died in a barrier; its stdout still carries the event.
        pass
    if proc.returncode != expect_code:
        raise AssertionError(
            f"batch CLI exited {proc.returncode}, expected {expect_code}\n"
            f"stdout={proc.stdout}\nstderr={proc.stderr}")
    return proc


def batch_cli_json(*args, env=None):
    return json.loads(run_batch_cli(*args, env=env).stdout)
