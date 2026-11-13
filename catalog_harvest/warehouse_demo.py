"""Offline end-to-end demonstration of the historical subject warehouse.

The scenario is original synthetic data: two catalog identifiers A and B with
multiple dated revisions (including a deletion and a later re-appearance) and
a small fixed taxonomy with a diamond-shaped ancestor graph. Everything runs
in-process on temporary files; no HTTP, external service or real archive data
is involved.

    python -m catalog_harvest.warehouse_demo --workdir run/wh-demo
"""
import argparse
import json
import sqlite3
from pathlib import Path

from .harvest import DDL
from .protocol import canonical
from .warehouse import build_warehouse, as_of


def record(identifier, datestamp, deleted=False, title=None, subjects=()):
    return {"identifier": identifier, "datestamp": datestamp,
            "deleted": deleted, "title": title,
            "subjects": sorted(subjects)}


def page(records):
    from xml.sax.saxutils import escape
    oai = "http://www.openarchives.org/OAI/2.0/"
    dco = "http://www.openarchives.org/OAI/2.0/oai_dc/"
    dc = "http://purl.org/dc/elements/1.1/"
    body = []
    for rec in records:
        if rec["deleted"]:
            body.append(
                "<record><header status=\"deleted\">"
                f"<identifier>{escape(rec['identifier'])}</identifier>"
                f"<datestamp>{rec['datestamp']}</datestamp></header></record>")
        else:
            subjects = "".join(
                f"<dc:subject>{escape(s)}</dc:subject>"
                for s in rec["subjects"])
            body.append(
                "<record><header>"
                f"<identifier>{escape(rec['identifier'])}</identifier>"
                f"<datestamp>{rec['datestamp']}</datestamp></header>"
                f"<metadata><oai_dc:dc xmlns:oai_dc=\"{dco}\" "
                f"xmlns:dc=\"{dc}\">"
                f"<dc:title>{escape(rec['title'])}</dc:title>"
                f"{subjects}</oai_dc:dc></metadata></record>")
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<OAI-PMH xmlns="{oai}" xmlns:oai_dc="{dco}" xmlns:dc="{dc}">'
        f'<ListRecords>{"".join(body)}</ListRecords></OAI-PMH>').encode("utf-8")


def write_source(database):
    """Create a harvest-shaped source DB with the warehouse demo revisions."""
    pages_records = [
        # A: active with hydro/water on Jan 1, deleted Jan 3, re-appears
        # active with no subjects on Jan 5.
        [record("oai:demo:A", "2026-01-01", title="River flow",
                subjects=["hydro", "water"])],
        [record("oai:demo:A", "2026-01-03", deleted=True)],
        [record("oai:demo:A", "2026-01-05", title="River flow reborn")],
        # B: active water/mystery on Jan 2, active with only mystery on Jan 4.
        [record("oai:demo:B", "2026-01-02", title="Survey B",
                subjects=["water", "mystery"])],
        [record("oai:demo:B", "2026-01-04", title="Survey B revised",
                subjects=["mystery"])],
    ]
    import hashlib
    database.parent.mkdir(parents=True, exist_ok=True)
    if database.exists():
        database.unlink()
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(DDL)
        for records in pages_records:
            raw = page(records)
            digest = hashlib.sha256(raw).hexdigest()
            db.execute("INSERT OR IGNORE INTO pages VALUES (?,?)", (digest, raw))
            for rec in records:
                db.execute(
                    "INSERT INTO revisions VALUES (?,?,?,?,?,?,?)",
                    (rec["identifier"], rec["datestamp"], int(rec["deleted"]),
                     rec["title"], json.dumps(rec["subjects"]),
                     canonical(rec), digest))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", default="run/wh-demo")
    args = parser.parse_args()
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    source = workdir / "source.sqlite"
    taxonomy = Path(__file__).resolve().parents[1] / "data" / "taxonomy_demo.json"
    warehouse = workdir / "warehouse.sqlite"

    write_source(source)
    summary = build_warehouse(
        source, taxonomy, "2026-01-01", "2026-01-07", warehouse)
    print("BUILD SUMMARY")
    print(json.dumps(summary, sort_keys=True, indent=2))
    for day in ("2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04",
                "2026-01-05", "2026-01-06"):
        result = as_of(warehouse, day)
        print(f"\nAS-OF {day}")
        print(json.dumps(
            {"counts": result["counts"],
             "topic_record_counts": result["topic_record_counts"],
             "identifiers": [v["identifier"] for v in result["versions"]]},
            sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
