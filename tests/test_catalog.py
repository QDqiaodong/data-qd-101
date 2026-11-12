import sqlite3
import tempfile
import unittest
from pathlib import Path
from catalog_harvest.protocol import parse_page
from catalog_harvest.harvest import harvest
from catalog_harvest.transport import page_fetcher
from catalog_harvest.demo import fixture_server

DATA=Path(__file__).resolve().parents[1]/"data"


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db=Path(self.temp.name)/"catalog.sqlite"
        self.one=(DATA/"page1.xml").read_bytes()
        self.two=(DATA/"page2.xml").read_bytes()

    def fetch(self,marker):
        return self.two if marker else self.one

    def test_http_pagination_and_replay(self):
        with fixture_server(DATA) as endpoint:
            first=harvest(page_fetcher(endpoint),self.db)
            second=harvest(page_fetcher(endpoint),self.db)
        self.assertEqual(first,{"pages":2,"records_seen":4,"inserted_revisions":3,"active_records":1,"deleted_records":1})
        self.assertEqual(second["inserted_revisions"],0)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT title FROM current_records WHERE deleted=0").fetchone()[0],"River flow & sediment")
            self.assertEqual(db.execute("SELECT count(*) FROM pages").fetchone()[0],2)

    def test_continuation_is_opaque_including_edge_whitespace(self):
        for index,marker in enumerate([" page two/+? ","  "]):
            first=self.one.replace(b"page two/+?",marker.encode())
            requested=[]
            def fetch(received):
                requested.append(received)
                if received=="":return first
                if received==marker:return self.two
                raise ValueError("continuation was modified")
            result=harvest(fetch,Path(self.temp.name)/f"opaque-{index}.sqlite")
            self.assertEqual(result["pages"],2)
            self.assertEqual(requested,["",marker])

    def test_tombstone_and_older_revision(self):
        harvest(self.fetch,self.db)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT deleted FROM current_records WHERE identifier='oai:demo:B'").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT count(*) FROM revisions WHERE identifier='oai:demo:B'").fetchone()[0],2)

    def test_cycle_and_page_budget_do_not_publish(self):
        for fetch,limit in [(lambda _:self.one,20),(self.fetch,1)]:
            with self.assertRaises(ValueError):harvest(fetch,self.db,limit)
            self.assertFalse(self.db.exists())

    def test_later_page_failure_does_not_publish(self):
        def failed(marker):
            if marker:raise OSError("fixture disconnected")
            return self.one
        with self.assertRaises(OSError):harvest(failed,self.db)
        self.assertFalse(self.db.exists())

    def test_conflict_rolls_back_new_page_and_revision(self):
        harvest(self.fetch,self.db)
        changed=self.two.replace(b"River flow &amp; sediment",b"Changed title")
        with self.assertRaisesRegex(ValueError,"conflicting"):
            harvest(lambda marker: changed if marker else self.one,self.db)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM pages").fetchone()[0],2)
            self.assertEqual(db.execute("SELECT count(*) FROM revisions").fetchone()[0],3)

    def test_invalid_xml_dates_and_entities(self):
        for raw in [b"<broken",self.one.replace(b"2026-01-01",b"2026-02-30"),b'<!DOCTYPE x [<!ENTITY y "x">]>'+self.one,self.one.replace(b"River flow &amp; sediment",b"")]:
            with self.subTest(raw=raw[:20]),self.assertRaises(ValueError):parse_page(raw)

    def test_utf16_entities_and_inconsistent_encoding_rejected(self):
        text=self.one.decode().replace('<?xml version="1.0" encoding="UTF-8"?>','<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE OAI-PMH [<!ENTITY x "expanded">]>').replace("River flow &amp; sediment","&x;")
        for raw in [text.encode("utf-16"),text.encode("utf-8"),self.one.replace(b"UTF-8",b"ISO-8859-1")]:
            with self.assertRaises(ValueError):parse_page(raw)

    def test_protocol_error_and_unknown_namespace(self):
        for raw in [b'<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><error code="badArgument"/></OAI-PMH>',self.one.replace(b"http://www.openarchives.org/OAI/2.0/",b"urn:wrong")]:
            with self.assertRaises(ValueError):parse_page(raw)

    def test_external_transport_and_oversize_rejected(self):
        for endpoint in ["https://example.com/oai","http://example.com/oai","http://u:p@localhost/oai","http://localhost/oai?x=1"]:
            with self.assertRaises(ValueError):page_fetcher(endpoint)
        with self.assertRaises(ValueError):parse_page(b"x"*(1024*1024+1))


if __name__=="__main__":unittest.main()
