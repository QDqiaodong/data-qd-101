"""A deliberately small OAI-PMH ListRecords / oai_dc profile."""
import json
import re
from datetime import date
from xml.etree import ElementTree as ET

OAI = "http://www.openarchives.org/OAI/2.0/"
DC = "http://purl.org/dc/elements/1.1/"
DCO = "http://www.openarchives.org/OAI/2.0/oai_dc/"
MAX_PAGE_BYTES = 1024 * 1024


def parse_page(raw):
    kind, payload = parse_response(raw)
    if kind == "error":
        raise ValueError("OAI error: " + ",".join(payload))
    return payload


def canonical(record):
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def parse_response(raw):
    """Parse a ListRecords-style response, surfacing OAI error codes explicitly.

    Returns ``(kind, payload)`` where kind is:
      * ``"records"`` -> payload is ``(records, resumption_token)``;
      * ``"error"``   -> payload is the list of OAI error codes present.

    All XML safety rules of :func:`parse_page` still apply; the only difference
    is that an OAI ``<error>`` element is reported instead of raised so the
    incremental window machine can implement the two documented error branches
    (noRecordsMatch on the dated request, badResumptionToken with one restart).
    """
    if len(raw) > MAX_PAGE_BYTES:
        raise ValueError("oversized page")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("only UTF-8 XML is supported") from exc
    declaration = re.match(r"<\?xml\s+[^?]*encoding\s*=\s*['\"]([^'\"]+)['\"]", text, re.I)
    if "\x00" in text or (declaration and declaration.group(1).lower() != "utf-8"):
        raise ValueError("only UTF-8 XML is supported")
    if re.search(r"<!\s*(DOCTYPE|ENTITY)", text, re.I):
        raise ValueError("unsupported DTD/entity declaration")
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise ValueError("invalid XML") from exc
    if root.tag != f"{{{OAI}}}OAI-PMH":
        raise ValueError("wrong protocol namespace")
    errors = root.findall(f"{{{OAI}}}error")
    if errors:
        return "error", [e.attrib.get("code", "unknown") for e in errors]
    lists = root.findall(f"{{{OAI}}}ListRecords")
    if len(lists) != 1:
        raise ValueError("one ListRecords required")
    records = []
    for element in lists[0].findall(f"{{{OAI}}}record"):
        headers = element.findall(f"{{{OAI}}}header")
        if len(headers) != 1:
            raise ValueError("one record header required")
        header = headers[0]
        def field(parent, name):
            values = parent.findall(name)
            if len(values) != 1 or not (values[0].text or "").strip():
                raise ValueError("missing or repeated required field: " + name)
            return values[0].text.strip()
        identifier = field(header, f"{{{OAI}}}identifier")
        stamp = field(header, f"{{{OAI}}}datestamp")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", stamp):
            raise ValueError("profile requires date-granularity datestamps")
        date.fromisoformat(stamp)
        status = header.attrib.get("status", "active")
        if status not in {"active", "deleted"}:
            raise ValueError("unsupported header status")
        deleted = status == "deleted"
        title, subjects = None, []
        if not deleted:
            metadata = element.find(f"{{{OAI}}}metadata/{{{DCO}}}dc")
            if metadata is None:
                raise ValueError("active record requires oai_dc metadata")
            title = field(metadata, f"{{{DC}}}title")
            subjects = sorted({" ".join("".join(s.itertext()).split()) for s in metadata.findall(f"{{{DC}}}subject") if "".join(s.itertext()).strip()})
        elif element.find(f"{{{OAI}}}metadata") is not None:
            raise ValueError("deleted record must not have metadata")
        records.append({"identifier": identifier, "datestamp": stamp, "deleted": deleted, "title": title, "subjects": subjects})
    tokens = lists[0].findall(f"{{{OAI}}}resumptionToken")
    if len(tokens) > 1:
        raise ValueError("multiple continuation markers")
    continuation = (tokens[0].text or "") if tokens else ""
    return "records", (records, continuation)


def parse_iso_date(value):
    """Strict YYYY-MM-DD validation; returns a :class:`datetime.date`."""
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("dates must use strict YYYY-MM-DD format")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("dates must use strict YYYY-MM-DD format") from exc
