# Catalog profile v1

The input is a synthetic OAI-PMH 2.0 ListRecords response with oai_dc metadata.
This is a documented subset, not a general protocol conformance implementation.
A caller-controlled loopback HTTP endpoint is required; external endpoints,
credentials, redirects, query strings on the base URL, and DTD/entities are
rejected. Only UTF-8 XML (optional UTF-8 BOM) is supported; NUL and other declared encodings
are rejected before parsing. Each page <=1 MiB, timeout 5s, default traversal <=20 pages. Tokens
are opaque query values (including punctuation); cycles and exhausted bounds
fail before opening the output database. HTTP fixture has no account or license
restrictions; bundled records are synthetic, not copied from a real archive.

Each record has one nonempty identifier and YYYY-MM-DD valid date datestamp.
Active records need one nonempty dc:title; subjects are whitespace-normalized,
unique, sorted strings. A deleted header has no metadata. Identifier/datestamp
is a revision identity: identical payload repeats are ignored, conflicting
payloads reject the whole storage batch. Later dates are retained as revisions;
current_records selects the latest date including deletion tombstones. An older
revision can be inserted without resurrecting a deleted current record.

SQLite pages preserve raw XML bytes by SHA256; each revision points to its first
accepted page. Entire fetched traversal commits together. Output summary counts
pages/seen records/inserted revisions and active/deleted current identifiers.
The full-harvest path has no checkpoint and keeps one bounded in-memory
traversal. A separate, opt-in incremental path
(`python -m catalog_harvest.sync`, documented in `incremental.md`) adds
resumable date-window checkpoints, a success watermark, per-page staging
tables and one persisted badResumptionToken restart; managed incremental
databases are rejected by the full-harvest entry point and vice versa.
There is no scheduler, external archive connector, metadata schema
negotiation or automatic correction policy. Single writer. Pages/revisions
are data, not executable XML or instructions. Runtime database stays under
ignored run/.
