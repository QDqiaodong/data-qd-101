# Offline historical subject warehouse — layout and semantics

Entry point: `python -m catalog_harvest.warehouse {build,as-of}`.
Library API: `catalog_harvest.warehouse.build_warehouse(...)` and
`catalog_harvest.warehouse.as_of(...)`. The offline end-to-end demonstration
is `python -m catalog_harvest.warehouse_demo` and uses the synthetic taxonomy
in `data/taxonomy_demo.json`.

```sh
python -m catalog_harvest.warehouse build \
  --source-db run/catalog.sqlite --taxonomy data/taxonomy_demo.json \
  --start 2026-01-01 --end 2026-01-07 --database run/warehouse.sqlite
python -m catalog_harvest.warehouse as-of \
  --database run/warehouse.sqlite --date 2026-01-03
```

Only the Python standard library and SQLite are used. There is no HTTP client,
no server process and no dependency on the source database or taxonomy file
after a successful build. The output is a single-writer, full-rebuild,
offline analytical database.

## 1. Source facts and time semantics

### Read-only, formal-facts-only source contract

* The source may be a full-harvest database or a managed incremental database.
  Only the official `pages` and `revisions` objects are read, inside one
  explicit read-only transaction (`file:...?mode=ro`, `PRAGMA query_only=ON`).
  `sync_stage_pages`, `sync_stage_revisions`, `sync_seen_tokens`,
  `sync_window`, `sync_meta`, unfinished windows and the success watermark are
  never queried and never exported.
* A missing source database is an error; the builder never creates an empty
  source. The source file is never modified (no analytical tables, indexes or
  pragmas are written to it).
* Every formal revision is re-validated: required columns, nonempty key,
  strict `YYYY-MM-DD` datestamp, `deleted IN (0,1)`, deleted rows having no
  title/subjects, active rows having a title, subjects being unique sorted
  whitespace-normalized strings, `canonical_json` re-derived with the same
  canonicalizer the harvester uses, and the referenced page existing. The raw
  page bytes must hash to the stored `sha256`, parse with the shipping
  `parse_page`, and contain the same identifier/datestamp with the same
  canonical payload. The cited page need not be the record's earliest
  appearance. Pages not referenced by any formal revision stay in the source
  and are neither promoted nor required to parse; they are not exported.
* A corrupt formal revision or missing source fails the whole build with an
  error that names the business key (`identifier@datestamp`) or the page
  digest. Invalid XML may exist in sync staging; it is never read.

### Half-open version timeline

For each identifier the formal revisions are ordered by `datestamp`. A
revision is effective from its own date up to (but excluding) the next
revision's date; the last revision extends to the build `end`. Builds use the
half-open range `[start, end)` with strict dates and `start < end`.

* The most recent revision strictly at or before `start` determines the state
  on `start` (active or deleted). Before an identifier's first revision it is
  **unknown**: it is not back-filled as active and no deletion is invented.
* A deletion occupies its own interval, ends the preceding active version, and
  a later active revision can re-create the identifier.
* Revisions outside the range stay at the source layer and still determine
  interval end points; only non-empty intervals intersecting the range become
  version facts.
* Adjacent versions with different revision identity are never merged, even
  when title and subjects are identical. Deleted versions have no title row
  value semantics beyond the source and no subject/topic bridges.
* Dates are interpreted as the source-declared effective day, not as local
  discovery time, and the catalog history is not assumed complete.

## 2. Static taxonomy and multi-valued bridges

The taxonomy is UTF-8 JSON whose top level is exactly
`schema_version`, `nodes`, `edges`, `bindings`. `schema_version` must be the
integer `1` (booleans are rejected).

* `nodes`: exactly `{"topic_id", "label"}`, unique non-empty
  no-surrounding-whitespace strings; the array may be empty.
* `edges`: exactly `{"child", "parent"}` referencing registered nodes.
  Duplicate edges, self edges and cycles in any component are rejected. A node
  may have several parents; the array may be empty.
* `bindings`: exactly `{"subject", "topic_id"}`. A subject must already be in
  the parser's non-empty whitespace-normalized form (it is never silently
  re-cased or re-trimmed), each subject binds at most once, and the target
  node must exist. Several subjects may bind to one node. Unused bindings and
  nodes are still validated. Extra fields, wrong types and content-identical
  duplicate bindings fail the build.
* The taxonomy is fixed for the whole build. Changing it requires a full
  rebuild; no historical taxonomy versioning is inherited.

The transitive closure is materialized including each node's self row. Each
`(descendant, ancestor)` pair appears exactly once with the shortest edge
distance; multiple paths never create duplicate rows and no exponential path
set is stored. An active version's summarized topic set is the union of its
direct topics and **all** ancestors, one row per `(version, ancestor)`. A
record contributes at most one vote per topic whether it reaches it through
two subjects, two synonym bindings, or two diamond paths. Unmapped subjects
never feed a real topic and are retained separately. Having no subjects and
having only unmapped subjects are distinct states.

## 3. Product tables, keys and grain

The format marker is `wh_meta.format = 'catalog_harvest_warehouse'`.

| Table / view | Grain (primary key) | Meaning |
| --- | --- | --- |
| `wh_meta` | `key` | Format id/version, build `[start,end)`, logical source digest, taxonomy digest. |
| `wh_source_pages` | `sha256` | Every **formal** source page's raw XML bytes and SHA256; self-contained evidence. |
| `wh_source_revisions` | `(identifier, datestamp)` | All formal normalized revisions (including out-of-range ones); FK to page. |
| `wh_taxonomy_provenance` | `taxonomy_sha256` | Raw taxonomy bytes, schema version and digest. |
| `wh_taxonomy_nodes` | `topic_id` | Node label. |
| `wh_taxonomy_edges` | `(child, parent)` | Direct edges, FKs to nodes, no self edges. |
| `wh_taxonomy_bindings` | `subject` | Subject → node mapping, FK to node. |
| `wh_topic_closure` | `(descendant, ancestor)` | Shortest-distance closure incl. self row. |
| `wh_versions` | surrogate `version_id`; unique `(identifier, datestamp, valid_from)` | One clipped half-open version interval with `deleted`, title, `has_subjects`, `has_unmapped`, page evidence and canonical payload; CHECK `valid_from < valid_to`. |
| `wh_version_subjects` | `(version_id, subject)` | Every original multi-valued subject of an active version. |
| `wh_version_subject_topics` | `(version_id, subject, topic_id)` | Only the mapped subjects, retaining which original subject hit which node. |
| `wh_version_topics` | `(version_id, topic_id)` | De-duplicated direct node bridge (one row even when several subjects bind there). |
| `wh_version_ancestors` | `(version_id, topic_id)` | De-duplicated direct+ancestor bridge used for counts. |
| `wh_topic_intervals` | `(topic_id, valid_from)` | Sparse maximal half-open constant-count intervals per node, including zero intervals and unused nodes. |
| `wh_catalog_intervals` | `(metric, valid_from)` | Sparse intervals for `active`, `deleted`, `active_no_subject`, `active_with_unmapped`. |
| `wh_topic_record_days` (view) | `topic_id` | Integer `sum(days * count)` over `wh_topic_intervals` — record-days, not distinct identifiers. |
| `wh_unmapped_subjects` (view) | `(version_id, subject)` | Subjects retained on the version but absent from bindings. |

Foreign keys reference the tables above and are enforced during the build.
Sync staging rows and local absolute paths never enter the product.

### Provenance digest

`source_facts_sha256` hashes a documented stable structure (sorted page
digests with decoded XML text, and revisions ordered by
`(identifier, datestamp)` with their full logical payload) serialized through
compact sorted-key JSON with real Unicode. It is a digest of the **logical
read transaction**, not of the SQLite file bytes, so staging-only changes that
leave formal facts untouched leave both the business tables and this digest
unchanged. `taxonomy_sha256` is the SHA256 of the exact taxonomy bytes;
reordering JSON arrays changes this digest without changing any business set
or metric.

## 4. Sparse interval counts

For each node, events are emitted only at version boundaries: `+1` when an
active version enters a topic's ancestor set and `-1` when it leaves. All
deltas on the same date are applied together between segments, so a version
exiting and another entering on one day cannot produce a transient negative or
double count. The sweep walks boundaries (never expanding calendar days) and
merges adjacent equal-count segments into maximal half-open intervals. Every
node, including unused nodes, has intervals that tile the entire build range,
including zero-count spans. `record_days` is recomputable in SQL:

```sql
SELECT topic_id,
       CAST(sum(CAST(julianday(valid_to) - julianday(valid_from) AS INTEGER)
                * record_count) AS INTEGER) AS record_days
FROM wh_topic_intervals
GROUP BY topic_id;
```

The four catalog metrics are computed the same way from version entry/exit
events. `active_no_subject` and `active_with_unmapped` overlap by design and
must not be treated as mutually exclusive groups. Every count is an integer;
no float aggregation is stored. Identical formal facts, taxonomy bytes and
parameters yield identical logical tables and a deterministic stdout JSON
summary (no timestamps or machine paths).

## 5. SQL recipes

Titles/subjects effective on a day:

```sql
SELECT v.identifier, v.datestamp, v.title
FROM wh_versions v
WHERE v.valid_from <= '2026-01-02' AND '2026-01-02' < v.valid_to
  AND v.deleted = 0
ORDER BY v.identifier;
```

De-duplicated per-node record count on a day (consistent with
`wh_topic_intervals`):

```sql
SELECT a.topic_id, count(DISTINCT v.identifier) AS records
FROM wh_versions v
JOIN wh_version_ancestors a ON a.version_id = v.version_id
WHERE v.deleted = 0
  AND v.valid_from <= '2026-01-02' AND '2026-01-02' < v.valid_to
GROUP BY a.topic_id;
```

Trace why a record voted for `root`, down to original subjects:

```sql
SELECT s.subject, s.topic_id AS direct_node, c.ancestor, c.distance
FROM wh_version_subject_topics s
JOIN wh_versions v ON v.version_id = s.version_id
JOIN wh_topic_closure c ON c.descendant = s.topic_id
WHERE v.identifier = 'oai:demo:A' AND c.ancestor = 'root';
```

Unmapped subjects and the full sparse history of one node:

```sql
SELECT v.identifier, u.subject
FROM wh_unmapped_subjects u JOIN wh_versions v USING (version_id);

SELECT valid_from, valid_to, record_count
FROM wh_topic_intervals WHERE topic_id = 'root' ORDER BY valid_from;
```

## 6. Atomic publication and failure rebuild

The builder writes a unique temporary database (`.whbuild-*` in the target
directory), loads all facts, runs `PRAGMA integrity_check`,
`PRAGMA foreign_key_check` and an interval-coverage check, closes the
database, and only then `os.replace`s the target. The output must not alias
the source database or taxonomy file, including via symlinks or hard links.

* Any input, transformation, write, validation or replacement failure exits
  non-zero after removing this run's temporary file; an existing target is
  left byte-for-byte intact and the run can simply be re-executed after the
  problem is corrected.
* A failure while printing stdout after a successful replacement cannot roll
  the committed database back. No concurrent-publication or power-loss
  protocol beyond single-writer atomic replacement is provided.
* Test-only hooks `CATALOG_HARVEST_WH_WRITE_FAIL` and
  `CATALOG_HARVEST_WH_REPLACE_FAIL` exercise the cleanup/rebuild paths and are
  not part of the operational contract.

## 7. `as-of` reader

`as-of --date D` requires `start <= D < end` and opens the product read-only;
it works after the source database and taxonomy file have been moved away. It
returns every version effective on that day (with original subjects, direct
topics, ancestor topics and unmapped subjects), the number of known versions
and distinct identifiers, the four catalog counts, and the de-duplicated
per-node record counts. Identifiers with no effective version are unknown and
are not counted as deletions.
