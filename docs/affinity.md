# Cross-topic co-occurrence affinity — plan, statistics and report contract

Entry point: `python -m catalog_harvest.affinity --database FILE --date
YYYY-MM-DD --plan FILE --output FILE`.
Library API: `catalog_harvest.affinity.analyze(...)`,
`catalog_harvest.affinity.serialize_report(...)` and
`catalog_harvest.affinity.publish_report(...)`.

The affinity reader is an *independent consumer* of a warehouse produced by
`python -m catalog_harvest.warehouse`. It never harvests, syncs, builds a
warehouse, alters the taxonomy, or writes to the source database. It opens
the existing product read-only inside one explicit SQLite read transaction
and writes exactly one deterministic JSON document through an atomic
temp-file replacement. There is no service, no web page and no network
access; only the Python standard library and SQLite are used.

## 1. Analysis plan

The plan is strict UTF-8 JSON. Its top level is *exactly* the four keys
`schema_version`, `topics`, `alpha` and `min_shared`; any missing or extra
key, and any repeated object key anywhere the parser is told to detect it,
fails the run.

* `schema_version` must be the integer `1`. Booleans are not integers and
  `true`, `"1"`, `1.0` and `2` are all rejected.
* `topics` is an array of 2 to 50 unique topic ids. Each id is a nonempty
  string with no leading or trailing whitespace that must reference a node
  already registered in the warehouse. Ids are never trimmed, re-cased or
  otherwise normalized. Array order carries no priority.
* `alpha` is an **ordinary decimal string** (a JSON string, never a JSON
  number) in `(0, 1]` with at most six decimal places, e.g. `"0.05"`,
  `"0.3"` or `"1"`. Exponents, signs, surrounding whitespace, `NaN` and
  `Infinity` cannot match and are rejected. The exact string is retained in
  the report alongside its exact rational value.
* `min_shared` is an integer in `1..1000000000`. Booleans are rejected.

The plan file's bytes are captured a single time; both the parse and the
`plan_bytes_sha256` digest use that one byte sequence. Reordering the
`topics` array changes the plan digest but never any business statistic,
rank or clue ordering.

## 2. Read-only single-snapshot contract

* A missing database is an error; the reader never creates a file, never
  runs DDL, upgrades or a warehouse build, and never writes pragmas, tables
  or indexes to the source.
* The warehouse format marker must be `catalog_harvest_warehouse` and its
  `format_version` must be supported. The analysis date must be strict
  `YYYY-MM-DD` and satisfy `build_start <= date < build_end`.
* All reads happen inside one explicit deferred read transaction
  (`file:...?mode=ro`, `PRAGMA query_only=ON`, `BEGIN` … `COMMIT`). Metadata
  is read first, then the taxonomy relations, then the day's versions and
  their bridges; they therefore share one snapshot and can never mix facts
  from two commits. A test-only hook
  (`CATALOG_HARVEST_AFFINITY_SNAPSHOT_HOOK`) runs after the metadata read and
  before the version/bridge reads to prove this under a concurrently
  committing connection.
* The required structure is validated, including the format marker and
  version, unique closure pairs with non-negative integer distances, edges
  having matching closure rows, bridge uniqueness and known-topic
  references, legal half-open version intervals, at most one effective
  version per identifier on the day, and deleted versions carrying neither
  a title nor topic bridges. A corrupt fact that the analysis actually uses
  fails the whole run and names the offending business key; corruption is
  never papered over by silent de-duplication.

## 3. Population and one-vote-per-record semantics

The analysis day selects versions with `valid_from <= date < valid_to`.

* An identifier is in the population only when its effective version is
  active (`deleted = 0`). A deletion tombstone known on the day stays in the
  per-record evidence and in the `known_deleted_versions` count but never
  enters the denominator `N`. An identifier whose first revision is later
  than the day is simply unknown and is not back-filled as either active or
  deleted.
* **Every active version enters `N`**, including records with no subjects,
  records whose subjects are all unmapped, and records that hit none of the
  selected topics. The denominator is never recomputed after discarding
  zero-hit records.
* A record's selected-topic set is the intersection of the plan topics with
  the de-duplicated `wh_version_ancestors` bridge. Synonym subjects bound to
  one node, several direct topics and diamond-shaped ancestor paths can
  contribute at most one vote per topic per record. Votes are never weighted
  by the raw subject count, the direct-topic count, the number of ancestor
  paths or the number of source pages.

## 4. Fixed testing family and ancestry exclusions

For every distinct unordered pair of selected topics, the warehouse closure
is consulted *before* any co-occurrence count, lift, p-value or
`min_shared` check:

* If either topic is an ancestor of the other, the pair is reported in
  `excluded_pairs` with status `ancestor_related`, the descendant, the
  ancestor, the existing shortest closure distance and the evidence table.
  It never enters the testing family.
* Every other pair — including pairs with zero co-occurrence, zero margins
  or low counts — forms the fixed testing family of size `m`. Two topics may
  share a descendant (for example the parents of a diamond) and still be a
  testable pair as long as neither is an ancestor of the other. Excluding
  ancestor pairs does not make the remaining tests independent.

Each pair is ordered into `A`, `B` by Unicode code point. When every pair is
ancestor-related the family is empty (`m = 0`) and the run reports the
normal `no_eligible_pairs` diagnostic without inventing tests.

## 5. Exact contingency table, Fisher tail and BH adjustment

For a pair `(A, B)` the four cells are `a` = contains both, `b` = only A,
`c` = only B, `d` = neither; they are non-negative integers summing to `N`.
The report carries both margins (`n_A = a+b`, `n_B = a+c`, `neither = d`),
`a/N`, the two directional confidences `a/(a+b)` and `a/(a+c)`, and
`lift = a*N / ((a+b)*(a+c))`. A metric whose denominator is zero is emitted
as JSON `null`; no `NaN` or `Infinity` ever appears.

When `N > 0` the one-sided fixed-margin Fisher enrichment tail is computed
exactly:

```
p = sum over feasible k in [a, min(nA,nB)] of
    C(nA,k) * C(N-nA,nB-k) / C(N,nB)
```

All arithmetic uses arbitrary-precision integers and `fractions.Fraction`.
There is no float, no rounding into a threshold comparison, no asymptotic
test and no random sampling. A legal zero-margin table yields `p = 1/1`.

Benjamini–Hochberg adjustment runs over the **whole** family: the exact
p-values are sorted ascending with the `(A, B)` pair as a stable tie-breaker,
ranked `1..m`, and then

```
q_i = min(1, min_{j >= i} (m * p_j / j))
```

using a reverse cumulative minimum. Significant items are never removed
first, and equal p-values give identical conclusions regardless of input
permutation. Each test stores its rank, exact `p`, exact `q` and the family
size `m` actually used.

A pair is selected as a clue only when all three hold, with all comparisons
on exact rational values:

```
a >= min_shared   AND   lift > 1   AND   q <= alpha
```

`min_shared` is applied only after adjustment; it filters clues but never
changes the family, ranks, p-values or q-values. The clue board is ordered by
`q` ascending, then `lift` descending, then `a` descending, then `(A, B)`
ascending. Every testable pair is reported with its per-criterion
pass/fail reasons, and the selected clues are listed separately.

When `N = 0` the run is a normal `no_active_population` result: the family,
zero tables, exclusions and per-record evidence are still emitted, but the
ratios, lift, p and q are `null`, ranks are `null`, and no clue is produced.

## 6. Report JSON contract

The output is one compact, sorted-key, real-Unicode UTF-8 JSON document with
its own `schema_version` and `mode`. Top-level fields:

| Field | Meaning |
| --- | --- |
| `schema_version`, `mode`, `format` | Report format id (`catalog_harvest_affinity_report`, mode `cross_topic_cooccurrence`). |
| `date` | The strict analysis day. |
| `warehouse` | Warehouse format/version, build `[start,end)`, and the preserved `source_facts_sha256` / `taxonomy_sha256`. |
| `plan` | Selected topics with labels, the raw and exact alpha, and `min_shared`. |
| `family` | Family size `m`, the code-point pair ordering and the fixed-before-statistics definition. |
| `excluded_pairs` | Every ancestor-related pair with descendant, ancestor, shortest distance and evidence. |
| `tests` | Every testable pair: table, margins, rational metrics, exact rank/p/q/family size and selection reasons. |
| `clues` | The selected pairs in the exact clue ordering. |
| `population` | `N`, active version count, known deleted count and total known versions on the day. |
| `diagnostics` | `no_active_population` and/or `no_eligible_pairs` when applicable. |
| `record_evidence` | Per-version evidence for **all** known versions on the day (active and deleted), including identifier, datestamp, valid interval, deleted flag, title, page digest summary and the selected topics it hit. Active zero-hit records are kept; the deleted set is reported rather than hidden. |
| `selected_relations` | The selected-topic edges and closure rows used to decide the family. |
| `digests` | Plan-byte digest and three logical digests (below). |
| `interpretation` | Explicit scope and non-causal caveats. |

Rational values use the canonical reduced string `numerator/positive-denominator`
with zero written `0/1`; counts stay JSON integers. The page summary cites the
warehouse's stored page digest only — the report does not embed or claim to
reproduce the original XML text.

### Logical digests

Three digests are defined over documented, stable, sorted, compact JSON
structures so they can be recomputed from the report alone after the
warehouse has been moved away:

* `metadata_sha256` — warehouse identity, range, analysis date and the two
  preserved source digests;
* `selected_relations_sha256` — selected topics with labels and the
  selected-topic edge/closure rows;
* `record_evidence_sha256` — the full ordered per-record evidence.

They are digests of logical facts, not of the SQLite file bytes, so adding
an unrelated table or changing database insertion order leaves them, the
family and every statistic unchanged. Identical logical facts, plan bytes and
date produce byte-identical reports; no current time, duration, `rowid` or
machine absolute path is written.

## 7. Atomic publication and failure semantics

The report is fully read, validated and computed before any output file is
touched. It is serialized, then written to a unique same-directory temporary
file (`.affinity-*.tmp`), flushed and fsynced, closed, and only then
`os.replace`d into place. The output must not alias the warehouse or the
plan, including through symlinks or hard links.

* Any configuration, read, computation, write or replacement failure exits
  the CLI non-zero, removes this run's temporary file, and leaves an existing
  report byte-for-byte intact; the corrected command can simply be re-run.
* Test-only hooks `CATALOG_HARVEST_AFFINITY_WRITE_FAIL` and
  `CATALOG_HARVEST_AFFINITY_REPLACE_FAIL` exercise the cleanup/rebuild paths
  and are not part of the operational contract.
* A failure while printing stdout after a successful replacement cannot roll
  the committed report back. The boundary is a single offline writer.

## 8. Interpretation limits

`p` is a diagnostic tail probability from a fixed-margin hypergeometric
model, not a population-wide significance guarantee, and `q` is the BH
adjustment for exactly this fixed family. No true-FDR guarantee is made
under arbitrary dependence between topic pairs. The catalog is an
incomplete, non-random administrative sample, so lift and enrichment must
not be read as universal population patterns or causal effects, and a clue
is a lead for manual review — not an automatic classification or a proven
interdisciplinary relationship.
