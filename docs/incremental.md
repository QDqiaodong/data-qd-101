# Incremental date-window sync — internal layout and recovery contract

Entry point: `python -m catalog_harvest.sync {run,status,discard}`.
Implementation: `catalog_harvest/sync.py`; transport: `window_fetcher` in
`catalog_harvest/transport.py`; structured OAI parsing (records vs. error
codes): `parse_response` in `catalog_harvest/protocol.py`.

The incremental database is *bound* to exactly one loopback endpoint and one
fixed start date **B**. It reuses the official `pages`, `revisions` and
`current_records` view from the full harvest and adds the managed tables
below. A database that already contains non-empty full-harvest
`pages`/`revisions` cannot be bound; the full harvest entry point refuses to
open a managed incremental database. The two paths never write the same file.

## Managed tables

| Table | Purpose |
| --- | --- |
| `sync_meta` | Singleton key/value: `endpoint`, `base_date` (B), `watermark` (W). |
| `sync_window` | One row per attempted window: frozen parameters, state, stage, current traversal, persisted restart count, confirmed page/revision counters, the next opaque token, last error. |
| `sync_stage_pages` | Checkpointed raw XML per `(window_id, traversal, page_seq)`, with sha256, record count, request kind (`first`/`resume`) and the next token. |
| `sync_stage_revisions` | Parsed revisions of the current traversal, keyed by `(window_id, traversal, identifier, datestamp)`; points at the first staging page that carried the identity. |
| `sync_seen_tokens` | Per-traversal token history used for cross-process cycle detection. |

`sync_window.state` is one of:

- `active` — fetching or resumable (including a publish attempt that rolled
  back); `stage` is `fetching` until the terminal page is stored, then
  `publishing` inside the publish transaction, then `done`.
- `failed` — a hard content/protocol/budget failure persisted an explicit
  error in `last_error`. `run` refuses to proceed until `discard` clears it.
- `complete` — facts and watermark committed together; the row is retained as
  window history and no longer counts as an unfinished window.

## Watermark and window math

W is the **inclusive date upper bound of a fully traversed window**, never the
maximum record datestamp. The first window is `[B, until]`. Each later window
is `[max(B, W - (overlap_days - 1) days), until]`, both ends inclusive.
`until < B` and `until < W` are rejected; `until == W` is allowed to
re-harvest. An empty window (`noRecordsMatch` on the dated request) still
advances W to `until`. Parameters are frozen when the window row is inserted;
a resume must match endpoint/until/overlap-days/max-pages exactly and never
recomputes the range.

## Checkpoint and recovery boundary

1. A page is fully validated (size/encoding/XML, OAI shape, record datestamps
   inside the frozen window, revision identity/conflict rules, token cycle,
   page budget) before anything is written.
2. One SQLite transaction stores raw XML, parsed revisions, page order,
   request kind, the next token and the cycle entry, and updates the window
   counters. The next HTTP request is only sent after this commit.
3. Official `pages`/`revisions` are never touched per page. On restart the
   machine reads `next_token` and `pages_confirmed` and continues from the
   next page; confirmed pages are never re-requested. If the terminal page
   (empty next token) is already present, publish runs with zero HTTP
   requests.
4. The page budget and cycle set are persisted per traversal, so they survive
   process restarts. Each replacement traversal gets its own budget and cycle
   set; the window-level restart budget is separate and never reset by a
   restart.

## badResumptionToken restart (at most once, persisted)

A continuation response whose only OAI error is `badResumptionToken` consumes
the single window restart budget, then in one transaction deletes every
staging page, revision and seen-token row of the abandoned traversal, bumps
`sync_window.traversal`, zeros the counters and clears the token/error. The
identical frozen `from`/`until` first request is then replayed. Nothing from
the abandoned traversal is spliced into the replacement. A second
`badResumptionToken` (in any later process) fails the window; the budget is
stored in `sync_window.restarts_used` and cannot regenerate. A
`badResumptionToken` on the dated first request always fails.

`noRecordsMatch` is only accepted on the dated first request (empty window).
On a continuation request it is a hard failure, not an end marker.

## Publish transaction

When the traversal is complete, a single transaction: merges staging pages
into `pages` (deduplicated by sha256), inserts new revisions into `revisions`
after re-checking same-key conflicts (identical payloads de-duplicate,
conflicting payloads roll the whole transaction back), sets
`sync_meta.watermark = window_until`, and marks the window `complete`. Facts,
watermark and completion therefore commit or roll back together. A rolled
back publish leaves all staging intact and the window `active`, so a new
process can republish without HTTP. A failure while printing the summary
cannot undo an already committed transaction.

Overlap re-harvest never deletes identifiers that the source simply omits:
only an explicit `status="deleted"` tombstone removes a record from the
current view, and an older active revision cannot resurrect a newer
tombstone because `current_records` always picks the maximum datestamp.

## State transitions

```
                          bind + parameters valid
  (no row)            ───────────────────────────────► active/fetching
  active/fetching     ── terminal page stored ───────► active/publishing
  active/publishing   ── commit ─────────────────────► complete/done
  active (any stage)  ── hard failure ────────────────► failed
  failed              ── discard ─────────────────────► (row removed; W/facts kept)
  active              ── discard ─────────────────────► (row removed; W/facts kept)
  active              ── HTTP/OS error or process exit► active (last checkpoint retained)
```

## Test-only hooks

The machine reads three environment variables that exist solely to make
cross-process recovery deterministic in tests; they are not part of the
operational contract:

- `CATALOG_HARVEST_EXIT_AFTER_PAGE=<n>` — exit code 7 right after page `n`'s
  checkpoint commits.
- `CATALOG_HARVEST_EXIT_AFTER_RESTART_PAGE=<n>` — same, but only after a
  badResumptionToken restart traversal.
- `CATALOG_HARVEST_EXIT_AFTER_FINAL_PAGE=1` — exit code 9 after the terminal
  page checkpoint, before publish.
- `CATALOG_HARVEST_EXIT_BEFORE_PUBLISH=1` — exit code 8 immediately before
  the publish transaction.
- `CATALOG_HARVEST_PUBLISH_FAIL=1` — raise inside the publish transaction to
  verify joint rollback.

## Known limits

Overlap windows only cover a bounded late-arrival horizon chosen by
`overlap-days`. OAI-PMH pagination does not promise a single server-side
snapshot across requests, so records can change between traversals and
processes; the harvester preserves revision identity and tombstone semantics
but cannot reconstruct a point-in-time archive view. No scheduler, no
concurrent writers, no external/non-loopback endpoints, and no automatic HTTP
retries beyond the one persisted badResumptionToken restart.
