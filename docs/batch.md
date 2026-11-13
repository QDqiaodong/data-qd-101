# Persistent batch orchestration — sync → warehouse → reports

Entry point: `python -m catalog_harvest.batch {run,status}`.
Library API: `catalog_harvest.batch.run_batch(...)` and
`catalog_harvest.batch.status_batch(...)`.
Deterministic demonstration: `python -m catalog_harvest.batch_demo`.

This module *chains* the three existing stages; it does not reimplement any
business rule. Sync still goes through `catalog_harvest.sync.run_sync`,
the warehouse through `catalog_harvest.warehouse.build_warehouse`, and each
report through `catalog_harvest.affinity.analyze` +
`publish_report`. Keys, canonicalization, tombstone rules, watermarks,
taxonomy semantics, family definitions, exact Fisher/BH arithmetic, and
atomic replacement behavior are unchanged.

## 1. Batch identity and frozen inputs

A batch is named by a caller-supplied `run_id` matching
`[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and lives in `<workdir>/<run_id>`.
The directory is created once, fully initialised in a private staging
directory and renamed into place, so a half-initialised batch is never
visible. The run-id is never a path.

On first creation the exact bytes of three inputs are copied into
`inputs/` and their SHA256 recorded in `batch.json`:

| Input | Frozen copy | Guarantee |
| --- | --- | --- |
| batch plan | `inputs/plan.json` | Parsed once, validated strictly. |
| taxonomy | `inputs/taxonomy.json` | Byte-identical to what the build reads. |
| affinity analysis plan | `inputs/affinity_plan.json` | Same bytes are parsed and hashed by the report. |

Resuming requires the supplied plan file to hash to the frozen plan; a
different plan, a different run-id, or a taxonomy/affinity file whose content
changed since creation is rejected before any stage runs. Frozen bytes, not
the external files, are authoritative once the batch exists, so the batch
remains reproducible after the source files move or change.

The plan is strict UTF-8 JSON with duplicate object keys rejected and exactly
these top-level keys:

```json
{
  "schema_version": 1,
  "sync": {"endpoint": "http://127.0.0.1:PORT/oai", "start_date": "2026-01-01",
           "until": "2026-01-05", "overlap_days": 2, "max_pages": 20},
  "warehouse": {"start": "2026-01-01", "end": "2026-01-08",
                "taxonomy_file": "data/taxonomy_demo.json"},
  "reports": ["2026-01-03", "2026-01-04"],
  "affinity": {"plan_file": "data/affinity_plan_example.json"}
}
```

`data/batch_plan_example.json` is the same shape with a placeholder loopback
endpoint (not a live service). `sync.endpoint` must pass the existing loopback
transport check at batch creation, before the batch directory is created.

Report dates must satisfy `warehouse.start <= date < warehouse.end`, be
unique and non-empty; their order is preserved as the execution order.

## 2. Stages, ledger and evidence

Stages execute strictly in order:

```
sync → warehouse → report:<date> (one task per plan date) → manifest
```

The SQLite ledger `state.sqlite` holds one `stage` row per task with its
planned intent (JSON), position, state, attempt counter, last error and
artifact evidence; an `attempt` row per execution (start/finish, outcome,
error); and a `recovery_event` row for every reattachment, resume, requeue or
verification failure. `batch.json` records format, run-id, plan digest and
input digests. Everything uses the standard library and SQLite only.

States are `pending`, `running`, `succeeded`, `failed` and `blocked`.
Execution is sequential, so a stage may start only after its immediate
predecessor is `succeeded`. A failed stage immediately marks every later
unexecuted stage `blocked` for that run; downstream stages never run against
an unverified upstream artifact.

Artifact evidence is always derived from the artifact itself, never from a
return value alone:

| Stage | Evidence |
| --- | --- |
| sync | Exactly one `sync_window` row in `complete`/`done` state; `sync_meta` endpoint, base date and watermark equal the frozen plan; frozen window parameters match; formal pages/revisions revalidated by the warehouse reader and hashed into `source_facts_sha256`; plus page/revision/active/deleted counts and the file SHA256. |
| warehouse | `wh_meta.format/format_version`, build range, `source_facts_sha256` and `taxonomy_sha256` all equal this batch's; `quick_check` is `ok`; plus version count and the file SHA256. |
| report | `format`, `schema_version`, report date, warehouse format/version and range, `source_facts_sha256`, `taxonomy_sha256`, `plan_bytes_sha256`, and the report's own recomputed `metadata_sha256`, `selected_relations_sha256` and `record_evidence_sha256`; plus the file SHA256. |
| manifest | Published only after every other stage is `succeeded`; derived from the verified per-stage evidence. |

A path that merely exists is never success. A file that is present but
foreign or damaged fails the stage instead of being overwritten.

## 3. The commit / ledger gap

Each stage is: run the shipped library call → commit/publish its artifact →
write evidence to the ledger. A crash between the last two leaves a
`succeeded`-looking artifact with a `running` (or still `pending`) ledger row.
The next `run` reconciles under the lock:

- **`succeeded`** rows are re-verified against their recorded artifact SHA256.
  A mismatch or missing artifact fails loudly in that run; the stage is never
  re-run (and the artifact never overwritten), so every later run repeats the
  same verification failure until the artifact is restored or the operator
  starts a new run-id.
- **`running`/`pending`** rows whose artifact proves this batch's identity and
  logical content are *reattached*: the row becomes `succeeded`, the evidence
  is recomputed from the artifact, and a `reattached` recovery event is
  recorded. A completed sync window is therefore not reopened and no second
  HTTP window is requested; a replaced warehouse is not rebuilt; a published
  report is not recomputed.
- **`running`** rows with no committed artifact return to `pending` and the
  stage simply resumes through the shipping library call (sync resumes from
  its persisted checkpoint/token; warehouse/report start fresh). This also
  never counts the dead process's open attempt: the attempt row is closed as
  `interrupted` and the next real execution starts from `pending`.
- A foreign or corrupt artifact fails the stage and blocks downstream stages.

The SQLite sync machine's own publish is a single transaction, so the only
gap for it is between commit and the batch ledger update — exactly the case
above. Test-only hooks (not part of the operational contract):
`CATALOG_HARVEST_BATCH_BARRIER=<event>` exits the process in that gap.
`after-sync`, `after-warehouse` and `after-report:<date>` stop immediately
after the artifact is published and before its evidence row; `before-<stage>`
stops after the evidence row says `running` and before the stage call;
`CATALOG_HARVEST_BATCH_FAIL_STAGE=<stage>` injects a failure for that stage.

## 4. Failure, retry and partial reports

- A failure inside any stage records `failed` with the exception type and
  message, keeps prior attempts, blocks downstream stages and stops the run
  (CLI exit 3). `status` then shows the exact stage and error.
- A partial report group does not publish a manifest. The manifest stage is
  refused unless every other stage is `succeeded`, and `status` reports the
  batch as incomplete with no delivery manifest.
- A later explicit `run` re-queues `failed` and `blocked` stages (a
  `requeued` recovery event) while every `succeeded` stage stays skipped and
  verified. Consequently a failed report is retried alone: successful reports
  keep their exact bytes and are not recomputed, and the downstream
  unexecuted reports run normally. No successful task is ever repeated because
  a sibling failed.
- Retrying a failed sync does not bypass its failure state: the sync machine
  still refuses the failed window before any HTTP request until an operator
  discards it with `python -m catalog_harvest.sync discard`.

## 5. Complete delivery manifest

`manifest.json` is written only when every stage has succeeded. It contains
the run-id, plan digest, frozen input digests, the plan (including sync
parameters, warehouse range and dates), stage order and attempt counts, the
sync evidence (endpoint, frozen window, watermark, counts, source-facts
digest, file digest), the warehouse evidence (range, digests, version count,
file digest), and per-report date, path, SHA256, population `N`, family size
and clue count. It is written through a same-directory temporary file with
`fsync` and then `os.replace`.

## 6. Contention and `status`

Single writer per batch. `run` takes an exclusive non-blocking `flock` on
`state.lock` (opened with `O_NOFOLLOW`); a second process is refused
immediately with exit 2, before any HTTP request or ledger change, and is
never allowed to wait unboundedly. Different run-ids never share state.

`status --workdir W --run-id R` is strictly read-only: it opens `batch.json`
and `state.sqlite` with `mode=ro` and `PRAGMA query_only=ON`, never takes the
lock, never writes, and never performs network access. It returns stage
states, intents, recorded artifact digests, bounded attempt history, recovery
events and whether a valid delivery manifest is present. `status` also refuses
a directory whose manifest exists while a stage is not succeeded, or whose
ledger stages do not match the frozen plan.

## 7. Layout

```
<workdir>/<run_id>/
  batch.json          format, run-id, plan digest, frozen input digests
  inputs/plan.json    exact frozen plan bytes
  inputs/taxonomy.json
  inputs/affinity_plan.json
  state.sqlite        stage / attempt / recovery_event ledger
  state.lock          exclusive writer lock
  catalog.sqlite      sync stage artifact
  warehouse.sqlite    warehouse stage artifact
  reports/<date>.json one report per planned date
  manifest.json       complete-batch delivery manifest
```

## 8. Limits

One loopback endpoint per batch and one date window per batch (the existing
sync contract). No scheduler, no daemon, no automatic HTTP retries, no
concurrent writers, no partial-batch manifest, and no attempt to reinterpret
or repair artifacts produced outside the batch. A corrupted batch directory
is reported and left untouched for the operator; a changed input requires a
new run-id.
