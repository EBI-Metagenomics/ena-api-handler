# Model Generation

`generate_models.py` keeps `src/ena_api_handler/models/` in sync with the fields the ENA
Portal API actually supports, so this client can be updated to reflect new/changed/removed
ENA fields without hand-editing generated code.

It runs in two phases: **fetch** (talk to ENA, update `snapshots/`) and **generate** (turn
`snapshots/` into Python). They can be run together or independently.

## Usage

```bash
# Full run: fetch latest fields from ENA, regenerate anything that changed.
uv run scripts/generate_models.py

# Preview what would change without writing any files.
uv run scripts/generate_models.py --dry-run

# Regenerate models from the snapshots already on disk — no network calls.
# Useful after manually editing a snapshot, or to reproduce generated code
# from a fresh checkout.
uv run scripts/generate_models.py --skip-fetch

# Always rewrite snapshots, even if the field set looks unchanged
# (e.g. field descriptions changed but columnIds didn't).
uv run scripts/generate_models.py --ignore-snapshots

# Limit the run to specific portals and/or result types.
uv run scripts/generate_models.py --portals ena,faang
uv run scripts/generate_models.py --results read_run,study
uv run scripts/generate_models.py --portals ena --results read_run --dry-run
```

Run `uv run scripts/generate_models.py --help` for the full flag reference.

## Phase 1: Fetch

For each `(portal, result_type, endpoint)` combination — `endpoint` being
`searchFields` or `returnFields` — the script:

1. Discovers all available result types via `GET {BASE_URL}/results`.
2. Requests `GET {BASE_URL}/{endpoint}?result={result}&dataPortal={portal}&format=json`,
   using an `asyncio.Semaphore(MAX_CONCURRENT)` (currently 10) to bound concurrent requests.
3. Normalizes each field entry to `{"columnId", "description", "type"}`, sorted by `columnId`.
4. Compares the new field set against the existing snapshot at
   `snapshots/<portal>/<result>/<endpoint>.json`, by `columnId` only (added/removed).
   If nothing was added or removed, the snapshot is left untouched — this keeps diffs small
   and avoids no-op rewrites when only response ordering changes.
5. If the field set changed (or `--ignore-snapshots` was passed, or there was no existing
   snapshot), the snapshot file is written and `(portal, result)` is marked as changed for
   phase 2.

Notes on fetch behavior:
- A `204 No Content` response is treated as "zero fields available" (not an error).
- Any other HTTP error or exception for a given `(endpoint, result, portal)` combination is
  logged and that combination is skipped — it does not abort the whole run.
- `BASE_URL` is `https://www.ebi.ac.uk/ena/portal/api/`; `ALL_PORTALS` (the default set of
  portals) is `ena, faang, metagenome, pathogen`.
- If nothing changed across every combination checked, phase 2 (generate) is skipped
  entirely — unless `--ignore-snapshots` was passed.
- `--skip-fetch` bypasses this phase entirely and discovers result types by walking the
  existing `snapshots/` directory tree instead of calling ENA.

## Phase 2: Generate

For every `(portal, result)` pair that has *both* a `searchFields.json` and a
`returnFields.json` snapshot on disk, the script writes
`src/ena_api_handler/models/<portal>/<result>.py` containing three classes:

- **`{Prefix}{ResultSegment}Fields(str, Enum)`** — one member per `returnFields` entry.
  Member name is `columnId.upper()`; value is the raw `columnId`; the field's `description`
  (if any) is appended as an inline comment.
- **`{Prefix}{ResultSegment}Query(ENABaseQuery)`** — one optional field per `searchFields`
  entry, named exactly as the ENA `columnId`, typed via `ENA_TYPE_MAP` (`text`→`str`,
  `numeric`→`float`, `date`→`str`, `boolean`→`bool`; anything unmapped defaults to `str`),
  with the ENA `description` as the Pydantic field description.
- **`{Prefix}{ResultSegment}Result(BaseModel)`** — same optional/typed fields as `Query`,
  but built from `returnFields`, with `model_config = ConfigDict(extra='allow')` so a result
  row remains valid even if it carries fields not currently modeled.

`{Prefix}` comes from `PORTAL_PREFIX` (`ena`→`ENA`, `faang`→`FAANG`, `metagenome`→`Metagenome`,
`pathogen`→`Pathogen`; anything else falls back to `portal.capitalize()`).
`{ResultSegment}` is the result type in PascalCase (`result_to_class_segment`), e.g.
`read_run` → `ReadRun`, `tls_set` → `TlsSet`.

It then (re)writes two index files, always, even if no individual model file changed:

- `src/ena_api_handler/models/<portal>/__init__.py` — imports and re-exports the
  `Fields`/`Query`/`Result` classes for every result type available under that portal.
- `src/ena_api_handler/models/__init__.py` — imports every portal's classes, and defines:
  - `ENAPortalResultType(str, Enum)` — union of every result type across all portals.
  - `RESULT_MODELS: dict[(ENAPortalDataPortal, ENAPortalResultType), type[BaseModel]]` —
    lookup from `(portal, result_type)` to that pair's generated `Result` class.

`--dry-run` logs what would be written (per model file and per index file) without touching
disk. Every generated file starts with an `# AUTO-GENERATED ... — do not edit manually.`
header — hand edits belong in `client.py`, `query.py`, `_processing.py`, or `types.py`
instead, never in `models/`.

## Directory layout this produces

```
snapshots/
  <portal>/                      # ena | faang | metagenome | pathogen
    <result_type>/                # e.g. read_run, study, sample, assembly, analysis, ...
      searchFields.json           # list[{columnId, description, type}], sorted by columnId
      returnFields.json           # same shape, for returnable/response fields

src/ena_api_handler/models/
  <portal>/
    __init__.py                   # generated: re-exports every result type's classes
    <result_type>.py               # generated: Fields / Query / Result classes
  __init__.py                     # generated: ENAPortalResultType + RESULT_MODELS
```

Snapshots are committed to the repo. They're both the cache that makes `--skip-fetch` /
diffing possible, and the audit trail of what ENA's schema looked like at each point in time
— `git diff snapshots/` after a fetch is the fastest way to see exactly which ENA fields were
added or removed.

## Testing

`tests/test_generator.py` unit-tests `result_to_class_segment`, `class_prefix`, `py_type`,
snapshot load/write, and the generated source for a given fixture snapshot (class names,
enum members, field types, descriptions). These tests build tiny snapshots under `tmp_path`
and never touch the real `snapshots/` directory or the network — safe to run as part of the
normal (non-`integration`) test suite.

## When to run this

There's currently no CI job or scheduled workflow that runs the generator automatically.
Run it manually — `uv run scripts/generate_models.py` — whenever you want to pick up ENA
Portal API field changes, then review the resulting diff under `snapshots/` and
`src/ena_api_handler/models/` before committing.