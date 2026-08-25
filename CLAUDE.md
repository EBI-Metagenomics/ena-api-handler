# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Python client for the ENA (European Nucleotide Archive) Portal API, used by the EMBL-EBI MGnify team. It has two halves:

- **Generated code** (`src/ena_api_handler/models/`) — typed Pydantic models for every `(data portal, result type)` pair ENA exposes, produced by a codegen script from live API metadata.
- **Handwritten code** (`src/ena_api_handler/client.py`, `query.py`, `_processing.py`, `types.py`) — the `ENAClient` (sync + async) and a composable query DSL built on top of the generated models.


## Commands

```bash
uv sync --extra dev              # install deps (Python 3.13+ required)

uv run pytest -q                 # run the default test suite (integration tests deselected)
uv run pytest -q -m integration  # run integration tests (hits the real ENA API)
uv run pytest -q tests/test_client.py::test_name   # run a single test

uv run pre-commit install        # install pre-commit hooks (ruff check --fix, ruff format)
uv run ruff check .
uv run ruff format .
```

Integration tests are marked `integration` in `pyproject.toml` and excluded by default via `addopts = "-m 'not integration'"`.

## Regenerating models from ENA

```bash
uv run scripts/generate_models.py                # fetch latest fields from ENA, regenerate changed models
uv run scripts/generate_models.py --dry-run       # preview without writing
uv run scripts/generate_models.py --skip-fetch    # regenerate from existing snapshots only, no network calls
uv run scripts/generate_models.py --portals ena,faang --results read_run,study
```

This is the "auto-update" mechanism the whole project exists for. Two phases:

1. **Fetch** — hits ENA Portal API's `/results`, `/searchFields`, `/returnFields` endpoints across all data portals (`ena`, `faang`, `metagenome`, `pathogen`) and result types (`read_run`, `study`, `sample`, `assembly`, `analysis`, etc.), diffs the response against `snapshots/<portal>/<result>/{searchFields,returnFields}.json`, and only rewrites snapshots whose field set actually changed.
2. **Generate** — reads the (possibly updated) snapshots and rewrites `src/ena_api_handler/models/<portal>/<result>.py`, plus the `<portal>/__init__.py` and top-level `models/__init__.py` index files.

`tests/test_generator.py` unit-tests the generation functions directly against fixture snapshots (no network, no real `snapshots/` dir touched). There is no CI workflow that runs the generator automatically — regeneration is currently a manual step.

See [`scripts/README.md`](scripts/README.md) for the full writeup: per-phase behavior, generated class/file naming rules, the `snapshots/`/`models/` directory layout, and error handling.

## Architecture

**Per-`(portal, result_type)` generated file** (e.g. `models/ena/read_run.py`) contains three classes, named `{Prefix}{ResultSegment}...` (e.g. `ENAReadRunFields`, `ENAReadRunQuery`, `ENAReadRunResult`; portal prefixes: `ENA`, `FAANG`, `Metagenome`, `Pathogen`):
- `*Fields(str, Enum)` — returnable column names, for the `fields=` argument of a search.
- `*Query(ENABaseQuery)` — searchable fields as typed Pydantic model attributes; setting any field builds a query clause.
- `*Result(BaseModel)` — typed response row (`extra='allow'` so unrequested/unknown fields don't fail validation).

`models/__init__.py` (also generated) aggregates all of these into `ENAPortalResultType` (enum of every result type) and `RESULT_MODELS: dict[(portal, result_type), model class]`.

**Query DSL** (`query.py`): `ENABaseQuery` (base of every generated `*Query` model) and `ENARawQuery` (hand-written escape hatch for expressions the typed models can't express, e.g. date ranges) both implement `ENAQueryClause.to_query_string()` and support `&` / `|` / `~` operators that compose into `ENAQueryPair` / `ENAQueryNot` trees, ultimately rendered to the ENA Portal API's `query` string syntax.

**`ENAClient`** (`client.py`):
- Sync and async APIs (`search`/`search_async`, `get_study`/`get_study_async`, etc.) with matching context-manager support.
- `search()`/`search_async()` are the low-level entry points: take a `result` enum, an `ENABaseQuery`/`ENAQueryClause`, optional `fields`, and can query multiple `portals` in sequence, falling back until one returns rows.
- Convenience wrappers (`get_study`, `get_sample`, `get_run`, `get_study_runs`, `get_study_assemblies`, `get_assembly`, `get_assembly_from_sample`, `get_updated_studies/runs/assemblies/tpa_assemblies`, each with an `_async` twin) build a query and call `search`/`search_async` internally. `get_study`/`get_study_async` accept either `primary_accession` or `secondary_accession` (or both) and try `READ_STUDY` then `ANALYSIS_STUDY` result types.
- Response post-processing (`_processing.py`): `field_coercions` (default: coerce `base_count`/`read_count` strings to `int`), `field_aliases` (rename response keys before model validation), `exclude` (drop rows matching field/value pairs) — all pluggable via `search()` kwargs.
- Credentials: `ENA_API_USER`/`ENA_API_PASSWORD` env vars, or explicit `username`/`password` args (explicit wins). Most reads work unauthenticated.
- Raises `ENAClientError` on API/HTTP errors, `ENAAvailabilityError` when `raise_on_empty=True` and every queried portal returned nothing.

## Notes

- The README documents usage in depth (quickstart, async usage, field coercion/aliasing, error handling, full import reference) — read it before re-explaining client usage in docs. It intentionally excludes model-generation details and points to `scripts/README.md` for those.