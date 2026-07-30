[![Testing](https://github.com/EBI-Metagenomics/ena-api-handler/actions/workflows/test.yml/badge.svg)](https://github.com/EBI-Metagenomics/ena-api-handler/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/EBI-Metagenomics/ena-api-handler/branch/master/graph/badge.svg)](https://codecov.io/gh/EBI-Metagenomics/ena-api-handler)

# ENA API Handler

`ena-api-handler` is a Python client for the ENA Portal API with:

- typed query models
- typed result models
- sync and async clients
- convenience helpers for common study, sample, run, and assembly lookups


## Requirements

- Python `3.13+`

## Installation

Install from the repository:

```bash
pip install git+ssh://git@github.com/EBI-Metagenomics/ena-api-handler.git
```

Or, if you are working locally in this repo:

```bash
pip install -e .
```

With `uv`:

```bash
uv sync
```

## Authentication

Most ENA Portal API reads do not require credentials, but the client supports basic auth for environments that need it.

You can provide credentials either through environment variables:

```bash
export ENA_API_USER="your-username"
export ENA_API_PASSWORD="your-password"
```

or explicitly in code:

```python
from ena_api_handler import ENAClient

client = ENAClient(username="your-username", password="your-password")
```

## Examples

The [`examples/`](examples/) folder has runnable scripts covering every use
case in this README: searching, query composition, all convenience methods,
async usage, result handling, field coercion/aliasing/filtering, and error
handling (including validation errors). See [`examples/README.md`](examples/README.md)
for the full index and how to run them.

## Quick Start

### Search for runs in a study

```python
from ena_api_handler import ENAClient
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery

with ENAClient() as client:
    results = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=[
            ENAReadRunFields.RUN_ACCESSION,
            ENAReadRunFields.FASTQ_FTP,
            ENAReadRunFields.BASE_COUNT,
        ],
        limit=5,
    )

for row in results:
    print(row.run_accession, row.fastq_ftp, row.base_count)
```

### Fetch a single sample

```python
from ena_api_handler import ENAClient

with ENAClient() as client:
    sample = client.get_sample("SAMN11835464")

if sample is not None:
    print(sample.sample_accession)
    print(sample.scientific_name)
```

## Core Concepts

### 1. `ENAClient`

`ENAClient` is the main entry point. It manages HTTP requests and returns typed Pydantic models.

It supports:

- `search()` for synchronous calls
- `search_async()` for asynchronous calls
- convenience methods such as `get_study()`, `get_sample()`, `get_run()`, and `get_study_runs()`

### 2. Result Types

Each ENA search target is represented by an `ENAPortalResultType`, for example:

- `READ_RUN`
- `READ_STUDY`
- `SAMPLE`
- `ANALYSIS`
- `STUDY`

Example:

```python
from ena_api_handler.models import ENAPortalResultType

result_type = ENAPortalResultType.READ_RUN
```

### 3. Typed Query Models

Generated query models give you typed fields for a specific result type and portal.

Example:

```python
from ena_api_handler.models.ena import ENAReadRunQuery

query = ENAReadRunQuery(
    study_accession="PRJEB1787",
    library_strategy="WGS",
)
```

Any field set to a non-`None` value is converted into an ENA query string joined with `AND`.

### 4. Typed Result Models

Search results come back as Pydantic models, not plain dictionaries.

Example:

```python
row = results[0]
print(type(row).__name__)
print(row.model_dump())
```

## Searching

### Basic typed search

```python
from ena_api_handler import ENAClient
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery

with ENAClient() as client:
    runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=[
            ENAReadRunFields.RUN_ACCESSION,
            ENAReadRunFields.SAMPLE_ACCESSION,
            ENAReadRunFields.LIBRARY_STRATEGY,
        ],
        limit=10,
    )
```

### Select fields explicitly

If `fields` is omitted, ENA decides what to return. For predictable downstream code, it is usually better to request the fields you need.

```python
fields = [
    ENAReadRunFields.RUN_ACCESSION,
    ENAReadRunFields.FASTQ_FTP,
]
```

You may also pass raw field names as strings:

```python
fields = ["run_accession", "fastq_ftp"]
```

### Search all results

Set `limit=0` to ask ENA for all matching rows.

```python
runs = client.search(
    result=ENAPortalResultType.READ_RUN,
    query=ENAReadRunQuery(study_accession="PRJEB1787"),
    limit=0,
)
```

### Search across portals

The client can try multiple ENA data portals in order and return the first non-empty result set.

```python
from ena_api_handler import ENAClient, ENAPortalDataPortal
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunQuery

with ENAClient() as client:
    rows = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        portals=(
            ENAPortalDataPortal.METAGENOME,
            ENAPortalDataPortal.ENA,
        ),
    )
```

Available portals:

- `ENAPortalDataPortal.ENA`
- `ENAPortalDataPortal.FAANG`
- `ENAPortalDataPortal.METAGENOME`
- `ENAPortalDataPortal.PATHOGEN`

### Fields/query validation

Query and fields models are generated per portal (e.g. `ENAReadRunQuery` vs.
`MetagenomeReadRunQuery`), so `search()` checks that any typed `query`/`fields`
you pass actually belong to the `(portal, result)` pair being queried. A
portal whose generated Query/Fields class doesn't match is silently skipped
in the fallback loop rather than sent a broken request; `search()` only
raises `ENAQueryValidationError` if **no** portal in `portals=` matches.
Untyped inputs — `ENARawQuery` and plain field-name strings — are always
considered compatible and are never rejected.

```python
from ena_api_handler import ENAQueryValidationError

try:
    client.search(
        result=ENAPortalResultType.STUDY,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),  # wrong query for STUDY
    )
except ENAQueryValidationError:
    print("fields/query don't match this result type")
```

## Composing Queries

The library supports query composition with `&`, `|`, and `~`.

### `AND`

```python
from ena_api_handler.models.ena import ENAReadRunQuery

query = (
    ENAReadRunQuery(study_accession="PRJEB1787")
    & ENAReadRunQuery(library_strategy="WGS")
)
```

### `OR`

```python
query = (
    ENAReadRunQuery(study_accession="PRJEB1787")
    | ENAReadRunQuery(secondary_study_accession="ERP001736")
)
```

### `NOT`

```python
query = ~ENAReadRunQuery(library_strategy="AMPLICON")
```

### Raw query fragments

Use `ENARawQuery` when you need syntax that is not represented by a generated query model, such as date comparisons.

```python
from ena_api_handler import ENARawQuery
from ena_api_handler.models.ena import ENAReadRunQuery

query = ENARawQuery("last_updated>=2024-01-01") & ENAReadRunQuery(
    study_accession="PRJEB1787"
)
```

## Convenience Methods

For common internal workflows, the client exposes higher-level helpers.

### `get_study()`

Looks up a study using a primary accession, a secondary accession, or both.

```python
with ENAClient() as client:
    study = client.get_study(primary_accession="PRJEB1787")
```

```python
with ENAClient() as client:
    study = client.get_study(secondary_accession="ERP001736")
```

### `check_study_availability()`

Determines whether a study is public, privately accessible with credentials,
or unavailable under either. `auth` is required: the method always probes
unauthenticated first, then retries with `auth` only if nothing public was
found, and returns `ENAAvailability.SUPPRESSED` if both probes come up empty.

```python
import httpx
from ena_api_handler import ENAAvailability

with ENAClient() as client:
    availability = client.check_study_availability(
        primary_accession="PRJEB1787",
        auth=httpx.BasicAuth("user", "pass"),
    )

if availability == ENAAvailability.PUBLIC:
    ...
elif availability == ENAAvailability.PRIVATE:
    ...
else:  # ENAAvailability.SUPPRESSED
    ...
```

### `get_sample()`

Looks up a sample by primary or secondary sample accession.

```python
sample = client.get_sample("SAMN11835464")
```

### `get_sample_studies()`

Looks up the set of `secondary_study_accession` values linked to a sample
(matching on either its primary or secondary sample accession). Defaults to
searching `read_run`; pass `result=` to search a different result type.

```python
studies = client.get_sample_studies("SAMN11835464")
```

### `get_run()`

Fetches a single run by accession. The returned model carries a computed
`raw_data_size` field — the sum of `fastq_bytes`, falling back to
`submitted_bytes` if `fastq_bytes` isn't present.

```python
run = client.get_run("ERR1701760")
```

### `get_study_runs()`

Fetches all runs for a study. Each returned run also carries the computed
`raw_data_size` field described above.

By default it filters out `AMPLICON` runs.

```python
runs = client.get_study_runs("PRJEB1787")
```

Disable that filter if needed:

```python
runs = client.get_study_runs(
    "PRJEB1787",
    filter_assembly_runs=False,
)
```

Restrict results to known run accessions:

```python
runs = client.get_study_runs(
    "PRJEB1787",
    filter_accessions=["ERR123", "ERR456"],
)
```

### `get_study_assemblies()`

Fetches assemblies linked to a study.

By default it restricts to `assembly_type="primary metagenome"`.

```python
assemblies = client.get_study_assemblies("ERP124933")
```

Allow non-primary assemblies:

```python
assemblies = client.get_study_assemblies(
    "ERP124933",
    allow_non_primary_assembly=True,
)
```

### `get_assembly()` and `get_assembly_from_sample()`

```python
assembly = client.get_assembly("ERZ1669402")
```

```python
assembly = client.get_assembly_from_sample("SAMEA1234567")
```

### Updated-record helpers

These helpers are useful for incremental sync workflows:

- `get_updated_studies()`
- `get_updated_runs()`
- `get_updated_assemblies()`
- `get_updated_tpa_assemblies()`

Example:

```python
updated_runs = client.get_updated_runs("2024-01-01")
```

These pass `limit=0` internally, which fetches *all* matching results (see
[Searching](#searching)). A broad cutoff date can match a large fraction of
ENA's archive and take a long time to download, so prefer a recent date
for incremental syncs.

## Async Usage

The async API mirrors the sync API closely.

### Async search

```python
from ena_api_handler import ENAClient
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery

async with ENAClient() as client:
    runs = await client.search_async(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=[ENAReadRunFields.RUN_ACCESSION],
        limit=5,
    )
```

### Async convenience methods

```python
async with ENAClient() as client:
    sample = await client.get_sample_async("SAMN11835464")
    runs = await client.get_study_runs_async("PRJEB1787")
```

Available async helpers mirror the sync versions:

- `get_study_async()`
- `check_study_availability_async()`
- `get_sample_async()`
- `get_sample_studies_async()`
- `get_run_async()`
- `get_study_runs_async()`
- `get_study_assemblies_async()`
- `get_assembly_async()`
- `get_assembly_from_sample_async()`
- `get_updated_studies_async()`
- `get_updated_runs_async()`
- `get_updated_assemblies_async()`
- `get_updated_tpa_assemblies_async()`

## Result Handling

Results are Pydantic models, so common operations include:

### Access attributes

```python
print(run.run_accession)
print(run.base_count)
```

### Convert to dictionaries

```python
payload = run.model_dump()
```

### Work with partial field sets

If you request only some fields, unrequested fields will typically be `None`.

```python
with ENAClient() as client:
    runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=[ENAReadRunFields.RUN_ACCESSION],
        limit=1,
        field_coercions=None,
    )

row = runs[0]
print(row.run_accession)
print(row.fastq_ftp)  # usually None if not requested
```

## Field Coercion, Aliasing, and Filtering

The low-level search methods support a few useful response-processing controls.

### Field coercion

By default the client coerces some fields into more useful types: `base_count`/`read_count`/`tax_id`/`genetic_code`/`merged_tax_id`/`status` (numeric strings → `int`), and `location`/`location_start`/`location_end` (a `"<lat> <N|S> <lon> <E|W>"` string → a `(lat, lon)` tuple of signed floats).

Coercion is applied via `model_copy()` after validation, so it doesn't change the model's
declared field type. Type checkers (mypy/pyright) will still see the original declared type
(e.g. `Optional[str]`) for a coerced field — use `typing.cast()` if you need an accurate
static type for the coerced value.

Disable that behavior if you want raw API values:

```python
rows = client.search(
    result=ENAPortalResultType.READ_RUN,
    query=ENAReadRunQuery(study_accession="PRJEB1787"),
    field_coercions=None,
)
```

### Field aliases

You can rename fields before validation.

```python
rows = client.search(
    result=ENAPortalResultType.STUDY,
    query=ENARawQuery('study_accession="PRJEB1787"'),
    field_aliases={
        "study_description": "description",
        "study_name": "study_alias",
    },
)
```

### Exclude rows

You can drop rows matching field-value pairs.

```python
rows = client.search(
    result=ENAPortalResultType.READ_RUN,
    query=ENAReadRunQuery(study_accession="PRJEB1787"),
    exclude={"library_strategy": "AMPLICON"},
)
```

## Error Handling

The client raises:

- `ENAClientError` for API errors and unsuccessful responses
- `ENAQueryValidationError` when typed `fields`/`query` don't match `result` for any queried portal (see [Fields/query validation](#fieldsquery-validation))
- `ENAAvailabilityError` when `raise_on_empty=True` and no portal returns data

Example:

```python
from ena_api_handler import ENAAvailabilityError, ENAClient, ENAClientError

try:
    with ENAClient() as client:
        rows = client.search(
            result=ENAPortalResultType.READ_RUN,
            query=ENAReadRunQuery(study_accession="PRJEB1787"),
            raise_on_empty=True,
        )
except ENAAvailabilityError:
    print("No results found in any requested portal")
except ENAClientError as exc:
    print(f"ENA request failed: {exc}")
```

## Development

Install development dependencies with `uv`:

```bash
uv sync --extra dev
```

Run the default test suite:

```bash
uv run pytest -q
```

Integration tests are marked `integration` and are deselected by default. Run them explicitly when you want live ENA API coverage:

```bash
uv run pytest -q -m integration
```

Install pre-commit hooks:

```bash
uv run pre-commit install
```

## Import Reference

Top-level exports:

```python
from ena_api_handler import (
    ENAAvailabilityError,
    ENABaseQuery,
    ENAClient,
    ENAClientError,
    ENAQueryClause,
    ENAQueryNot,
    ENAQueryPair,
    ENARawQuery,
    ENAPortalDataPortal,
)
```

Common generated imports:

```python
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery
```

## Notes

- This README focuses on using the library.
- Model-generation details are intentionally excluded here — see [`scripts/README.md`](scripts/README.md) for how the typed models in `ena_api_handler.models` are generated and kept up to date with the ENA Portal API.
