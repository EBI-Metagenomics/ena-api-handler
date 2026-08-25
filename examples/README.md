# Examples

Runnable scripts covering every `ena-api-handler` use case. Each script makes
live network calls to the ENA Portal API, so you'll need network access to
run them (no credentials required — see the main [README](../README.md#authentication)
for the private-data case).

Run any of them with:

```bash
uv run python examples/<script>.py
```

| Script | Covers |
| --- | --- |
| [`01_quickstart.py`](01_quickstart.py) | Minimal search + single-record fetch |
| [`02_searching.py`](02_searching.py) | `search()`: field selection, `limit=0`, cross-portal fallback, `ENAQueryValidationError` |
| [`03_composing_queries.py`](03_composing_queries.py) | `&` / `|` / `~` query composition and `ENARawQuery` |
| [`04_convenience_methods.py`](04_convenience_methods.py) | Every sync convenience helper (`get_study`, `get_sample`, `get_run`, `get_study_runs`, `get_study_assemblies`, `get_assembly`, `get_assembly_from_sample`, `get_updated_*`, `check_study_availability`) |
| [`05_async_usage.py`](05_async_usage.py) | `search_async()` and `_async` convenience twins |
| [`06_result_handling_and_processing.py`](06_result_handling_and_processing.py) | Attribute access, `model_dump()`, field coercion, field aliasing, row exclusion |
| [`07_error_handling.py`](07_error_handling.py) | `ENAClientError`, `ENAQueryValidationError`, `ENAAvailabilityError`, and Pydantic `ValidationError` |

## Linting and type checking

These scripts are linted and type-checked the same way as the rest of the
codebase:

```bash
uv run ruff check examples
uv run ruff format --check examples
uv run mypy examples
```
