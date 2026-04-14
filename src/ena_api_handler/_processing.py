"""Post-processing utilities for ENA Portal API response rows."""

from __future__ import annotations

from typing import Any, Callable

# ── Default field coercions ────────────────────────────────────────────────────
# Fields that ENA returns as strings but are semantically numeric.
# Applied to raw response dicts before Pydantic model validation.
# Pass ``field_coercions=None`` to ``search()`` / ``search_async()`` to disable.

DEFAULT_FIELD_COERCIONS: dict[str, Callable[[Any], Any]] = {
    "base_count": lambda v: int(v) if v not in (None, "") else None,
    "read_count": lambda v: int(v) if v not in (None, "") else None,
}


# ── Row transformations ────────────────────────────────────────────────────────


def apply_aliases(row: dict[str, Any], aliases: dict[str, str]) -> dict[str, Any]:
    """Rename keys in a result row according to the alias map."""
    if not aliases:
        return row
    return {aliases.get(k, k): v for k, v in row.items()}


def apply_coercions(
    row: dict[str, Any],
    coercions: dict[str, Callable[[Any], Any]],
) -> dict[str, Any]:
    """Apply type coercions to specific fields. Failures are silently ignored."""
    if not coercions:
        return row
    result = dict(row)
    for field, coerce in coercions.items():
        if field in result:
            try:
                result[field] = coerce(result[field])
            except (ValueError, TypeError):
                pass
    return result


def apply_exclude(
    rows: list[dict[str, Any]],
    exclude: dict[str, Any],
) -> list[dict[str, Any]]:
    """Filter out rows where any field matches the exclusion criteria."""
    if not exclude:
        return rows
    return [row for row in rows if not any(row.get(k) == v for k, v in exclude.items())]
