"""Post-processing utilities for ENA Portal API response rows."""

from __future__ import annotations

from typing import Any, Callable

from pydantic import BaseModel

# ── Default field coercions ────────────────────────────────────────────────────
# Fields that ENA returns as strings but are semantically numeric.
# Applied to raw response dicts before Pydantic model validation.
# Pass ``field_coercions=None`` to ``search()`` / ``search_async()`` to disable.


def _parse_location(value: Any) -> tuple[float, float] | None:
    """Parse an ENA latlon string, e.g. "18.5839 N 66.4727 E", into a signed (lat, lon) tuple."""
    if not value:
        return None
    lat_str, lat_hem, lon_str, lon_hem = value.split()
    lat_hem, lon_hem = lat_hem.upper(), lon_hem.upper()
    if lat_hem not in ("N", "S") or lon_hem not in ("E", "W"):
        raise ValueError(f"invalid hemisphere markers in location: {value!r}")
    lat = float(lat_str) * (-1 if lat_hem == "S" else 1)
    lon = float(lon_str) * (-1 if lon_hem == "W" else 1)
    return (lat, lon)


DEFAULT_FIELD_COERCIONS: dict[str, Callable[[Any], Any]] = {
    "base_count": lambda v: int(v) if v not in (None, "") else None,
    "read_count": lambda v: int(v) if v not in (None, "") else None,
    "tax_id": lambda v: int(v) if v not in (None, "") else None,
    "genetic_code": lambda v: int(v) if v not in (None, "") else None,
    "merged_tax_id": lambda v: int(v) if v not in (None, "") else None,
    "status": lambda v: int(v) if v not in (None, "") else None,
    "location": _parse_location,
    "location_start": _parse_location,
    "location_end": _parse_location,
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


def compute_raw_data_size(row: BaseModel | dict[str, Any]) -> int | None:
    """Sum semicolon-separated byte counts from fastq_bytes, falling back to submitted_bytes."""

    def _get(field: str) -> Any:
        return row.get(field) if isinstance(row, dict) else getattr(row, field, None)

    for field in ("fastq_bytes", "submitted_bytes"):
        value = _get(field)
        if value:
            return sum(int(s) for s in value.split(";") if s)
    return None
