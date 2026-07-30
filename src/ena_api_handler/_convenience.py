from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.query import ENABaseQuery, ENAQueryClause, ENARawQuery
from ena_api_handler.types import ENAPortalDataPortal

CONVENIENCE_PORTALS = (ENAPortalDataPortal.METAGENOME, ENAPortalDataPortal.ENA)

STUDY_RESULT_TYPE_ALIASES: list[tuple[Enum, dict[str, str] | None]] = [
    (ENAPortalResultType.READ_STUDY, None),
    (ENAPortalResultType.ANALYSIS_STUDY, None),
    (
        ENAPortalResultType.STUDY,
        {"study_description": "description", "study_name": "study_alias"},
    ),
]

STUDY_AVAILABILITY_RESULT_TYPES: tuple[Enum, ...] = (
    ENAPortalResultType.READ_STUDY,
    ENAPortalResultType.ANALYSIS_STUDY,
    ENAPortalResultType.STUDY,
)


def normalize_cutoff_date(cutoff_date: str | date | datetime) -> str:
    """Validate and normalize a cutoff date to an ISO ``YYYY-MM-DD`` string.

    Accepts a ``date``, ``datetime`` (truncated to its date), or an ISO
    ``YYYY-MM-DD`` string. Raises ``ValueError`` on any other format.
    """
    if isinstance(cutoff_date, datetime):
        return cutoff_date.date().isoformat()
    if isinstance(cutoff_date, date):
        return cutoff_date.isoformat()
    return date.fromisoformat(cutoff_date).isoformat()


def accession_query(
    primary_accession: str | None,
    secondary_accession: str | None,
) -> ENABaseQuery | ENAQueryClause:
    """Build the ``study_accession=... | secondary_study_accession=...`` clause.

    Used by both the study-lookup and study-availability convenience methods.
    """
    if not primary_accession and not secondary_accession:
        raise ValueError(
            "Either primary_accession or secondary_accession must be provided"
        )

    query_parts: list[ENAQueryClause] = []
    if primary_accession:
        query_parts.append(ENARawQuery(f'study_accession="{primary_accession}"'))
    if secondary_accession:
        query_parts.append(
            ENARawQuery(f'secondary_study_accession="{secondary_accession}"')
        )

    query: ENABaseQuery | ENAQueryClause = query_parts[0]
    for part in query_parts[1:]:
        query = query | part
    return query


def sample_accession_query(sample_accession: str) -> ENAQueryClause:
    """Build the ``sample_accession=... | secondary_sample_accession=...`` clause."""
    return ENARawQuery(f'sample_accession="{sample_accession}"') | ENARawQuery(
        f'secondary_sample_accession="{sample_accession}"'
    )


def study_accession_query(study_accession: str) -> ENAQueryClause:
    """Build the ``study_accession=... | secondary_study_accession=...`` clause."""
    return ENARawQuery(f'study_accession="{study_accession}"') | ENARawQuery(
        f'secondary_study_accession="{study_accession}"'
    )


def study_runs_search_args(
    study_accession: str,
    fields: list[Enum | str] | None,
    filter_assembly_runs: bool,
) -> tuple[ENAQueryClause, list[Enum | str] | None, dict[str, Any] | None]:
    """Build the query/fields/exclude args shared by ``get_study_runs`` and its async twin."""
    query = study_accession_query(study_accession)
    search_fields = list(fields) if fields is not None else None
    if (
        filter_assembly_runs
        and search_fields is not None
        and all(
            (f.value if isinstance(f, Enum) else f) != "library_strategy"
            for f in search_fields
        )
    ):
        search_fields.append("library_strategy")
    exclude = {"library_strategy": "AMPLICON"} if filter_assembly_runs else None
    return query, search_fields, exclude


def study_assemblies_query(
    study_accession: str,
    allow_non_primary_assembly: bool,
) -> ENABaseQuery | ENAQueryClause:
    """Build the query shared by ``get_study_assemblies`` and its async twin."""
    query: ENABaseQuery | ENAQueryClause = study_accession_query(study_accession)
    if not allow_non_primary_assembly:
        query = query & ENARawQuery('assembly_type="primary metagenome"')
    return query


def filter_by_field(rows: list[Any], field: str, allowed: list[str]) -> list[Any]:
    """Keep only rows whose ``field`` attribute is present in ``allowed``."""
    return [r for r in rows if getattr(r, field, None) in allowed]


def updated_query(cutoff_date: str | date | datetime) -> ENARawQuery:
    """Build the ``last_updated>=cutoff_date`` clause, normalizing ``cutoff_date`` first."""
    return ENARawQuery(f"last_updated>={normalize_cutoff_date(cutoff_date)}")


def updated_tpa_query(
    cutoff_date: str | date | datetime,
) -> ENABaseQuery | ENAQueryClause:
    """Build the ``last_updated>=cutoff_date & assembly_type="primary metagenome"`` clause."""
    return updated_query(cutoff_date) & ENARawQuery(
        'assembly_type="primary metagenome"'
    )
