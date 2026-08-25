"""Core `search()` usage: field selection, unbounded results, cross-portal
fallback, and the ENAQueryValidationError path for mismatched query/fields.

Run with: uv run python examples/02_searching.py
"""

from ena_api_handler import ENAClient, ENAPortalDataPortal, ENAQueryValidationError
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery

with ENAClient() as client:
    # Basic typed search with explicit fields (typed Fields enum members).
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
    for run in runs:
        print(run.run_accession, run.sample_accession, run.library_strategy)

    # Fields may also be passed as plain strings.
    runs_by_string_fields = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=["run_accession", "fastq_ftp"],
        limit=5,
    )
    print(f"{len(runs_by_string_fields)} runs via string fields")

    # limit=0 asks ENA for every matching row.
    all_runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        limit=0,
    )
    print(f"{len(all_runs)} total runs in PRJEB1787")

    # Try multiple data portals in order, returning the first non-empty set.
    cross_portal_runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        portals=(
            ENAPortalDataPortal.METAGENOME,
            ENAPortalDataPortal.ENA,
        ),
    )
    print(f"{len(cross_portal_runs)} runs via cross-portal search")

    # Typed query/fields are validated against the requested result type. A
    # READ_RUN query passed against a STUDY search matches no portal, so
    # search() raises ENAQueryValidationError instead of sending a broken
    # request.
    try:
        client.search(
            result=ENAPortalResultType.STUDY,
            query=ENAReadRunQuery(study_accession="PRJEB1787"),
        )
    except ENAQueryValidationError as exc:
        print(f"validation error as expected: {exc}")
