"""Composing queries with `&` (AND), `|` (OR), `~` (NOT), and ENARawQuery for
syntax the typed models can't express (e.g. date ranges).

Run with: uv run python examples/03_composing_queries.py
"""

from ena_api_handler import ENAClient, ENARawQuery
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery

with ENAClient() as client:
    and_query = ENAReadRunQuery(study_accession="PRJEB1787") & ENAReadRunQuery(
        library_strategy="WGS"
    )
    wgs_runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=and_query,
        fields=[ENAReadRunFields.RUN_ACCESSION],
        limit=5,
    )
    print(f"{len(wgs_runs)} WGS runs")

    or_query = ENAReadRunQuery(study_accession="PRJEB1787") | ENAReadRunQuery(
        secondary_study_accession="ERP001736"
    )
    either_study_runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=or_query,
        fields=[ENAReadRunFields.RUN_ACCESSION],
        limit=5,
    )
    print(f"{len(either_study_runs)} runs across either study")

    not_query = ~ENAReadRunQuery(library_strategy="AMPLICON")
    non_amplicon_runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787") & not_query,
        fields=[ENAReadRunFields.RUN_ACCESSION],
        limit=5,
    )
    print(f"{len(non_amplicon_runs)} non-AMPLICON runs")

    # ENARawQuery covers syntax the generated models don't, e.g. date ranges,
    # and composes with typed queries the same way.
    raw_query = ENARawQuery("last_updated>=2024-01-01") & ENAReadRunQuery(
        study_accession="PRJEB1787"
    )
    recently_updated_runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=raw_query,
        fields=["run_accession"],
        limit=5,
    )
    print(f"{len(recently_updated_runs)} recently updated runs")
