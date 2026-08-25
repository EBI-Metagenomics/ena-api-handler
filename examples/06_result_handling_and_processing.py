"""Working with results as Pydantic models, plus the response-processing
controls: field coercion, field aliasing, and row exclusion.

Run with: uv run python examples/06_result_handling_and_processing.py
"""

from ena_api_handler import ENAClient, ENARawQuery
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery

with ENAClient() as client:
    runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=[ENAReadRunFields.RUN_ACCESSION, ENAReadRunFields.SAMPLE_ACCESSION],
        limit=1,
    )
    run = runs[0]

    # Access attributes directly...
    print(run.run_accession, run.sample_accession)

    # ...or convert to a plain dict.
    payload = run.model_dump()
    print(payload["run_accession"])

    # Unrequested fields are typically None.
    print(run.fastq_ftp)

    # By default base_count/read_count are coerced from string to int. This
    # is applied after validation via model_copy(), so model_dump() on a
    # coerced row can warn about the field's declared str type — call
    # model_dump() before requesting coerced fields if you need both.
    #
    # model_copy() also doesn't change the model's declared field type, so
    # type checkers (mypy/pyright) still see `Optional[str]` here even
    # though the runtime value is an int. Use typing.cast() if you need an
    # accurate static type for a coerced field.
    coerced_runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=[ENAReadRunFields.RUN_ACCESSION, ENAReadRunFields.BASE_COUNT],
        limit=1,
    )
    print(coerced_runs[0].run_accession, type(coerced_runs[0].base_count))

    # field_coercions=None disables that and leaves the raw API value.
    raw_runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=[ENAReadRunFields.RUN_ACCESSION, ENAReadRunFields.BASE_COUNT],
        limit=1,
        field_coercions=None,
    )
    print(type(raw_runs[0].base_count))

    # field_aliases renames fields before validation, useful when a result
    # type's raw field names don't match what you want to call them.
    study_rows = client.search(
        result=ENAPortalResultType.STUDY,
        query=ENARawQuery('study_accession="PRJEB1787"'),
        field_aliases={
            "study_description": "description",
            "study_name": "study_alias",
        },
        limit=1,
    )
    print(len(study_rows))

    # exclude drops rows matching field/value pairs.
    non_amplicon = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        exclude={"library_strategy": "AMPLICON"},
        limit=10,
    )
    print(f"{len(non_amplicon)} runs after excluding AMPLICON")
