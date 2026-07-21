"""Quickest path to a result: search for runs in a study, then fetch a sample.

Run with: uv run python examples/01_quickstart.py
"""

from ena_api_handler import ENAClient
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery

with ENAClient() as client:
    runs = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession="PRJEB1787"),
        fields=[
            ENAReadRunFields.RUN_ACCESSION,
            ENAReadRunFields.FASTQ_FTP,
            ENAReadRunFields.BASE_COUNT,
        ],
        limit=5,
    )

    for run in runs:
        print(run.run_accession, run.fastq_ftp, run.base_count)

    sample = client.get_sample("SAMN11835464")

if sample is not None:
    print(sample.sample_accession, sample.scientific_name)
