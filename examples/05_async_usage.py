"""Async mirrors the sync API closely: `async with`, `search_async()`, and
`_async` convenience twins.

Run with: uv run python examples/05_async_usage.py
"""

import asyncio

from ena_api_handler import ENAClient
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunFields, ENAReadRunQuery


async def main() -> None:
    async with ENAClient() as client:
        runs = await client.search_async(
            result=ENAPortalResultType.READ_RUN,
            query=ENAReadRunQuery(study_accession="PRJEB1787"),
            fields=[ENAReadRunFields.RUN_ACCESSION],
            limit=5,
        )
        for run in runs:
            print(run.run_accession)

        sample = await client.get_sample_async("SAMN11835464")
        if sample is not None:
            print(sample.sample_accession)

        study_runs = await client.get_study_runs_async("PRJEB1787")
        print(f"{len(study_runs)} runs")


if __name__ == "__main__":
    asyncio.run(main())
