"""One call each for the client's higher-level convenience helpers.

Run with: uv run python examples/04_convenience_methods.py
"""

import httpx

from ena_api_handler import ENAAvailability, ENAClient

with ENAClient() as client:
    study_by_primary = client.get_study(primary_accession="PRJEB1787")
    study_by_secondary = client.get_study(secondary_accession="ERP001736")
    if study_by_primary is not None:
        print(study_by_primary.study_accession)
    if study_by_secondary is not None:
        print(study_by_secondary.secondary_study_accession)

    # check_study_availability always probes unauthenticated first, then
    # retries with `auth` only if nothing public was found.
    availability = client.check_study_availability(
        primary_accession="PRJEB1787",
        auth=httpx.BasicAuth("user", "pass"),
    )
    if availability == ENAAvailability.PUBLIC:
        print("study is public")
    elif availability == ENAAvailability.PRIVATE:
        print("study is private but accessible")
    else:
        print("study is suppressed / not found")

    sample = client.get_sample("SAMN11835464")
    if sample is not None:
        print(sample.sample_accession)

    linked_studies = client.get_sample_studies("SAMN11835464")
    print(f"sample is linked to {len(linked_studies)} studies")

    run = client.get_run("ERR1701760")
    if run is not None:
        # raw_data_size is computed and attached at runtime; it isn't a
        # declared field on the generated Result model.
        print(run.run_accession, run.raw_data_size)  # type: ignore[union-attr]

    study_runs = client.get_study_runs("PRJEB1787")
    print(f"{len(study_runs)} runs (AMPLICON runs filtered out by default)")

    all_study_runs = client.get_study_runs("PRJEB1787", filter_assembly_runs=False)
    print(f"{len(all_study_runs)} runs including AMPLICON")

    specific_runs = client.get_study_runs(
        "PRJEB1787",
        filter_accessions=["ERR1701760"],
    )
    print(f"{len(specific_runs)} matching run(s) after filter_accessions")

    assemblies = client.get_study_assemblies("ERP124933")
    print(f"{len(assemblies)} primary metagenome assemblies")

    non_primary_assemblies = client.get_study_assemblies(
        "ERP124933",
        allow_non_primary_assembly=True,
    )
    print(f"{len(non_primary_assemblies)} assemblies including non-primary")

    assembly = client.get_assembly("ERZ1669402")
    if assembly is not None:
        print(assembly.analysis_accession)

    assembly_from_sample = client.get_assembly_from_sample("SAMEA1234567")
    if assembly_from_sample is not None:
        print(assembly_from_sample.analysis_accession)

    updated_studies = client.get_updated_studies("2024-01-01")
    updated_runs = client.get_updated_runs("2024-01-01")
    updated_assemblies = client.get_updated_assemblies("2024-01-01")
    updated_tpa_assemblies = client.get_updated_tpa_assemblies("2024-01-01")
    print(
        f"updated since 2024-01-01: {len(updated_studies)} studies, "
        f"{len(updated_runs)} runs, {len(updated_assemblies)} assemblies, "
        f"{len(updated_tpa_assemblies)} TPA assemblies"
    )
