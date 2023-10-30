import os

ENA_API_URL = os.environ.get(
    "ENA_API_URL", "https://www.ebi.ac.uk/ena/portal/api/v2.0/search" # TODO why v2.0
)

RETRY_COUNT = 5

STUDY_DEFAULT_FIELDS = ",".join(
    [
        "study_accession",
        "secondary_study_accession",
        "description",
        "study_alias",
        "study_title",
        "tax_id",
        "scientific_name",
        "center_name",
        "first_public",
    ]
)

SAMPLE_DEFAULT_FIELDS = ",".join(
    [
        "sample_accession",
        "secondary_sample_accession",
        "sample_alias",
        "description",
        "tax_id",
        "scientific_name",
        "host_tax_id",
        "host_status",
        "host_sex",
        "submitted_host_sex",
        "host_body_site",
        "host_gravidity",
        "host_genotype",
        "host_phenotype",
        "host_growth_conditions",
        "collection_date",
        "collected_by",
        "country",
        "location",
        "depth",
        "altitude",
        "elevation",
        "first_public",
        "checklist",
        "center_name",
        "broker_name",
        "ncbi_reporting_standard",
        "investigation_type",
        "experimental_factor",
        "environment_biome",
        "environment_feature",
        "environment_material",
        "temperature",
        "salinity",
        "ph",
        "sample_collection",
        "project_name",
        "target_gene",
        "sequencing_method",
        "sample_title",
        "status",
        "host",
    ]
)

RUN_DEFAULT_FIELDS = [
    "study_accession",
    "secondary_study_accession",
    "run_accession",
    "library_source",
    "library_strategy",
    "library_layout",
    "fastq_ftp",
    "fastq_md5",
    "fastq_bytes",
    "base_count",
    "read_count",
    "instrument_platform",
    "instrument_model",
    "secondary_sample_accession",
    "library_name",
    "sample_alias",
    "sample_title",
    "sample_description",
    "first_public",
    "status",
]
RUN_DEFAULT_FIELDS_STR = ",".join(RUN_DEFAULT_FIELDS)

# To get the possible fields:
# https://www.ebi.ac.uk/ena/portal/api/v2.0/returnFields?result=analysis&dataPortal=metagenome
# https://www.ebi.ac.uk/ena/portal/api/v2.0/returnFields?result=analysis&dataPortal=ena
ASSEMBLY_DEFAULT_FIELDS = [
    "analysis_accession",
    "analysis_alias",
    "analysis_title",
    "analysis_type",
    "assembly_type",
    "broker_name",
    "center_name",
    "description",
    "first_public",
    "last_updated",
    "pipeline_name",
    "pipeline_version",
    "sample_accession",
    "sample_alias",
    "sample_description",
    "sample_title",
    "scientific_name",
    "secondary_sample_accession",
    "secondary_study_accession",
    "study_accession",
    "study_alias",
    "study_title",
    "submitted_aspera",
    "submitted_bytes",
    "submitted_ftp",
    "submitted_galaxy",
    "submitted_md5",
    "tax_id",
    "sequencing_method",  # Sequencing method used
    "assembly_quality",  # Quality of assembly
    "assembly_software",  # Assembly software
    "taxonomic_classification",  # Taxonomic classification
    "completeness_score",  # Completeness score (%)
    "contamination_score",  # Contamination score (%)
    "binning_software",  # Binning software
]
ASSEMBLY_DEFAULT_FIELDS_STR = ",".join(ASSEMBLY_DEFAULT_FIELDS)
