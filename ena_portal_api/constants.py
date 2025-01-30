from enum import Enum

ALLOWED_LIBRARY_SOURCE = ["METAGENOMIC", "METATRANSCRIPTOMIC"]
SINGLE_END_LIBRARY_LAYOUT = "SINGLE"
PAIRED_END_LIBRARY_LAYOUT = "PAIRED"
METAGENOME_SCIENTIFIC_NAME = "metagenome"


class ENAPortalResultType(str, Enum):
    ANALYSIS = "analysis"
    ANALYSIS_STUDY = "analysis_study"
    ASSEMBLY = "assembly"
    CODING = "coding"
    NONCODING = "noncoding"
    READ_EXPERIMENT = "read_experiment"
    READ_RUN = "read_run"
    READ_STUDY = "read_study"
    SAMPLE = "sample"
    STUDY = "study"
    TAXON = "taxon"
    TLS_SET = "tls_set"
    TSA_SET = "tsa_set"
    WGS_SET = "wgs_set"
