from datetime import date
from enum import Enum
from typing import Optional

from pydantic import Field

from ena_portal_api.query.base import BaseENAQueryConditions


class ReadRunFields(str, Enum):
    BASE_COUNT = "base_count"
    FASTQ_BYTES = "fastq_bytes"
    FASTQ_FTP = "fastq_ftp"
    FASTQ_MD5 = "fastq_md5"
    FIRST_PUBLIC = "first_public"
    INSTRUMENT_MODEL = "instrument_model"
    INSTRUMENT_PLATFORM = "instrument_platform"
    LIBRARY_LAYOUT = "library_layout"
    LIBRARY_NAME = "library_name"
    LIBRARY_SOURCE = "library_source"
    LIBRARY_STRATEGY = "library_strategy"
    READ_COUNT = "read_count"
    RUN_ACCESSION = "run_accession"
    SAMPLE_ALIAS = "sample_alias"
    SAMPLE_DESCRIPTION = "sample_description"
    SAMPLE_TITLE = "sample_title"
    SECONDARY_SAMPLE_ACCESSION = "secondary_sample_accession"
    SECONDARY_STUDY_ACCESSION = "secondary_study_accession"
    STATUS = "status"
    STUDY_ACCESSION = "study_accession"


class ReadRunQuery(BaseENAQueryConditions):
    base_count: Optional[int] = Field(None, description="Number of bases in the run")
    fastq_bytes: Optional[str] = Field(None, description="Size of FASTQ files in bytes")
    fastq_ftp: Optional[str] = Field(None, description="FTP locations of FASTQ files")
    fastq_md5: Optional[str] = Field(None, description="MD5 checksums of FASTQ files")
    first_public: Optional[date] = Field(None, description="Date when run was first made public")
    instrument_model: Optional[str] = Field(None, description="Model of the sequencing instrument")
    instrument_platform: Optional[str] = Field(None, description="Sequencing platform used")
    library_layout: Optional[str] = Field(None, description="Layout of the library (SINGLE or PAIRED)")
    library_name: Optional[str] = Field(None, description="Name of the sequencing library")
    library_source: Optional[str] = Field(None, description="Source of the library (e.g., METAGENOMIC)")
    library_strategy: Optional[str] = Field(None, description="Sequencing strategy used")
    read_count: Optional[int] = Field(None, description="Number of reads in the run")
    run_accession: Optional[str] = Field(None, description="Unique run accession number")
    sample_alias: Optional[str] = Field(None, description="Alias of the sample")
    sample_description: Optional[str] = Field(None, description="Description of the sample")
    sample_title: Optional[str] = Field(None, description="Title of the sample")
    secondary_sample_accession: Optional[str] = Field(None, description="Secondary accession number of the sample")
    secondary_study_accession: Optional[str] = Field(None, description="Secondary accession number of the study")
    status: Optional[str] = Field(None, description="Status of the run")
    study_accession: Optional[str] = Field(None, description="Accession number of the parent study")