from datetime import date
from enum import Enum
from typing import Optional, Literal
from pydantic import Field

from ..query.base import BaseENAQueryConditions


class AnalysisFields(str, Enum):
    #   from https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=metagenome&result=analysis 2025-01-30

    ANALYSIS_ACCESSION = "analysis_accession"  # accession number
    ANALYSIS_ALIAS = "analysis_alias"  # submitter's name for the analysis
    ANALYSIS_TITLE = "analysis_title"  #  brief sequence analysis description
    ANALYSIS_TYPE = "analysis_type"  #   type of sequence analysis
    ASSEMBLY_QUALITY = "assembly_quality"  # Quality of assembly
    ASSEMBLY_SOFTWARE = "assembly_software"  # Assembly software
    ASSEMBLY_TYPE = "assembly_type"  #   analysis Assembly type
    BINNING_SOFTWARE = "binning_software"  # Binning software"""
    BROKER_NAME = "broker_name"  #   broker name
    CENTER_NAME = "center_name"  #   Submitting center
    COMPLETENESS_SCORE = "completeness_score"  # Completeness score (%)
    CONTAMINATION_SCORE = "contamination_score"  # Contamination score (%)
    DESCRIPTION = "description"  #   brief sequence description
    FIRST_PUBLIC = "first_public"  # date when made public
    GENERATED_ASPERA = "generated_aspera"  # Aspera links for generated files
    GENERATED_BYTES = "generated_bytes"  #  size (in bytes) of generated files
    GENERATED_FTP = "generated_ftp"  #   FTP links for generated files
    GENERATED_GALAXY = "generated_galaxy"  #    Galaxy links for generated files
    GENERATED_MD5 = "generated_md5"  #  MD5 checksum of generated files
    LAST_UPDATED = "last_updated"  # date when last updated
    PIPELINE_NAME = "pipeline_name"  #   analysis pipeline name
    PIPELINE_VERSION = "pipeline_version"  # analysis pipeline version
    SAMPLE_ACCESSION = "sample_accession"  # sample accession number
    SAMPLE_ALIAS = "sample_alias"  # submitter's name for the study
    SAMPLE_DESCRIPTION = "sample_description"  # detailed sample description
    SAMPLE_TITLE = "sample_title"  # brief sample title
    SCIENTIFIC_NAME = "scientific_name"  #   scientific name of an organism
    SECONDARY_SAMPLE_ACCESSION = (
        "secondary_sample_accession"  # secondary sample accession number
    )
    SECONDARY_STUDY_ACCESSION = (
        "secondary_study_accession"  #   secondary study accession number
    )
    SEQUENCING_METHOD = "sequencing_method"  # Sequencing method used
    STUDY_ACCESSION = "study_accession"  #   study accession number
    STUDY_ALIAS = "study_alias"  #   submitter's name for the study
    STUDY_TITLE = "study_title"  #   brief sequencing study description
    SUBMITTED_ASPERA = "submitted_aspera"  # Aspera links for submitted files
    SUBMITTED_BYTES = "submitted_bytes"  #   size (in bytes) of submitted files
    SUBMITTED_FTP = "submitted_ftp"  #   FTP links for submitted files
    SUBMITTED_GALAXY = "submitted_galaxy"  #  Galaxy links for submitted files
    SUBMITTED_MD5 = "submitted_md5"  #   MD5 checksum of submitted files
    TAX_ID = "tax_id"  # NCBI taxonomic classification
    TAXONOMIC_CLASSIFICATION = "taxonomic_classification"  # Taxonomic classification


class AnalysisQuery(BaseENAQueryConditions):
    analysis_accession: Optional[str] = Field(None, description="accession number")
    analysis_alias: Optional[str] = Field(
        None, description="submitter's name for the analysis"
    )
    analysis_title: Optional[str] = Field(
        None, description="brief sequence analysis description"
    )
    analysis_type: Optional[
        Literal["SEQUENCE_ASSEMBLY", "REFERENCE_ALIGNMENT"]
    ] = Field(None, description="Type of sequence analysis")
    assembly_quality: Optional[str] = Field(None, description="Quality of assembly")
    assembly_software: Optional[str] = Field(None, description="Assembly software")
    assembly_type: Optional[str] = Field(None, description="analysis Assembly type")
    #   broker name is controlled field but too many options
    binning_software: Optional[str] = Field(None, description="Binning software")
    broker_name: Optional[str] = Field(None, description="broker name")
    center_name: Optional[str] = Field(None, description="Submitting center")
    completeness_score: Optional[float] = Field(
        None, description="Completeness score (%)"
    )
    contamination_score: Optional[float] = Field(
        None, description="Contamination score (%)"
    )
    description: Optional[str] = Field(None, description="brief sequence description")
    first_public: Optional[date] = Field(None, description="date when made public")
    generated_aspera: Optional[str] = Field(
        None, description="Aspera links for generated file"
    )
    generated_bytes: Optional[str] = Field(
        None, description="size (in bytes) of generated files"
    )
    generated_ftp: Optional[str] = Field(
        None, description="FTP links for generated files"
    )
    generated_galaxy: Optional[str] = Field(
        None, description="Galaxy links for generated files"
    )
    generated_md5: Optional[str] = Field(
        None, description="MD5 checksum of generated files"
    )
    last_updated: Optional[date] = Field(None, description="date when last updated")
    pipeline_name: Optional[str] = Field(None, description="analysis pipeline name")
    pipeline_version: Optional[str] = Field(
        None, description="analysis pipeline version"
    )
    sample_accession: Optional[str] = Field(None, description="sample accession number")
    sample_alias: Optional[str] = Field(
        None, description="submitter's name for the sample"
    )
    sample_description: Optional[str] = Field(
        None, description="detailed sample description"
    )
    sample_title: Optional[str] = Field(None, description="detailed sample title")
    scientific_name: Optional[str] = Field(
        None, description="scientific name of an organism"
    )
    secondary_sample_accession: Optional[str] = Field(
        None, description="secondary sample accession number"
    )
    secondary_study_accession: Optional[str] = Field(
        None, description="secondary study accession number"
    )
    sequencing_method: Optional[str] = Field(None, description="sequencing method used")
    study_accession: Optional[str] = Field(None, description="study accession number")
    study_alias: Optional[str] = Field(None, description="study alias")
    study_title: Optional[str] = Field(None, description="study title")
    submitted_aspera: Optional[str] = Field(
        None, description="Aspera links for submitted file"
    )
    submitted_bytes: Optional[str] = Field(
        None, description="size (in bytes) of submitted files"
    )
    submitted_ftp: Optional[str] = Field(
        None, description="FTP links for submitted files"
    )
    submitted_galaxy: Optional[str] = Field(
        None, description="Galaxy links for submitted files"
    )
    submitted_md5: Optional[str] = Field(
        None, description="MD5 checksum of submitted files"
    )
    tax_id: Optional[int] = Field(None, description="NCBI taxonomic classification")
    taxonomic_classification: Optional[str] = Field(
        None, description="Taxonomic classification"
    )
