import pytest

from ena_portal_api.constants import ENAPortalResultType
from ena_portal_api.ena_api_requests import ENAAPIRequest


class AssemblyQuery:
    pass


@pytest.mark.integration
def test_assembly_fields_query():
    """Test assembly fields and query with real API request"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.ASSEMBLY,
            query=AssemblyQuery(assembly_type="metagenome-assembled-genome"),  # Common type for metagenomic assemblies
            fields=[
                AssemblyFields.ANALYSIS_ACCESSION,
                AssemblyFields.ASSEMBLY_TYPE,
                AssemblyFields.SCIENTIFIC_NAME,
                AssemblyFields.TAX_ID,
                AssemblyFields.ASSEMBLY_QUALITY,
                AssemblyFields.COMPLETENESS_SCORE,
                AssemblyFields.CONTAMINATION_SCORE,
                AssemblyFields.BINNING_SOFTWARE,
            ],
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0
        assert "analysis_accession" in data[0]
        assert "assembly_type" in data[0]


@pytest.mark.integration
def test_assembly_no_query():
    """Test assembly request without query"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.ASSEMBLY,
            fields=[
                AssemblyFields.ANALYSIS_ACCESSION,
                AssemblyFields.ASSEMBLY_TYPE,
                AssemblyFields.SCIENTIFIC_NAME,
                AssemblyFields.STUDY_ACCESSION,
            ],
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) <= 5
        assert "analysis_accession" in data[0]


@pytest.mark.integration
def test_assembly_comprehensive_fields():
    """Test assembly request with all relevant metagenomic fields"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.ASSEMBLY,
            fields=[
                AssemblyFields.ANALYSIS_ACCESSION,
                AssemblyFields.ANALYSIS_TITLE,
                AssemblyFields.ASSEMBLY_TYPE,
                AssemblyFields.SCIENTIFIC_NAME,
                AssemblyFields.TAX_ID,
                AssemblyFields.SEQUENCING_METHOD,
                AssemblyFields.ASSEMBLY_QUALITY,
                AssemblyFields.ASSEMBLY_SOFTWARE,
                AssemblyFields.TAXONOMIC_CLASSIFICATION,
                AssemblyFields.COMPLETENESS_SCORE,
                AssemblyFields.CONTAMINATION_SCORE,
                AssemblyFields.BINNING_SOFTWARE,
            ],
            query=AssemblyQuery(
                assembly_type="metagenome-assembled-genome",
                analysis_type="ASSEMBLY",
            ),
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0

        # Check if essential fields are present
        first_record = data[0]
        assert "analysis_accession" in first_record
        assert "assembly_type" in first_record
        assert first_record["assembly_type"] == "metagenome-assembled-genome"