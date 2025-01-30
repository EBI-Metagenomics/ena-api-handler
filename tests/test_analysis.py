from ena_portal_api.ena_api_requests import ENAAPIRequest, ENAPortalResultType
from ena_portal_api.models.analysis import AnalysisFields, AnalysisQuery
import pytest


@pytest.mark.integration
def test_analysis_fields_query():
    """Test analysis fields and query with real API request"""
    request = ENAAPIRequest(
        result=ENAPortalResultType.ANALYSIS,
        query=(AnalysisQuery(study_accession="PRJEB51815")),
        fields=[
            AnalysisFields.ANALYSIS_ACCESSION,
            AnalysisFields.ANALYSIS_ALIAS,
            AnalysisFields.ANALYSIS_TYPE,
            AnalysisFields.ASSEMBLY_TYPE,
            AnalysisFields.ANALYSIS_TITLE,
            AnalysisFields.GENERATED_FTP,
            AnalysisFields.SAMPLE_ACCESSION,
            AnalysisFields.COMPLETENESS_SCORE,
            AnalysisFields.CONTAMINATION_SCORE,
        ],
        limit=20,
    )
    response = request.get()
    assert response.status_code == 200
    data = response.json()

    assert isinstance(data, list)
    assert len(data) > 0
    assert "analysis_accession" in data[0]
    assert "analysis_type" in data[0]
    assert data[0]["generated_ftp"] != ""  # ftps are not empty

    assert data[0]["analysis_type"] == "SEQUENCE_ASSEMBLY"
    assert "primary metagenome" in [x["assembly_type"] for x in data]
    assert "binned metagenome" in [x["assembly_type"] for x in data]

    binned_metagenomes = [x for x in data if x["assembly_type"] == "binned metagenome"]
    assert "completeness_score" in binned_metagenomes[0]
    assert "contamination_score" in binned_metagenomes[0]


@pytest.mark.integration
def test_analysis_no_query():
    """Test analysis request without query"""
    request = ENAAPIRequest(
        result=ENAPortalResultType.ANALYSIS,
        fields=[
            AnalysisFields.ANALYSIS_ACCESSION,
            AnalysisFields.ANALYSIS_TYPE,
            AnalysisFields.STUDY_ACCESSION,
            AnalysisFields.SCIENTIFIC_NAME,
        ],
        limit=5,
    )
    response = request.get()
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 5
    assert "analysis_accession" in data[0]


@pytest.mark.integration
def test_analysis_comprehensive_fields():
    """Test analysis request with comprehensive field set"""
    request = ENAAPIRequest(
        result=ENAPortalResultType.ANALYSIS,
        fields=list(AnalysisFields),
        query=AnalysisQuery(
            analysis_type="SEQUENCE_ASSEMBLY",
            study_accession="PRJEB51815",  # Example study accession
        ),
        limit=5,
    )
    response = request.get()
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

    if len(data) > 0:  # Only check if we got results
        first_record = data[0]
        assert len(first_record) == 134
        assert "analysis_accession" in first_record
        assert "analysis_type" in first_record
        assert first_record["study_accession"] == "PRJEB51815"


# @pytest.mark.integration
# def test_analysis_multiple_conditions():
#     """Test analysis request with multiple query conditions"""
#     try:
#         request = ENAAPIRequest(
#             result=ENAPortalResultType.ANALYSIS,
#             query=(
#                 AnalysisQuery(analysis_type="SEQUENCE_ASSEMBLY") &
#                 AnalysisQuery(center_name="EMBL-EBI")
#             ),
#             fields=[
#                 AnalysisFields.ANALYSIS_ACCESSION,
#                 AnalysisFields.CENTER_NAME,
#                 AnalysisFields.ANALYSIS_TYPE,
#             ],
#             limit=5
#         )
#         response = request.get()
#         assert response.status_code == 200
#         data = response.json()
#         assert isinstance(data, list)
#
#         if len(data) > 0:  # Only check if we got results
#             assert "analysis_accession" in data[0]
#             assert data[0]["center_name"] == "EMBL-EBI"
#             assert data[0]["analysis_type"] == "SEQUENCE_ASSEMBLY"
