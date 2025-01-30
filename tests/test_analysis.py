from datetime import date, timedelta
from ena_portal_api.ena_api_requests import ENAAPIRequest, ENAPortalResultType
from ena_portal_api.models.analysis import AnalysisFields, AnalysisQuery


@pytest.mark.integration
def test_analysis_fields_query():
    """Test analysis fields and query with real API request"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.ANALYSIS,
            query=AnalysisQuery(analysis_type="SEQUENCE_ASSEMBLY"),  # Common analysis type
            fields=[
                AnalysisFields.ANALYSIS_ACCESSION,
                AnalysisFields.ANALYSIS_TYPE,
                AnalysisFields.ANALYSIS_TITLE,
                AnalysisFields.STUDY_ACCESSION,
                AnalysisFields.PIPELINE_NAME,
                AnalysisFields.PIPELINE_VERSION,
            ],
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0
        assert "analysis_accession" in data[0]
        assert "analysis_type" in data[0]
        assert data[0]["analysis_type"] == "SEQUENCE_ASSEMBLY"


@pytest.mark.integration
def test_analysis_no_query():
    """Test analysis request without query"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.ANALYSIS,
            fields=[
                AnalysisFields.ANALYSIS_ACCESSION,
                AnalysisFields.ANALYSIS_TYPE,
                AnalysisFields.STUDY_ACCESSION,
                AnalysisFields.SCIENTIFIC_NAME,
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
def test_analysis_comprehensive_fields():
    """Test analysis request with comprehensive field set"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.ANALYSIS,
            fields=[
                AnalysisFields.ANALYSIS_ACCESSION,
                AnalysisFields.ANALYSIS_ALIAS,
                AnalysisFields.ANALYSIS_TITLE,
                AnalysisFields.ANALYSIS_TYPE,
                AnalysisFields.STUDY_ACCESSION,
                AnalysisFields.STUDY_TITLE,
                AnalysisFields.SAMPLE_ACCESSION,
                AnalysisFields.SCIENTIFIC_NAME,
                AnalysisFields.TAX_ID,
                AnalysisFields.PIPELINE_NAME,
                AnalysisFields.PIPELINE_VERSION,
                AnalysisFields.SUBMITTED_FTP,
                AnalysisFields.SUBMITTED_MD5,
            ],
            query=AnalysisQuery(
                analysis_type="SEQUENCE_ASSEMBLY",
                study_accession="PRJEB12345"  # Example study accession
            ),
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

        if len(data) > 0:  # Only check if we got results
            first_record = data[0]
            assert "analysis_accession" in first_record
            assert "analysis_type" in first_record
            assert first_record["study_accession"] == "PRJEB12345"

@pytest.mark.integration
def test_analysis_multiple_conditions():
    """Test analysis request with multiple query conditions"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.ANALYSIS,
            query=(
                AnalysisQuery(analysis_type="SEQUENCE_ASSEMBLY") &
                AnalysisQuery(center_name="EMBL-EBI")
            ),
            fields=[
                AnalysisFields.ANALYSIS_ACCESSION,
                AnalysisFields.CENTER_NAME,
                AnalysisFields.ANALYSIS_TYPE,
            ],
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

        if len(data) > 0:  # Only check if we got results
            assert "analysis_accession" in data[0]
            assert data[0]["center_name"] == "EMBL-EBI"
            assert data[0]["analysis_type"] == "SEQUENCE_ASSEMBLY"