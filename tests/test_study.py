# tests/models/test_study.py
from ena_portal_api.ena_api_requests import ENAAPIRequest, ENAPortalResultType
from ena_portal_api.models.study import StudyFields, StudyQuery


def test_study_fields_query():
    """Test study fields and query with real API request"""
    request = ENAAPIRequest(
        result=ENAPortalResultType.STUDY,
        query=(
            StudyQuery(study_accession="PRJDA33427")
            | StudyQuery(secondary_study_accession="ERP1")
        ),
        fields=[
            StudyFields.STUDY_NAME,
            StudyFields.STUDY_ACCESSION,
            StudyFields.TAX_ID,
            StudyFields.SECONDARY_STUDY_ACCESSION,
        ],
    )
    response = request.get()
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert "study_accession" in data[0]


def test_study_no_query():
    """Test study request without query, using limit"""
    request = ENAAPIRequest(
        result=ENAPortalResultType.STUDY,
        fields=[
            StudyFields.STUDY_NAME,
            StudyFields.STUDY_ACCESSION,
        ],
        limit=5,  # Small limit for testing
    )
    response = request.get()
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 5
    assert "study_accession" in data[0]
