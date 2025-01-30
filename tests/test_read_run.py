# tests/models/test_read_run.py
from ena_portal_api.constants import ENAPortalResultType
from ena_portal_api.ena_api_requests import ENAAPIRequest
from ena_portal_api.models.read_run import ReadRunFields, ReadRunQuery


def test_read_run_fields_query():
    """Test read run fields and query with real API request"""
    request = ENAAPIRequest(
        result=ENAPortalResultType.READ_RUN,
        query=ReadRunQuery(
            library_source="METAGENOMIC"
        ),  # More likely to exist than specific accession
        fields=[
            ReadRunFields.RUN_ACCESSION,
            ReadRunFields.STUDY_ACCESSION,
            ReadRunFields.LIBRARY_LAYOUT,
            ReadRunFields.LIBRARY_SOURCE,
            ReadRunFields.FASTQ_FTP,
            ReadRunFields.READ_COUNT,
        ],
        limit=5,  # Small limit for testing
    )
    response = request.get()
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert "run_accession" in data[0]
    assert data[0]["library_source"] == "METAGENOMIC"


def test_read_run_no_query():
    """Test read run request without query"""
    request = ENAAPIRequest(
        result=ENAPortalResultType.READ_RUN,
        fields=[
            ReadRunFields.RUN_ACCESSION,
            ReadRunFields.STUDY_ACCESSION,
            ReadRunFields.LIBRARY_SOURCE,
        ],
        limit=5,  # Small limit for testing
    )
    response = request.get()
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 5
    assert "run_accession" in data[0]
