# tests/test_coding.py
import pytest
from ena_portal_api.ena_api_requests import ENAAPIRequest, ENAPortalResultType
from ena_portal_api.models.coding import CodingFields, CodingQuery

@pytest.mark.integration
def test_coding_fields_query():
    """Test coding sequence fields and query with real API request"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.CODING,
            query=CodingQuery(coding_length="1000-2000"),  # Query for coding sequences between 1-2kb
            fields=[
                CodingFields.ACCESSION,
                CodingFields.DESCRIPTION,
                CodingFields.CODING_LENGTH,
                CodingFields.SCIENTIFIC_NAME,
                CodingFields.TAX_ID,
            ],
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        if len(data) > 0:
            assert "accession" in data[0]
            assert "coding_length" in data[0]
            length = int(data[0]["coding_length"])
            assert 1000 <= length <= 2000

@pytest.mark.integration
def test_coding_no_query():
    """Test coding sequence request without query"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.CODING,
            fields=[
                CodingFields.ACCESSION,
                CodingFields.DESCRIPTION,
                CodingFields.CODING_LENGTH,
                CodingFields.SCIENTIFIC_NAME,
            ],
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) <= 5
        if len(data) > 0:
            assert "accession" in data[0]

@pytest.mark.integration
def test_coding_comprehensive_fields():
    """Test coding sequence request with comprehensive field set"""
    try:
        request = ENAAPIRequest(
            result=ENAPortalResultType.CODING,
            fields=[
                CodingFields.ACCESSION,
                CodingFields.DESCRIPTION,
                CodingFields.CODING_LENGTH,
                CodingFields.SCIENTIFIC_NAME,
                CodingFields.TAX_ID,
                CodingFields.GENE_NAME,
                CodingFields.PROTEIN_NAME,
                CodingFields.EC_NUMBER,
                CodingFields.ORGANISM,
                CodingFields.STRAIN,
                CodingFields.MOLTYPE,
                CodingFields.TOPOLOGY,
            ],
            query=CodingQuery(
                moltype="mRNA",
                organism="Escherichia coli"
            ),
            limit=5
        )
        response = request.get()
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        if len(data) > 0:
            first_record = data[0]
            assert "accession" in first_record
            assert "moltype" in first_record
            assert first_record["moltype"] == "mRNA"
            assert first_record["organism"].lower() == "escherichia coli"

# @pytest.mark.integration
# def test_coding_multiple_conditions():
#     """Test coding sequence request with multiple query conditions"""
#     try:
#         request = ENAAPIRequest(
#             result=ENAPortalResultType.CODING,
#             query=(
#                 CodingQuery(moltype="mRNA") &
#                 CodingQuery(coding_length="500-1000")
#             ),
#             fields=[
#                 CodingFields.ACCESSION,
#                 CodingFields.DESCRIPTION,
#                 CodingFields.CODING_LENGTH,
#                 CodingFields.MOLTYPE,
#             ],
#             limit=5
#         )
#         response = request.get()
#         assert response.status_code == 200
#         data = response.json()
#         assert isinstance(data, list)
#         if len(data) > 0:
#             assert "accession" in data[0]
#             assert "moltype" in data[0]
#             assert data[0]["moltype"] == "mRNA"
#             length = int(data[0]["coding_length"])
#             assert 500 <= length <= 1000