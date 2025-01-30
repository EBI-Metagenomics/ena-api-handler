# # tests/test_analysis_study.py
# import pytest
# from ena_portal_api.ena_api_requests import ENAAPIRequest, ENAPortalResultType
# from ena_portal_api.models.analysis_study import AnalysisStudyFields, AnalysisStudyQuery
#
#
# @pytest.mark.integration
# def test_analysis_study_fields_query():
#     """Test analysis study fields and query with real API request"""
#     request = ENAAPIRequest(
#         result=ENAPortalResultType.ANALYSIS_STUDY,
#         query=AnalysisStudyQuery(study_accession="PRJEB12345"),
#         fields=[
#             AnalysisStudyFields.STUDY_ACCESSION,
#             AnalysisStudyFields.STUDY_TITLE,
#             AnalysisStudyFields.ANALYSIS_TYPE,
#             AnalysisStudyFields.SCIENTIFIC_NAME,
#             AnalysisStudyFields.TAX_ID,
#         ],
#         limit=5,
#     )
#
#     response = request.get()
#     assert response.status_code == 200
#     data = response.json()
#     assert isinstance(data, list)
#     if len(data) > 0:
#         assert "study_accession" in data[0]
#         assert "analysis_type" in data[0]
#
#
# @pytest.mark.integration
# def test_analysis_study_no_query():
#     """Test analysis study request without query"""
#     request = ENAAPIRequest(
#         result=ENAPortalResultType.ANALYSIS_STUDY,
#         fields=[
#             AnalysisStudyFields.STUDY_ACCESSION,
#             AnalysisStudyFields.STUDY_TITLE,
#             AnalysisStudyFields.ANALYSIS_TYPE,
#         ],
#         limit=5,
#     )
#     response = request.get()
#     assert response.status_code == 200
#     data = response.json()
#     assert isinstance(data, list)
#     assert len(data) <= 5
#     if len(data) > 0:
#         assert "study_accession" in data[0]
#
#
# @pytest.mark.integration
# def test_analysis_study_comprehensive_fields():
#     """Test analysis study request with comprehensive field set"""
#     request = ENAAPIRequest(
#         result=ENAPortalResultType.ANALYSIS_STUDY,
#         fields=[
#             AnalysisStudyFields.STUDY_ACCESSION,
#             AnalysisStudyFields.STUDY_TITLE,
#             AnalysisStudyFields.STUDY_ALIAS,
#             AnalysisStudyFields.ANALYSIS_TYPE,
#             AnalysisStudyFields.CENTER_NAME,
#             AnalysisStudyFields.PIPELINE_NAME,
#             AnalysisStudyFields.PIPELINE_VERSION,
#             AnalysisStudyFields.SCIENTIFIC_NAME,
#             AnalysisStudyFields.SAMPLE_ACCESSION,
#             AnalysisStudyFields.TAX_ID,
#         ],
#         query=AnalysisStudyQuery(analysis_type="SEQUENCE_ASSEMBLY"),
#         limit=5,
#     )
#     response = request.get()
#     assert response.status_code == 200
#     data = response.json()
#     assert isinstance(data, list)
#     if len(data) > 0:
#         first_record = data[0]
#         assert "study_accession" in first_record
#         assert "analysis_type" in first_record
#         assert first_record["analysis_type"] == "SEQUENCE_ASSEMBLY"
#
#
# # @pytest.mark.integration
# # def test_analysis_study_multiple_conditions():
# #     """Test analysis study request with multiple query conditions"""
# #     request = ENAAPIRequest(
# #         result=ENAPortalResultType.ANALYSIS_STUDY,
# #         query=(
# #                 AnalysisStudyQuery(analysis_type="SEQUENCE_ASSEMBLY") &
# #                 AnalysisStudyQuery(center_name="EMBL-EBI")
# #         ),
# #         fields=[
# #             AnalysisStudyFields.STUDY_ACCESSION,
# #             AnalysisStudyFields.CENTER_NAME,
# #             AnalysisStudyFields.ANALYSIS_TYPE,
# #             AnalysisStudyFields.STUDY_TITLE,
# #         ],
# #         limit=5
# #     )
# #     response = request.get()
# #     assert response.status_code == 200
# #     data = response.json()
# #     assert isinstance(data, list)
# #     if len(data) > 0:
# #         assert "study_accession" in data[0]
# #         assert data[0]["center_name"] == "EMBL-EBI"
# #         assert data[0]["analysis_type"] == "SEQUENCE_ASSEMBLY"
