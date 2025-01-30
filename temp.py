# from ena_portal_api import EnaApiHandler
from ena_portal_api.ena_api_requests import ENAAPIRequest, ENAPortalResultType, ENAStudyQuery, ENAStudyFields
from ena_portal_api.ena_handler import EnaApiHandler

# Create an instance of the API handler
# You can optionally provide username and password if you have API credentials
ena_handler = EnaApiHandler()  # or EnaApiHandler(username="your_username", password="your_password")

try:
    request = ENAAPIRequest(
        result=ENAPortalResultType.STUDY,
        query=(
                  ENAStudyQuery(study_accession="PRJDA33427")
                  | ENAStudyQuery(secondary_study_accession="ERP1")
              ),
              # & ENAStudyQuery(tax_id="408170"),
        fields=[
            ENAStudyFields.STUDY_NAME,
            ENAStudyFields.STUDY_ACCESSION,
            ENAStudyFields.TAX_ID,
            ENAStudyFields.SECONDARY_STUDY_ACCESSION,
        ],
        # limit=10,
    )
    response = request.get()
    print(response.text)
except ValueError as e:
    print(f"Error fetching study: {e}")

# 1. Fetch a study using its primary accession
# try:
#     study = ena_handler.get_study(primary_accession="PRJEB12345")
#     print(f"Study title: {study['study_title']}")
#     print(f"Description: {study['description']}")
# except ValueError as e:
#     print(f"Error fetching study: {e}")

# # 2. Fetch a study using its secondary accession
# try:
#     study = ena_handler.get_study(secondary_accession="ERP012345")
#     print(f"Study title: {study['study_title']}")
# except ValueError as e:
#     print(f"Error fetching study: {e}")
#
# # 3. Fetch studies updated after a specific date
# try:
#     # Date format should be YYYY-MM-DD
#     updated_studies = ena_handler.get_updated_studies(cutoff_date="2024-01-01")
#     for study in updated_studies:
#         print(f"Updated study: {study['study_accession']} - {study['study_title']}")
# except ValueError as e:
#     print(f"Error fetching updated studies: {e}")
#
# # 4. Fetch a study with specific fields
# custom_fields = ",".join([
#     "study_accession",
#     "study_title",
#     "first_public",
#     "center_name"
# ])
#
# try:
#     study = ena_handler.get_study(
#         primary_accession="PRJEB12345",
#         fields=custom_fields
#     )
#     print(f"Study title: {study['study_title']}")
#     print(f"Center: {study['center_name']}")
# except ValueError as e:
#     print(f"Error fetching study: {e}")
