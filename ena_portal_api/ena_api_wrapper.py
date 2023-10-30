import os
import requests
from pydantic import BaseModel, ValidationError
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
from dotenv import load_dotenv

# Load environment variables from secrets.env
load_dotenv(dotenv_path='secrets.env')

class ENAError(Exception):
    """A custom exception class for ENA API errors."""

class ENARecord(BaseModel):
    accession: str
    description: str
    length: int
    # ... other fields ...

class ENA:
    BASE_URL = 'https://www.ebi.ac.uk/ena/portal/api'

    def __init__(self):
        self.auth = HTTPBasicAuth(os.getenv('ENA_API_USER'), os.getenv('ENA_API_PASSWORD'))

    def _request(self, endpoint, params=None):
        url = f'{self.BASE_URL}/{endpoint}'
        try:
            response = requests.get(url, params=params, auth=self.auth, timeout=10)
            response.raise_for_status()  # This will raise HTTPError for bad responses (4xx and 5xx)
        except RequestException as e:
            # This block will catch any requests-related exceptions, including HTTPError, Timeout, etc.
            raise ENAError(f"Request failed: {e}") from e

        try:
            response_data = response.json()
        except ValueError as e:
            raise ENAError(f"Failed to decode JSON: {e}") from e

        # Check for API-specific errors in the response data (assuming they come in a 'error' field)
        error_message = response_data.get('error')
        if error_message:
            raise ENAError(f"ENA API Error: {error_message}")

        return response_data

    def get_record(self, accession):
        endpoint = 'search'
        params = {
            'query': f'accession="{accession}"',
            'result': 'sequence_release',
            'format': 'json',
        }
        response_data = self._request(endpoint, params)
        if response_data:
            try:
                return ENARecord(**response_data[0])
            except ValidationError as e:
                raise ENAError(f"Failed to validate response data: {e}") from e
        return None

    def search(self, query, result='sequence_release', limit=None, offset=None):
        params = {
            'query': query,
            'result': result,
            'limit': limit,
            'offset': offset,
            'format': 'json'
        }
        return self._request('search', params)

    def get_records(self, accessions):
        records = [self.get_record(accession) for accession in accessions]
        return records

    def get_taxonomy(self, accession):
        params = {
            'query': f'accession="{accession}"',
            'result': 'taxonomy',
            'format': 'json'
        }
        return self._request('search', params)

    def get_sequence(self, accession):
        params = {
            'query': f'accession="{accession}"',
            'result': 'sequence',
            'format': 'text'
        }
        return self._request('search', params)

    def get_metadata(self, accession):
        params = {
            'query': f'accession="{accession}"',
            'result': 'sequence_release',
            'format': 'json'
        }
        return self._request('search', params)

# # Usage:
# ena = ENA()
# try:
#     record = ena.get_record('EU490707')
#     print(record)
# except ENAError as e:
#     print(f"An error occurred: {e}")
