#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2018-2023 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import os
from multiprocessing.pool import ThreadPool
from time import sleep

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from ena_portal_api.constants import *
from ena_portal_api.utils import *

logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)

class EnaApiHandler:
    def __init__(self, username=None, password=None):
        self.url = ENA_API_URL
        username = username or os.getenv("ENA_API_USER")
        password = password or os.getenv("ENA_API_PASSWORD")
        if username and password:
            self.auth = (username, password)
        else:
            self.auth = None

    def post_request(self, data):
        if self.auth:
            response = requests.post(
                self.url, data=data, auth=self.auth, **get_default_connection_headers()
            )
        else:
            logging.warning(
                "Not authenticated, set env vars ENA_API_USER and ENA_API_PASSWORD to access private data."  # noqa: E501
            )
            response = requests.post(
                self.url, data=data, **get_default_connection_headers()
            )
        return response

    def _parse_response_error(self, response):
        """ENA Portal API error response parser.
        This wrapper will try to get the "message" content from the response data.
        If that fails it will use the response.txt.
        If the above fails, it will return "no_content_on_response".
        """
        message = response.text or "no_content_on_response"
        try:
            data = response.json()
            if data:
                return data.get("message", message)
            else:
                return message
        except json.decoder.JSONDecodeError:
            return message

    # Supports ENA primary and secondary study accessions
    def get_study(
        self, primary_accession=None, secondary_accession=None, fields=None, attempt=0
    ):
        """FIXME: no doc string"""
        data = get_default_params()
        data["result"] = "read_study"
        data["fields"] = fields or STUDY_DEFAULT_FIELDS

        if primary_accession and not secondary_accession:
            data["query"] = 'study_accession="{}"'.format(primary_accession)
        elif not primary_accession and secondary_accession:
            data["query"] = 'secondary_study_accession="{}"'.format(secondary_accession)
        else:
            data[
                "query"
            ] = 'study_accession="{}" AND secondary_study_accession="{}"'.format(
                primary_accession, secondary_accession
            )

        query_params = []
        for result_type in ["study", "read_study", "analysis_study"]:
            for data_portal in ["ena", "metagenome"]:
                param = data.copy()
                param["result"] = result_type
                param["dataPortal"] = data_portal
                if result_type == "study":
                    if "description" in param["fields"]:
                        param["fields"] = param["fields"].replace(
                            "description", "study_description"
                        )
                    if "study_alias" in param["fields"]:
                        param["fields"] = param["fields"].replace(
                            "study_alias", "study_name"
                        )
                query_params.append(param)

        for param in query_params:
            try:
                return self._get_study(param)
            except NoDataException:
                logging.info(
                    "No info found to fetch study with params {}".format(param)
                )

                pass
            except (IndexError, TypeError, ValueError, KeyError):
                logging.info("Failed to fetch study with params {}".format(param))

        raise ValueError(
            "Could not find study {} {} in ENA.".format(
                primary_accession, secondary_accession
            )
        )

    def _get_study(self, data):
        """FIXME: no doc string"""
        response = self.post_request(data)
        if response.status_code == 204:
            raise NoDataException()
        try:
            # FIXME: use response.json()[0]
            study = json.loads(response.text)[0]
        except (IndexError, TypeError, ValueError, KeyError) as e:
            raise e
        if data["result"] == "study":
            study = self.remap_study_fields(study)
        return study

    @staticmethod
    def remap_study_fields(data):
        """FIXME: no doc string"""
        if "study_description" in data:
            data["description"] = data.pop("study_description")
        if "study_name" in data:
            data["study_alias"] = data.pop("study_name")
        return data

    def get_sample(self, sample_accession, fields=None, search_params=None, attempt=0):
        """FIXME: no doc string"""
        data = get_default_params()
        data["result"] = "sample"
        data["fields"] = fields or SAMPLE_DEFAULT_FIELDS
        data[
            "query"
        ] = '(sample_accession="{acc}" OR secondary_sample_accession="{acc}") '.format(
            acc=sample_accession
        )

        if search_params:
            data.update(search_params)

        response = self.post_request(data)
        if response.status_code == 200:
            data = response.json()
            if len(data):
                return data[0]
            else:
                raise ValueError(f"Could not find sample {sample_accession} in ENA.")

        if not response.ok:
            logging.error(
                "Error retrieving sample {}, response code: {}".format(
                    sample_accession, response.status_code
                )
            )
            api_error = self._parse_response_error(response)
            raise ValueError(
                f"Could not retrieve sample with accession {sample_accession}. Error: {api_error}"
            )
        elif response.status_code == 204:
            if attempt < 2:
                new_params = {
                    "dataPortal": "metagenome" if data["dataPortal"] == "ena" else "ena"
                }
                attempt += 1
                return self.get_sample(
                    sample_accession,
                    fields=fields,
                    search_params=new_params,
                    attempt=attempt,
                )
            else:
                api_error_message = self._parse_response_error(response)
                error = (
                    f"Could not find sample {sample_accession} in ENA after {RETRY_COUNT} attempts. "
                    f"ENA response: {api_error_message}"
                )
                raise ValueError(error)

    def get_sample_studies(self, primary_sample_accession, result="read_run"):
        """FIXME: no doc string"""
        data = get_default_params()
        data["result"] = result
        data["fields"] = "secondary_study_accession"
        data["query"] = 'sample_accession="{acc}"'.format(acc=primary_sample_accession)
        response = self.post_request(data)
        if not response.ok:
            logging.error(
                "Error retrieving sample {}, response code: {}".format(
                    primary_sample_accession, response.status_code
                )
            )
            api_error = self._parse_response_error(response)
            raise ValueError(
                (
                    f"Could not retrieve sample with accession {primary_sample_accession}. "
                    f"Error: {api_error}",
                )
            )
        elif response.status_code == 204:
            raise ValueError(
                "Could not find study for sample {} in ENA after {} attempts".format(
                    primary_sample_accession, RETRY_COUNT
                )
            )
        else:
            # FIXME: refactor this, use response.json()
            return {s["secondary_study_accession"] for s in json.loads(response.text)}

    def get_run(
        self, run_accession, fields=None, public=True, attempt=0, search_params=None
    ):
        """FIXME: no doc string"""
        data = get_default_params()
        data["result"] = "read_run"
        data["fields"] = fields or RUN_DEFAULT_FIELDS_STR
        data["query"] = 'run_accession="{}"'.format(run_accession)

        if search_params:
            data.update(search_params)

        response = self.post_request(data)
        if not response.ok:
            api_error = self._parse_response_error(response)
            raise ValueError(
                (
                    f"Could not retrieve run with accession {run_accession}. "
                    f"Error: {api_error}"
                )
            )

        if response.status_code == 204:
            if attempt < 2:
                attempt += 1
                sleep(1)
                logging.warning(
                    "Error 204 when retrieving run {} in dataPortal {}, "
                    "retrying {}".format(run_accession, data.get("dataPortal"), attempt)
                )
                return self.get_run(
                    run_accession=run_accession,
                    fields=fields,
                    public=public,
                    attempt=attempt,
                    search_params=search_params,
                )
            elif attempt == 2 and data["dataPortal"] != "ena":
                return self.get_run(
                    run_accession=run_accession,
                    fields=fields,
                    public=public,
                    search_params={"dataPortal": "ena"},
                )
            else:
                api_error = self._parse_response_error(response)
                raise ValueError(
                    (
                        f"Could not find run {run_accession} in ENA after {RETRY_COUNT} attempts. "
                        f"ENA response: {api_error}"
                    )
                )
        try:
            data = response.json()
            if not len(data):
                raise ValueError(f"Could not find run {run_accession} in ENA.")
            run = data[0]
        except (IndexError, json.decoder.JSONDecodeError) as exception:
            logging.exception(exception)
            raise ValueError(
                f"Could not find run {run_accession} in ENA. Error {exception}"
            )

        # FIXME: Explain this #
        if fields is None or "raw_data_size" in fields:
            run["raw_data_size"] = self.get_run_raw_size(run)

        for int_param in ("read_count", "base_count"):
            if int_param in run:
                try:
                    run[int_param] = int(run[int_param])
                except ValueError as e:
                    if not public:
                        raise e
                    run[int_param] = -1
        return run

    def get_study_runs(
        self,
        study_acc,
        fields=None,
        filter_assembly_runs=True,
        filter_accessions=None,
        search_params=None,
    ):
        """FIXME: no doc string"""
        data = get_default_params()
        data["result"] = "read_run"
        data["fields"] = fields or RUN_DEFAULT_FIELDS_STR
        data[
            "query"
        ] = '(study_accession="{}" OR secondary_study_accession="{}")'.format(
            study_acc, study_acc
        )

        if search_params:
            data.update(search_params)

        if filter_assembly_runs and "library_strategy" not in data["fields"]:
            data["fields"] += ",library_strategy"

        response = self.post_request(data)

        if not response.ok:
            logging.debug(
                "Error retrieving study runs {}, response code: {}".format(
                    study_acc, response.status_code
                )
            )
            api_error = self._parse_response_error(response)
            raise ValueError(
                (
                    f"Could not retrieve runs for study {study_acc}. "
                    f"ENA response: {api_error}"
                )
            )
        elif response.status_code == 204:
            return []

        runs = response.json()

        if filter_assembly_runs:
            runs = list(filter(run_filter, runs))
        if filter_accessions:
            runs = list(filter(lambda r: r["run_accession"] in filter_accessions, runs))

        for run in runs:
            run["raw_data_size"] = self.get_run_raw_size(run)
            for int_param in ("read_count", "base_count"):
                if int_param in run:
                    try:
                        run[int_param] = int(run[int_param])
                    except ValueError:
                        run[int_param] = None
        return runs

    def get_study_assemblies(
        self,
        study_accession,
        fields=None,
        filter_accessions=None,
        allow_non_primary_assembly=False,
        data_portal="metagenome",
        retry=True,
    ):
        """FIXME: no doc string"""
        data = get_default_params()
        data["dataPortal"] = data_portal
        data["result"] = "analysis"
        data["fields"] = fields or ASSEMBLY_DEFAULT_FIELDS_STR

        query = "("
        query += 'study_accession="{study_accession}"'.format(
            study_accession=study_accession
        )
        query += " OR "
        query += 'secondary_study_accession="{study_accession}"'.format(
            study_accession=study_accession
        )
        query += ")"

        if not allow_non_primary_assembly:
            query += ' AND assembly_type="primary metagenome"'

        data["query"] = query

        response = self.post_request(data)

        if not response.ok:
            logging.debug(
                "Error retrieving study assemblies {}, response code: {}".format(
                    study_accession, response.status_code
                )
            )
            api_error = self._parse_response_error(response)
            raise ValueError(
                f"Could not retrieve assemblies for study {study_accession}. Error {api_error}"
            )
        #   try with different data portal if empty response
        elif response.status_code == 204 or not len(response.json()):
            if retry:
                new_portal = "ena" if data_portal == "metagenome" else "ena"
                return self.get_study_assemblies(
                    study_accession,
                    fields,
                    filter_accessions,
                    allow_non_primary_assembly,
                    new_portal,
                    retry=False,
                )
            else:
                # TODO: this doesn't match other empty responses, which raise a ValueError
                logging.debug(
                    f"There are no assemblies for the study {study_accession}"
                )
                return []

        assemblies = response.json()

        if filter_accessions:
            assemblies = list(
                filter(
                    lambda r: r["analysis_accession"] in filter_accessions, assemblies
                )
            )

        return assemblies

    def get_assembly_from_sample(
        self, sample_name, fields=None, data_portal="metagenome", retry=True
    ):
        """FIXME: no doc string"""
        data = get_default_params()
        data["result"] = "analysis"
        data["fields"] = fields or ASSEMBLY_DEFAULT_FIELDS_STR
        data["query"] = 'sample_accession="{}"'.format(sample_name)
        data["dataPortal"] = data_portal

        response = self.post_request(data)
        if not response.ok:
            logging.error(
                "Error retrieving assembly of sample {}, response code: {}".format(
                    sample_name, response.status_code
                )
            )
            api_error = self._parse_response_error(response)
            raise ValueError(
                f"Could not retrieve assembly for sample {sample_name}. Error {api_error}"
            )
        elif retry and response.status_code == 204:
            new_portal = "ena" if data_portal == "metagenome" else "metagenome"
            return self.get_assembly_from_sample(
                sample_name, fields, new_portal, retry=False
            )
        try:
            assembly = response.json()[0]
        except (json.decoder.JSONDecodeError, IndexError, TypeError, ValueError):
            raise ValueError(f"Could not find assembly of sample {sample_name} in ENA.")

        return assembly

    def get_assembly(
        self, assembly_name, fields=None, data_portal="metagenome", retry=True
    ):
        """FIXME: no doc string"""
        data = get_default_params()
        data["result"] = "analysis"
        data["fields"] = fields or ASSEMBLY_DEFAULT_FIELDS_STR
        data["query"] = 'analysis_accession="{}"'.format(assembly_name)
        data["dataPortal"] = data_portal
        response = self.post_request(data)

        if not response.ok:
            logging.error(
                "Error retrieving assembly {}, response code: {}".format(
                    assembly_name, response.status_code
                )
            )
            api_error = self._parse_response_error(response)
            raise ValueError(
                f"Could not retrieve assembly {assembly_name}. Error {api_error}"
            )
        elif retry and response.status_code == 204:
            new_portal = "ena" if data_portal == "metagenome" else "metagenome"
            return self.get_assembly(assembly_name, fields, new_portal, retry=False)
        try:
            assembly = response.json()[0]
        except (
            IndexError,
            TypeError,
            ValueError,
        ) as exception:
            logging.exception(exception)
            raise ValueError(
                f"There was an error while getting assembly {assembly_name} from ENA."
            )

        return assembly

    @staticmethod
    def requests_retry_session(
        retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None
    ):
        """FIXME: no doc string"""
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_run_raw_size(self, run):
        """Sum the values of fastq_bytes or submitted_bytes."""
        if "fastq_bytes" in run:
            if len(run["fastq_bytes"]):
                return sum([int(s) for s in run["fastq_bytes"].split(";")])
        if "submitted_bytes" in run:
            if len(run["submitted_ftp"]):
                return sum([int(s) for s in run["submitted_bytes"].split(";")])
        logging.warning("Cannot get the RAW read file size.")
        return None

    def get_updated_studies(self, cutoff_date, fields=None):
        """FIXME: no doc string"""
        data = get_default_params()
        data["dataPortal"] = "metagenome"
        data["limit"] = 0
        data["result"] = "study"
        data["fields"] = fields or STUDY_DEFAULT_FIELDS
        data["query"] = "last_updated>={}".format(cutoff_date)

        response = self.post_request(data)
        status_code = str(response.status_code)

        if status_code[0] != "2":
            logging.error(
                "Error retrieving studies, response code: {}".format(
                    response.status_code
                )
            )
            api_error = self._parse_response_error(response)
            raise ValueError(f"Could not retrieve studies. Error {api_error}")
        elif status_code == "204":
            logging.warning("No updated studies found since {}".format(cutoff_date))
            return []
        try:
            studies = response.json()
        except (
            json.decoder.JSONDecodeError,
            IndexError,
            TypeError,
            ValueError,
        ) as exception:
            logging.exception(exception)
            raise ValueError("Could not find studies in ENA.")

        return list(map(self.remap_study_fields, studies))

    def get_updated_runs(self, cutoff_date, fields=None):
        """FIXME: no doc string"""
        data = get_default_params()
        data["limit"] = 0
        data["result"] = "read_run"
        data["dataPortal"] = "metagenome"
        data["fields"] = fields or RUN_DEFAULT_FIELDS_STR
        data["query"] = "last_updated>={}".format(cutoff_date)

        response = self.post_request(data)

        # FIXME: status code to STR?
        status_code = str(response.status_code)
        if status_code[0] != "2":
            logging.error(
                f"Error retrieving run, response code: {response.status_code}"
            )
            api_error = self._parse_response_error(response)
            raise ValueError(f"Could not retrieve runs. Error: {api_error}")
        elif status_code == "204":
            logging.warning("No updated runs found since {}".format(cutoff_date))
            return []
        try:
            # TODO: just use response.json()
            runs = json.loads(response.text)
        except (IndexError, TypeError, ValueError) as exception:
            logging.exception(exception)
            logging.debug(response.text)
            raise ValueError("Could not find runs in ENA.")
        return runs

    # cutoff_date in format YYYY-MM-DD
    def get_updated_tpa_assemblies(self, cutoff_date, fields=None):
        """FIXME: no doc string"""
        data = get_default_params()
        data["dataPortal"] = "metagenome"
        data["limit"] = 0
        data["result"] = "analysis"
        data["fields"] = fields or ASSEMBLY_DEFAULT_FIELDS_STR
        data["query"] = 'assembly_type="{}" AND last_updated>={}'.format(
            "primary metagenome", cutoff_date
        )
        response = self.post_request(data)
        # FIXME: status code to STR?
        status_code = str(response.status_code)
        if status_code[0] != "2":
            logging.debug(
                "Error retrieving assemblies, response code: {}".format(
                    response.status_code
                )
            )
            logging.debug("Response: {}".format(response.text))
            raise ValueError("Could not retrieve assemblies.")
        elif status_code == "204":
            logging.warning("No updated assemblies found since {}".format(cutoff_date))
            return []
        try:
            assemblies = json.loads(response.text)
        except (IndexError, TypeError, ValueError) as e:
            logging.debug(e)
            logging.debug(response.text)
            raise ValueError(
                "Could not find any assemblies in ENA updated after {}".format(
                    cutoff_date
                )
            )
        return assemblies

    # cutoff_date in format YYYY-MM-DD
    def get_updated_assemblies(self, cutoff_date, fields=None):
        """FIXME: no doc string"""
        data = get_default_params()
        data["dataPortal"] = "metagenome"
        data["limit"] = 0
        data["result"] = "assembly"
        data["fields"] = fields or ASSEMBLY_DEFAULT_FIELDS_STR
        data["query"] = "last_updated>={}".format(cutoff_date)
        response = self.post_request(data)
        # FIXME: status code to STR?
        status_code = str(response.status_code)
        if status_code[0] != "2":
            logging.error(
                "Error retrieving assemblies, response code: {}".format(
                    response.status_code
                )
            )
            api_error = self._parse_response_error(response)
            raise ValueError(f"Could not retrieve assemblies. Error {api_error}")
        elif status_code == "204":
            logging.warning("No updated assemblies found since {}".format(cutoff_date))
            return []
        try:
            # TODO: use response.json()
            assemblies = json.loads(response.text)
        except (IndexError, TypeError, ValueError) as exception:
            logging.exception(exception)
            logging.debug(response.text)
            raise ValueError(
                f"Could not find any assemblies in ENA updated after {cutoff_date}"
            )
        return assemblies

    @staticmethod
    def flatten(list_):
        return [item for sublist in list_ for item in sublist]

    def download_runs(self, runs):
        urls = self.flatten(r["fastq_ftp"].split(";") for r in runs)
        download_jobs = [(url, os.path.basename(url)) for url in urls]
        results = ThreadPool(8).imap_unordered(self.fetch_url, download_jobs)

        for path in results:
            logging.info("Downloaded file: {}".format(path))

    def fetch_url(self, entry):
        uri, path = entry
        if "ftp://" not in uri and "http://" not in uri and "https://" not in uri:
            uri = "http://" + uri
        if not os.path.exists(path):
            r = self.requests_retry_session().get(uri, stream=True, timeout=(2, 5))
            if r.status_code == 200:
                with open(path, "wb") as f:
                    for chunk in r:
                        f.write(chunk)
        return path

class NoDataException(ValueError):
    pass
