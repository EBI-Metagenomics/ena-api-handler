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

import os
from dotenv import load_dotenv
import unittest
from unittest import mock
from unittest.mock import patch
import re

import pytest
import requests

from ena_portal_api import ena_handler
from tests.test_constants import *
from tests.test_utils import *

load_dotenv("secrets.env")
ENA_API_USER = os.environ.get("ENA_API_USER")
ENA_API_PASSWORD = os.environ.get("ENA_API_PASSWORD")

class MockResponse:
    def __init__(self, status_code, data=None, text=None):
        self.status_code = status_code
        self.data = data
        self.text = text

    def json(self):
        return self.data

    @property
    def ok(self):
        return self.status_code < 400

class TestEnaHandler:
    @pytest.mark.parametrize(
        "accession_arg",
        (
            {"primary_accession": "PRJEB1787"},
            {"secondary_accession": "ERP001736"},
            {"primary_accession": "PRJEB1787", "secondary_accession": "ERP001736"},
        ),
    )
    def test_get_study_from_accessions_should_retrieve_default_fields(
        self, accession_arg
    ):
        """This will iterate over all cases above. It will test each accession
        type individual and together.
        :param accession_arg:
        :return:
        """
        ena = ena_handler.EnaApiHandler()
        print(ena.auth)
        study = ena.get_study(**accession_arg)
        assert isinstance(study, dict)
        assert len(study.keys()) == len(ena_handler.STUDY_DEFAULT_FIELDS.split(","))

    @pytest.mark.parametrize(
        "accession_arg",
        (
            {"secondary_accession": "ERP001736"},
            {"primary_accession": "PRJEB1787"},
            {"primary_accession": "PRJEB1787", "secondary_accession": "ERP001736"},
        ),
    )
    def test_get_study_secondary_accession_should_retrieve_study_filtered_fields(
        self, accession_arg
    ):
        ena = ena_handler.EnaApiHandler()
        study = ena.get_study(fields="study_accession", **accession_arg)
        assert isinstance(study, dict)
        assert len(study.keys()) == 1
        assert "study_accession" in study

    def test_get_study_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError):
            ena.get_study("Invalid accession")

    def test_get_study_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_study("ERP001736")

    def test_get_study_api_no_results(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(204, text=None)
        with pytest.raises(ValueError):
            ena.get_study("ERP001736")

    def test_get_run_should_retrieve_run_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        run = ena.get_run("ERR1701760")
        assert isinstance(run, dict)
        # The API returns sample_accession by default, if you specify any of return
        assert check_fields(list_of_fields=ena_handler.RUN_DEFAULT_FIELDS, result=run)

    def test_get_run_should_retrieve_run_filtered_fields(self):
        ena = ena_handler.EnaApiHandler()
        run = ena.get_run("ERR1701760", fields="run_accession")
        assert isinstance(run, dict)
        assert len(run) == 1
        assert "run_accession" in run

    def test_get_run_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError):
            ena.get_run("Invalid accession")

    def test_get_run_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_run("ERR1701760")

    def test_get_run_api_invalid_fields(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError) as exc_info:
            ena.get_run("ERR1701760", fields="not_valid,doesnt_exist")
        error_message = (
            "Could not retrieve run with accession ERR1701760. "
            "Error: Invalid fieldName(s) supplied: not_valid,doesnt_exist"
        )
        assert exc_info.value.args[0] == error_message

    def test_get_run_should_try_with_different_data_portal(self):
        ena = ena_handler.EnaApiHandler()
        original_ena_post_request = ena.post_request

        def mock_response_handler(data):
            if data["dataPortal"] == "metagenome":
                return MockResponse(204)
            else:
                return original_ena_post_request(data)

        ena.post_request = mock_response_handler
        run = ena.get_run("ERR1701760", fields="run_accession")
        assert run == {"run_accession": "ERR1701760"}

    def test_get_run_should_fail_after_N_retries(self):
        # TODO: this and the sample one are very similiar, refactor
        ena = ena_handler.EnaApiHandler()

        def mock_response_handler(data):
            return MockResponse(204, data={"message": "Mock error"})

        ena.post_request = mock_response_handler
        error_message = (
            f"Could not find run ERRXXXX in ENA after {ena_handler.RETRY_COUNT} "
            "attempts. ENA response: Mock error"
        )
        with pytest.raises(ValueError, match=error_message):
            ena.get_run("ERRXXXX")

    def test_get_study_runs_should_have_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        runs = ena.get_study_runs("SRP125161")
        assert len(runs) == 4
        for run in runs:
            assert check_fields(
                list_of_fields=ena_handler.RUN_DEFAULT_FIELDS, result=run
            )
            assert isinstance(run, dict)

    def test_get_study_runs_should_have_filter_run_accessions(self):
        ena = ena_handler.EnaApiHandler()
        runs = ena.get_study_runs("SRP125161", filter_accessions=["SRR6301444"])
        assert len(runs) == 1
        for run in runs:
            assert check_fields(
                list_of_fields=ena_handler.RUN_DEFAULT_FIELDS, result=run
            )
            assert isinstance(run, dict)

    @patch("ena_portal_api.ena_handler.EnaApiHandler.post_request")
    @patch("ena_portal_api.ena_handler.json.loads")
    def test_get_study_runs_should_not_fetch_size_if_private(
        self, mock_json_load, mock_post_request
    ):
        ena = ena_handler.EnaApiHandler()
        response = requests.Response()
        response.status_code = 200
        mock_post_request.return_value = response
        mock_json_load.return_value = MOCKED_RUNS

        runs = ena.get_study_runs(
            "SRP125161", filter_accessions=["SRR6301444"]
        )  # private=True was removed
        assert len(runs) == 1
        for run in runs:
            assert 20 == len(run)
            assert isinstance(run, dict)
            assert run["raw_data_size"] is None

    def test_get_study_runs_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        assert [] == ena.get_study_runs("Invalid accession")

    def test_get_study_runs_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_study_runs("SRP125161")

    def test_get_study_assemblies_should_have_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        assemblies = ena.get_study_assemblies("ERP112609")
        for assembly in assemblies:
            assert len(assembly) == 35
            assert isinstance(assembly, dict)

    def test_get_study_assemblies_should_filter_fields(self):
        ena = ena_handler.EnaApiHandler()
        assemblies = ena.get_study_assemblies(
            "ERP112609", fields="analysis_accession,study_accession"
        )
        for assembly in assemblies:
            assert len(assembly) == 2
            assert isinstance(assembly, dict)

    def test_get_study_assemblies_should_filter_accessions(self):
        ena = ena_handler.EnaApiHandler()
        assemblies = ena.get_study_assemblies(
            "ERP112609", filter_accessions=["GCA_001751075"]
        )
        for assembly in assemblies:
            assert len(assembly) == 35
            assert isinstance(assembly, dict)

    def test_get_study_assemblies_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        assert [] == ena.get_study_assemblies("Invalid accession")

    def test_get_study_assemblies_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_study_assemblies("ERP112609")

    def test_get_assembly_should_have_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        assembly = ena.get_assembly("ERZ1669402")
        assert check_fields(
            list_of_fields=ena_handler.ASSEMBLY_DEFAULT_FIELDS, result=assembly
        )
        assert isinstance(assembly, dict)

    def test_get_assembly_should_filter_fields(self):
        ena = ena_handler.EnaApiHandler()
        assembly = ena.get_assembly(
            "ERZ1669402", fields="analysis_accession,study_accession"
        )
        assert len(assembly) == 2
        assert isinstance(assembly, dict)

    def test_get_assembly_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_assembly("ERZ795049")

    def test_get_assembly_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError):
            ena.get_assembly("Invalid_accession")

    @unittest.skipIf(
        "GITHUB_ACTIONS" in os.environ,
        "Skipping this test on GITHUB_ACTIONS CI.",
    )
    def test_download_runs(self, tmpdir):
        tmpdir = tmpdir.strpath
        current_dir = os.getcwd()
        os.chdir(tmpdir)
        run = {
            "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/ERR866/ERR866589/ERR866589_1.fastq.gz;"  # noqa: E501
            "ftp.sra.ebi.ac.uk/vol1/fastq/ERR866/ERR866589/ERR866589_2.fastq.gz"
        }

        ena = ena_handler.EnaApiHandler()
        try:
            ena.download_runs([run])
            fs = os.listdir(tmpdir)
            assert 2 == len(fs)
            assert "ERR866589_1.fastq.gz" in fs
            assert "ERR866589_2.fastq.gz" in fs

            os.chdir(current_dir)
        except requests.exceptions.ConnectionError:
            # If Max retries exceeded with url
            # then there is no way of running this test successfully
            assert True

    def test_get_study_runs_should_return_all_accessions(self):
        ena = ena_handler.EnaApiHandler()
        assert set([r["run_accession"] for r in ena.get_study_runs("ERP000339")]) == {
            "ERR019477",
            "ERR019478",
        }

    def test_get_study_runs_should_not_return_amplicons(self):
        ena = ena_handler.EnaApiHandler()
        assert len([r["run_accession"] for r in ena.get_study_runs("ERP001506")]) == 18

    def test_get_study_runs_should_return_all_accessions_including_amplicon(self):
        ena = ena_handler.EnaApiHandler()
        assert (
            len(
                [
                    r["run_accession"]
                    for r in ena.get_study_runs("ERP001506", filter_assembly_runs=False)
                ]
            )
            == 24
        )

    @unittest.skip("Raised with: ENA #659864")
    def test_get_study_assembly_accessions_should_return_all_accessions(self):
        ena = ena_handler.EnaApiHandler()
        expected = set(
            [
                "ERZ1669402",
                "ERZ1669403",
                "ERZ1669404",
                "ERZ1669405",
                "ERZ1669406",
                "ERZ1669411",
                "ERZ1669412",
                "ERZ1669415",
                "ERZ1669417",
            ]
        )

        assert check_fields(
            list_of_fields=expected,
            result=[
                a["analysis_accession"] for a in ena.get_study_assemblies("ERP124933")
            ],
        )

    def test_get_study_assembly_accessions_should_return_empty_list_if_study_contains_no_assemblies(  # noqa: E501
        self,
    ):
        ena = ena_handler.EnaApiHandler()
        assert len(ena.get_study_assemblies("PRJEB2280")) == 0

    def test_get_study_runs_should_return_empty_list_if_study_contains_no_runs(self):
        ena = ena_handler.EnaApiHandler()
        assert len(ena.get_study_runs("ERP105889")) == 0

    def test_get_sample_valid_accession(self):
        ena = ena_handler.EnaApiHandler()
        sample = ena.get_sample("SAMN11835464")
        expected_sample = {
            "sample_accession": "SAMN11835464",
            "secondary_sample_accession": "SRS5453518",
            "sample_alias": "T8",
            "description": "human metagenome isolated from Oncocytoma of the kidney",
            "tax_id": "646099",
            "scientific_name": "human metagenome",
            "host_tax_id": "",
            "host_status": "",
            "host_sex": "",
            "submitted_host_sex": "",
            "host_body_site": "",
            "host_gravidity": "",
            "host_genotype": "",
            "host_phenotype": "",
            "host_growth_conditions": "",
            "collection_date": "2015-03-24",
            "collected_by": "",
            "country": "Italy: Aquila",
            "location": "42.21 N 13.24 E",
            "depth": "",
            "altitude": "",
            "elevation": "",
            "first_public": "2019-05-22",
            "checklist": "",
            "center_name": "University of Milan",
            "broker_name": "NCBI",
            "investigation_type": "",
            "experimental_factor": "",
            "environment_biome": "",
            "environment_feature": "",
            "environment_material": "",
            "temperature": "",
            "salinity": "",
            "ph": "",
            "sample_collection": "",
            "project_name": "",
            "target_gene": "",
            "sequencing_method": "",
            "sample_title": "Metagenome or environmental sample from human metagenome",
            'host': "Homo sapiens",
            'ncbi_reporting_standard': "Metagenome or environmental",
            'status': "public",
        }
        assert sample == expected_sample

    def test_get_sample_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError, match="Could not find sample FAKESAMPLE in ENA"):
            sample = ena.get_sample("FAKESAMPLE")
            assert sample is None

    def test_get_sample_unkown_fields(self):
        ena = ena_handler.EnaApiHandler()
        expected_error = (
            "Could not retrieve sample with accession SAMN11835464."
            " Error: Invalid fieldName(s) supplied: "
            "wrong_field,and_very_wrong_field"
        )
        with pytest.raises(ValueError, match=re.escape(expected_error)):
            ena.get_sample("SAMN11835464", fields="wrong_field,and_very_wrong_field")

    def test_get_sample_with_retry(self):
        ena = ena_handler.EnaApiHandler()
        original_ena_post_request = ena.post_request

        def mock_response_handler(data):
            if data["dataPortal"] == "metagenome":
                return MockResponse(204)
            else:
                return original_ena_post_request(data)

        ena.post_request = mock_response_handler
        sample = ena.get_sample("SAMN11835464", fields="sample_accession")
        assert sample == {"sample_accession": "SAMN11835464"}

    def test_get_sample_api_fail(self):
        ena = ena_handler.EnaApiHandler()

        def mock_response_handler(data):
            return MockResponse(404, data={"message": "ENA API ERROR"})

        ena.post_request = mock_response_handler
        error_message = (
            "Could not retrieve sample with accession"
            " SAMN11835464. Error: ENA API ERROR"
        )
        with pytest.raises(ValueError, match=error_message):
            sample = ena.get_sample("SAMN11835464", fields="sample_accession")
            assert sample is None

    def test_get_sample_api_fail_response_not_json(self):
        ena = ena_handler.EnaApiHandler()

        def mock_response_handler(data):
            return MockResponse(404, text="ENA API ERROR")

        ena.post_request = mock_response_handler
        error_message = (
            "Could not retrieve sample with accession"
            " SAMN11835464. Error: ENA API ERROR"
        )
        with pytest.raises(ValueError, match=error_message):
            sample = ena.get_sample("SAMN11835464", fields="sample_accession")
            assert False
            assert sample is None

    def test_get_sample_should_fail_after_N_retries(self):
        ena = ena_handler.EnaApiHandler()

        def mock_response_handler(data):
            return MockResponse(204, data={"message": "Mock error"})

        ena.post_request = mock_response_handler
        error_message = (
            f"Could not find sample ERSXXXX in ENA after {ena_handler.RETRY_COUNT} "
            "attempts. "
            "ENA response: Mock error"
        )
        with pytest.raises(ValueError, match=error_message):
            ena.get_sample("ERSXXXX")
