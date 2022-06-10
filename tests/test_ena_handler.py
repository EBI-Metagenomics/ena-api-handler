# -*- coding: utf-8 -*-

# Copyright 2018-2022 EMBL - European Bioinformatics Institute
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
import unittest
from unittest import mock
from unittest.mock import patch

import pytest
import requests

from ena_portal_api import ena_handler


class MockResponse:
    def __init__(self, status_code, data=None, text=None):
        self.data = data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self.data

    @property
    def ok(self):
        return self.status_code < 400


MOCKED_RUNS = [
    {
        "sample_accession": "SAMN06251743",
        "study_accession": "PRJNA362212",
        "secondary_study_accession": "SRP125161",
        "run_accession": "SRR6301444",
        "library_source": "METAGENOMIC",
        "library_strategy": "WGS",
        "library_layout": "PAIRED",
        "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/SRR630/004/SRR6301444/SRR6301444_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/SRR630/004/SRR6301444/SRR6301444_2.fastq.gz",  # noqa: E501
        "fastq_md5": "1b9be155ad5ee224640a75fd8cdddc58;8379e6b1aa9a83bd0a0c3e0349a880de",  # noqa: E501
        "base_count": "65506757180",
        "read_count": "260375006",
        "instrument_platform": "ILLUMINA",
        "instrument_model": "Illumina HiSeq 2500",
        "secondary_sample_accession": "SRS2696342",
        "library_name": "4484_3-4cm",
        "sample_alias": "Metagenome from Guaymas Basin sediment dive 4484 depth 3-4cm",  # noqa: E501
        "sample_title": "Metagenome from Guaymas Basin sediment dive 4484 depth 3-4cm",  # noqa: E501
        "sample_description": "Metagenome from Guaymas Basin sediment dive 4484 depth 3-4cm",  # noqa: E501
        "first_public": "",
    }
]


class TestEnaHandler:
    @mock.patch.dict(
        os.environ, {"ENA_API_USER": "username", "ENA_API_PASSWORD": "password"}
    )
    def test_authentication_set(self):
        ena = ena_handler.EnaApiHandler()
        assert ena.auth == ("username", "password")

    def test_authentication_not_set(self):
        if "ENA_API_USER" in os.environ:
            del os.environ["ENA_API_USER"]
        if "ENA_API_PASSWORD" in os.environ:
            del os.environ["ENA_API_PASSWORD"]
        ena = ena_handler.EnaApiHandler()
        assert ena.auth is None

    @mock.patch.dict(
        os.environ, {"ENA_API_USER": "username", "ENA_API_PASSWORD": "password"}
    )
    def test_authentication_set_in_constructor(self):
        ena = ena_handler.EnaApiHandler(username="username1", password="password1")
        assert ena.auth == ("username1", "password1")

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
        assert 22 == len(run)

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

    def test_get_study_runs_should_have_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        runs = ena.get_study_runs("SRP125161")
        assert len(runs) == 4
        for run in runs:
            assert 22 == len(run)
            assert isinstance(run, dict)

    def test_get_study_runs_should_have_filter_run_accessions(self):
        ena = ena_handler.EnaApiHandler()
        runs = ena.get_study_runs("SRP125161", filter_accessions=["SRR6301444"])
        assert len(runs) == 1
        for run in runs:
            assert 22 == len(run)
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
        assert len(assembly) == 35
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
        assert {
            a["analysis_accession"] for a in ena.get_study_assemblies("ERP124933")
        } == expected

    def test_get_study_assembly_accessions_should_return_empty_list_if_study_contains_no_assemblies(  # noqa: E501
        self,
    ):
        ena = ena_handler.EnaApiHandler()
        assert len(ena.get_study_assemblies("PRJEB2280")) == 0

    def test_get_study_runs_should_return_empty_list_if_study_contains_no_runs(self):
        ena = ena_handler.EnaApiHandler()
        assert len(ena.get_study_runs("ERP105889")) == 0
