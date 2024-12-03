#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2018-2024 EMBL - European Bioinformatics Institute
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

from enum import Enum
from typing import Optional

import httpx

from ena_api.models import Study, StudyReportAPI
from ena_api.settings import Settings


class DataPortal(Enum):
    ena = "ena"
    faang = "faang"
    metagenome = "metagenome"
    pathogen = "pathogen"


class Format(Enum):
    json = "json"
    tsv = "tsv"


class ResultType(Enum):
    study = "study"
    sample = "sample"
    read_run = "read_run"  # Raw reads
    analysis = "analysis"  # Assemblies in our case - Nucleotide sequence analyses from reads


def get_study(study_accession: str, private: bool = False) -> Optional[Study]:
    """Get a Study from the Portal API
    :param study_accession: The study primary or secondary accession
    :type study_accession: str
    :param private: Whether the study is private
    :type private: bool
    :return: Study or None
    """
    settings = Settings()

    # For private data we have to use the Submission API (which has different fields)
    if private:
        response = httpx.get(
            f"{settings.report_api}/studies/{study_accession}",
            auth=(settings.webin, settings.password),
            params={"format": "json"},
        )
        response.raise_for_status()
        data = response.json()
        if data:
            return StudyReportAPI.model_validate(data[0].get("report")).convert_to_study()
        return

    # Public data
    study_ena_query = {
        "result": ResultType.study.value,
        "query": f'study_accession="{study_accession}" OR secondary_study_accession="{study_accession}"',
        "dataPortal": DataPortal.ena.value,
        "fields": ",".join(Study.model_fields.keys()),
        "format": Format.json.value,
    }

    response = httpx.get(f"{settings.portal_api}", params=study_ena_query)

    if response.status_code == 204:
        raise Exception()

    response.raise_for_status()
    # The response is a list #
    return Study.model_validate(response.json()[0])


# def get_study_readruns_from_ena(
#     accession: str, limit: int = 20, filter_library_strategy: str = None
# ) -> List[str]:
#     logger = get_run_logger()
#
#     # api call arguments
#     query = f'"(study_accession={accession} OR secondary_study_accession={accession})"'
#     if filter_library_strategy:
#         query = f'{query[:-1]} AND library_strategy={filter_library_strategy}"'
#     query = query.replace('"', "%22")
#
#     fields = ",".join(EMG_CONFIG.ena.readrun_metadata_fields)
#     result_type = "read_run"
#
#     logger.info(f"Will fetch study {accession} read-runs from ENA portal API")
#
#     mgys_study = analyses.models.Study.objects.get(ena_study__accession=accession)
#
#     portal = httpx.get(
#         create_ena_api_request(result_type=result_type, query=query, limit=limit, fields=fields)
#         + "&dataPortal=metagenome"
#     )
#     if not portal.status_code == httpx.codes.OK:
#         raise Exception(f"Bad status! {portal.status_code} {portal}")
