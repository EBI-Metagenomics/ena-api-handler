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

import re
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field

STUDY_PRIMARY_ACCESSION_PATTERN = "(PRJ[EDN][A-Z][0-9]+)"
STUDY_SECONDARY_ACCESSION_PATTERN = "(E|D|S)RP[0-9]{6,}"

STUDY_PRIMARY_ACCESSION_RE = re.compile(STUDY_PRIMARY_ACCESSION_PATTERN)
STUDY_SECONDARY_ACCESSION_RE = re.compile(STUDY_SECONDARY_ACCESSION_PATTERN)


class Study(BaseModel):
    study_accession: str = Field(pattern=STUDY_PRIMARY_ACCESSION_PATTERN)
    # TODO adjust when the secondary accession has multiples accession!
    secondary_study_accession: str = Field(pattern=STUDY_SECONDARY_ACCESSION_PATTERN)
    description: str
    study_alias: str
    study_title: str
    tax_id: Optional[int]
    scientific_name: Optional[str]
    center_name: Optional[str]
    first_public: Optional[date]


class StudyReportAPI(BaseModel):
    """The report API returns different fields compared to the Portal API
    This model is used to validate the studies coming from the report API,
    and it provides a method to convert into a "canonical" (portalAPI) Study
    """

    id: str = Field(pattern="(E|D|S)RP[0-9]{6,}")
    secondary_id: str = Field(alias="secondaryId", pattern="(PRJ[EDN][A-Z][0-9]+)")
    submission_account_id: str = Field(alias="submissionAccountId", pattern=r"Webin-\d+")
    alias: str
    first_created: Optional[datetime] = Field(alias="firstCreated")  # 2024-11-15T13:55:08
    first_public: Optional[date] = Field(alias="firstPublic")
    release_status: str = Field(alias="releaseStatus")  # possible values? -> "PRIVATE"
    title: str

    def convert_to_study(self) -> Study:
        # TODO: how dow e get the tax_id and scientific name for private studies?
        # The id and the secondaryId are different than the Study study primary and secondary accessions.
        return Study(
            study_accession=self.secondary_id,
            secondary_study_accession=self.id,
            description="",
            study_alias=self.alias,
            study_title=self.title,
            tax_id=None,
            scientific_name=None,
            center_name=None,
            first_public=self.first_public,
        )


class Run(BaseModel):
    primary_accession: str
    secondary_accession: str
