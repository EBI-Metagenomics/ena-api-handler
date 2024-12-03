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

import rich_click as click

from ena_api.models import STUDY_PRIMARY_ACCESSION_RE, STUDY_SECONDARY_ACCESSION_RE


def validate_study_accession(ctx, param, value) -> bool:
    primary_match = STUDY_PRIMARY_ACCESSION_RE.match(value)
    secondary_match = STUDY_SECONDARY_ACCESSION_RE.match(value)
    if primary_match:
        return primary_match.group(0)
    if secondary_match:
        return secondary_match.group(0)
    raise click.BadParameter(f"Invalid study accession {value}")
