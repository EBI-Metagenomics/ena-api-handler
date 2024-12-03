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


# ena <model> --study-accession ""

import rich_click as click
from rich import print_json

from ena_api import cli_validators, ena


@click.group()
@click.version_option()
def main():
    """Entry point."""
    pass


@click.command()
@click.option(
    "--study-accession",
    callback=cli_validators.validate_study_accession,
    required=True,
    help="Get a study by study primary or secondary accession",
)
@click.option("--private", is_flag=True, default=False, help="Enable private accessions")
def studies(study_accession: str, private: bool):
    print_json(ena.get_study(study_accession, private=private).model_dump_json())


main.add_command(studies)

if __name__ == "__main__":
    main()
