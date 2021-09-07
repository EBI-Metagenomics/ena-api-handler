# -*- coding: utf-8 -*-

# Copyright 2018-2021 EMBL - European Bioinformatics Institute
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

from setuptools import setup

import os
import sys

version = "1.0.5"

_base = os.path.dirname(os.path.abspath(__file__))
_requirements = os.path.join(_base, "requirements.txt")
_requirements_test = os.path.join(_base, "requirements-test.txt")
_env_activate = os.path.join(_base, "venv", "bin", "activate")

install_requirements = []
with open(_requirements) as f:
    install_requirements = f.read().splitlines()

test_requirements = []
if "test" in sys.argv:
    with open(_requirements_test) as f:
        test_requirements = f.read().splitlines()

setup(
    name="ena_api_libs",
    author="Miguel Boland, Maxim Scheremetjew",
    author_email="mdb@ebi.ac.uk, maxim@ebi.ac.uk",
    version=version,
    packages=["ena_portal_api"],
    install_requires=install_requirements,
    include_package_data=True,
    tests_require=test_requirements,
    test_suite="tests",
    setup_requires=["pytest-runner"],
)
