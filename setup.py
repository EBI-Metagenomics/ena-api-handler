from setuptools import setup, find_packages

import os
import sys

version = "1.0.2"

_base = os.path.dirname(os.path.abspath(__file__))
_requirements = os.path.join(_base, 'requirements.txt')
_requirements_test = os.path.join(_base, 'requirements-test.txt')
_env_activate = os.path.join(_base, 'venv', 'bin', 'activate')

install_requirements = []
with open(_requirements) as f:
    install_requirements = f.read().splitlines()

test_requirements = []
if "test" in sys.argv:
    with open(_requirements_test) as f:
        test_requirements = f.read().splitlines()

setup(
    name="ena_api_libs",
    author='Miguel Boland, Maxim Scheremetjew',
    author_email='mdb@ebi.ac.uk, maxim@ebi.ac.uk',
    version=version,
    packages=['ena_portal_api'],
    install_requires=install_requirements,
    include_package_data=True,
    tests_require=test_requirements,
    test_suite="tests",
    setup_requires=['pytest-runner'],
)
