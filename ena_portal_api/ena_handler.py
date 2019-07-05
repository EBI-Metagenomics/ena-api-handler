#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2018 EMBL - European Bioinformatics Institute
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

from __future__ import print_function

import requests
import json
import os
import logging
from multiprocessing.pool import ThreadPool
from time import sleep

ENA_API_URL = os.environ.get('ENA_API_URL', "https://www.ebi.ac.uk/ena/portal/api/search")

STUDY_DEFAULT_FIELDS = 'study_accession,secondary_study_accession,description,study_alias,study_title,' \
                       'tax_id,scientific_name,center_name,first_public'

RUN_DEFAULT_FIELDS = 'study_accession,secondary_study_accession,run_accession,library_source,library_strategy,' \
                     'library_layout,fastq_ftp,fastq_md5,base_count,read_count,instrument_platform,instrument_model,' \
                     'secondary_sample_accession,library_name,sample_alias,sample_title,sample_description'

ASSEMBLY_DEFAULT_FIELDS = 'analysis_accession,study_accession,secondary_study_accession,sample_accession,' \
                          'secondary_sample_accession,analysis_title,analysis_type,center_name,first_public,' \
                          'last_updated,study_title,description, tax_id,scientific_name,analysis_alias,study_alias,' \
                          'submitted_bytes,submitted_md5,submitted_ftp,submitted_aspera,submitted_galaxy,' \
                          'sample_alias,broker_name,sample_title,sample_description,pipeline_name,' \
                          'pipeline_version,assembly_type,description'

logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)


def get_default_connection_headers():
    return {
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*"
        }
    }


def get_default_params():
    return {
        'format': 'json',
        'includeMetagenomes': True,
        'dataPortal': 'metagenome'
    }


def run_filter(d):
    return d['library_strategy'] != 'AMPLICON'


RETRY_COUNT = 5


class EnaApiHandler:
    url = ENA_API_URL

    def __init__(self):
        self.url = "https://www.ebi.ac.uk/ena/portal/api/search"
        if 'ENA_API_USER' in os.environ and 'ENA_API_PASSWORD' in os.environ:
            self.auth = (os.environ['ENA_API_USER'], os.environ['ENA_API_PASSWORD'])
        else:
            self.auth = None

    def post_request(self, data):
        if self.auth:
            response = requests.post(self.url, data=data, auth=self.auth, **get_default_connection_headers())
        else:
            logging.warning('Not authenticated')
            response = requests.post(self.url, data=data, **get_default_connection_headers())
        return response

    # Supports ENA primary and secondary study accessions
    def get_study(self, primary_accession=None, secondary_accession=None, fields=None, attempt=0, search_params=None):
        data = get_default_params()
        data['result'] = 'read_study'
        data['fields'] = fields or STUDY_DEFAULT_FIELDS

        if primary_accession and not secondary_accession:
            data['query'] = 'study_accession="{}"'.format(primary_accession)
        elif not primary_accession and secondary_accession:
            data['query'] = 'secondary_study_accession="{}"'.format(secondary_accession)
        else:
            data['query'] = 'study_accession="{}" AND secondary_study_accession="{}"' \
                .format(primary_accession, secondary_accession)

        if search_params:
            data.update(search_params)

        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            logging.debug(data)
            logging.debug(
                'Error retrieving study {} {}, response code: {}'.format(primary_accession, secondary_accession,
                                                                         response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve runs for study %s %s.', primary_accession, secondary_accession)
        elif response.status_code == 204:
            if not search_params:
                search_params = {}
            if attempt > 1:
                # Try all other result types
                if data['result'] == 'read_study':
                    search_params['result'] = 'analysis_study'
                elif data['result'] == 'analysis_study':
                    search_params['result'] = 'study'
                    fields = data['fields']
                    if 'description' in data['fields']:
                        fields = fields.replace('description', 'study_description')
                    if 'study_alias' in data['fields']:
                        fields = fields.replace('study_alias', 'study_name')
                    search_params['fields'] = fields
                elif data['dataPortal'] == 'ena':
                    search_params['dataPortal'] = 'metagenome'
                else:
                    raise ValueError('Could not find study {} {} in ENA.'.format(primary_accession,
                                                                                 secondary_accession))
                attempt = 0
            attempt += 1
            sleep(1)
            logging.warning(
                'Error 204 when retrieving study {} {} (options {})'.format(primary_accession,
                                                                            secondary_accession,
                                                                            search_params,
                                                                            attempt))
            return self.get_study(primary_accession=primary_accession, secondary_accession=secondary_accession,
                                  fields=fields, attempt=attempt, search_params=search_params)

        try:
            study = json.loads(response.text)[0]
        except (IndexError, TypeError, ValueError, KeyError) as e:
            logging.error(e)
            logging.error(response.status_code)
            logging.error(response.text)
            raise ValueError('Could not find study {} {} in ENA.'.format(primary_accession, secondary_accession))
        if data['result'] == 'study':
            return self.remap_study_fields(study)
        return study

    @staticmethod
    def remap_study_fields(data):
        if 'study_description' in data:
            data['description'] = data.pop('study_description')
        if 'study_name' in data:
            data['study_alias'] = data.pop('study_name')
        return data

    def get_run(self, run_accession, fields=None, public=True, attempt=0, search_params=None):
        data = get_default_params()
        data['result'] = 'read_run'
        data['fields'] = fields or RUN_DEFAULT_FIELDS
        data['query'] = 'run_accession=\"{}\"'.format(run_accession)

        if search_params:
            data.update(search_params)

        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            logging.debug('Error retrieving run {}, response code: {}'.format(run_accession, response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve run with accession %s.', run_accession)
        elif response.status_code == 204:
            if attempt < 2:
                attempt += 1
                sleep(1)
                logging.warning('Error 204 when retrieving run {} in dataPortal {}, '
                                'retrying {}'.format(run_accession,
                                                     data.get('dataPortal'),
                                                     attempt))

                return self.get_run(run_accession=run_accession, fields=fields, public=public, attempt=attempt,
                                    search_params=search_params)
            elif attempt == 2 and data['dataPortal'] != 'ena':
                return self.get_run(run_accession=run_accession, fields=fields, public=public,
                                    search_params={'dataPortal': 'ena'})
            else:
                raise ValueError('Could not find run {} in ENA after {} attempts'.format(run_accession, RETRY_COUNT))
        try:
            run = json.loads(response.text)[0]
        except (IndexError, TypeError, ValueError):
            raise ValueError('Could not find run {} in ENA.'.format(run_accession))

        if fields is None or 'raw_data_size' in fields:
            if public and 'fastq_ftp' in run and len(run['fastq_ftp']) > 0:
                run['raw_data_size'] = self.get_run_raw_size(run)
            elif public and 'submitted_ftp' in run and len(run['submitted_ftp']) > 0:
                run['raw_data_size'] = self.get_run_raw_size(run, 'submitted_ftp')
            else:
                run['raw_data_size'] = None

        for int_param in ('read_count', 'base_count'):
            if int_param in run:
                try:
                    run[int_param] = int(run[int_param])
                except ValueError as e:
                    if not public:
                        raise e
                    run[int_param] = -1
        return run

    def get_study_runs(self, study_sec_acc, fields=None, filter_assembly_runs=True, private=False,
                       filter_accessions=None, search_params=None):
        data = get_default_params()
        data['result'] = 'read_run'
        data['fields'] = fields or RUN_DEFAULT_FIELDS
        data['query'] = 'secondary_study_accession=\"{}\"'.format(study_sec_acc)

        if search_params:
            data.update(search_params)

        if filter_assembly_runs and 'library_strategy' not in data['fields']:
            data['fields'] += ',library_strategy'
        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            logging.debug(
                'Error retrieving study runs {}, response code: {}'.format(study_sec_acc, response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve runs for study %s.', study_sec_acc)
        elif response.status_code == 204:
            return []
        runs = json.loads(response.text)
        if filter_assembly_runs:
            runs = list(filter(run_filter, runs))
        if filter_accessions:
            runs = list(filter(lambda r: r['run_accession'] in filter_accessions, runs))

        for run in runs:
            if not private and 'fastq_ftp' in run and len(run['fastq_ftp']) > 0:
                run['raw_data_size'] = self.get_run_raw_size(run)
            elif not private and 'submitted_ftp' in run and len(run['submitted_ftp']) > 0:
                run['raw_data_size'] = self.get_run_raw_size(run, field='submitted_ftp')
            else:
                run['raw_data_size'] = None

            for int_param in ('read_count', 'base_count'):
                if int_param in run:
                    try:
                        run[int_param] = int(run[int_param])
                    except ValueError:
                        run[int_param] = None
        return runs

    # Specific fo
    def get_study_assemblies(self, study_accession, fields=None, filter_accessions=None,
                             allow_non_primary_assembly=False):
        data = get_default_params()
        data['result'] = 'analysis'
        data['fields'] = fields or ASSEMBLY_DEFAULT_FIELDS
        query = '(study_accession=\"{study_accession}\" ' \
                'OR secondary_study_accession=\"{study_accession}\") '.format(study_accession=study_accession)
        if not allow_non_primary_assembly:
            query += ' AND assembly_type=\"primary metagenome\"'
        data['query'] = query
        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            logging.debug(
                'Error retrieving study assemblies {}, response code: {}'.format(study_accession,
                                                                                 response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve assemblies for study %s.', study_accession)
        elif response.status_code == 204:
            return []
        assemblies = json.loads(response.text)
        if filter_accessions:
            assemblies = list(filter(lambda r: r['analysis_accession'] in filter_accessions, assemblies))

        return assemblies

    def get_assembly(self, assembly_name, fields=None):
        data = get_default_params()
        data['result'] = 'analysis'
        data['fields'] = fields or ASSEMBLY_DEFAULT_FIELDS
        data['query'] = 'analysis_accession=\"{}\"'.format(assembly_name)

        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            logging.debug(
                'Error retrieving assembly {}, response code: {}'.format(assembly_name, response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve assembly %s.', assembly_name)

        try:
            assembly = json.loads(response.text)[0]
        except (IndexError, TypeError, ValueError):
            raise ValueError('Could not find assembly {} in ENA.'.format(assembly_name))

        return assembly

    # def get_study_assembly_accessions(self, study_prim_acc):
    #     try:
    #         return [assembly['analysis_accession'] for assembly in
    #                 self.get_study_assemblies(study_prim_acc, 'analysis_accession')]
    #     except ValueError:
    #         return []

    def get_run_raw_size(self, run, field='fastq_ftp'):
        urls = run[field].split(';')
        return sum(
            [int(requests.head('http://' + url, auth=self.auth).headers.get('content-length') or 0) for url in urls])

    def get_updated_studies(self, cutoff_date, fields=None):
        data = get_default_params()
        data['dataPortal'] = 'metagenome'
        data['limit'] = 0
        data['result'] = 'study'
        data['fields'] = fields or STUDY_DEFAULT_FIELDS
        data['query'] = 'last_updated>={}'.format(cutoff_date)
        response = self.post_request(data)
        status_code = str(response.status_code)
        if status_code[0] != '2':
            logging.debug('Error retrieving studies, response code: {}'.format(response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve studies.')
        elif status_code == '204':
            logging.warning('No updated studies found since {}'.format(cutoff_date))
            return []
        try:
            studies = json.loads(response.text)
        except (IndexError, TypeError, ValueError) as e:
            logging.debug(e)
            logging.debug(response.text)
            raise ValueError('Could not find studies in ENA.')
        return list(map(self.remap_study_fields, studies))

    def get_updated_runs(self, cutoff_date, fields=None):
        data = get_default_params()
        data['limit'] = 0
        data['result'] = 'read_run'
        data['dataPortal'] = 'metagenome'
        data['fields'] = fields or RUN_DEFAULT_FIELDS
        data['query'] = 'last_updated>={}'.format(cutoff_date)
        response = self.post_request(data)
        status_code = str(response.status_code)
        if status_code[0] != '2':
            logging.debug('Error retrieving run, response code: {}'.format(response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve runs.')
        elif status_code == '204':
            logging.warning('No updated runs found since {}'.format(cutoff_date))
            return []
        try:
            runs = json.loads(response.text)
        except (IndexError, TypeError, ValueError) as e:
            logging.debug(e)
            logging.debug(response.text)
            raise ValueError('Could not find runs in ENA.')
        return runs

    # cutoff_date in format YYYY-MM-DD
    def get_updated_tpa_assemblies(self, cutoff_date, fields=None):
        data = get_default_params()
        data['dataPortal'] = 'metagenome'
        data['limit'] = 0
        data['result'] = 'analysis'
        data['fields'] = fields or ASSEMBLY_DEFAULT_FIELDS
        data['query'] = 'assembly_type="{}" AND last_updated>={}'.format('primary metagenome', cutoff_date)
        response = self.post_request(data)
        status_code = str(response.status_code)
        if status_code[0] != '2':
            logging.debug('Error retrieving assemblies, response code: {}'.format(response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve assemblies.')
        elif status_code == '204':
            logging.warning('No updated assemblies found since {}'.format(cutoff_date))
            return []
        try:
            assemblies = json.loads(response.text)
        except (IndexError, TypeError, ValueError) as e:
            logging.debug(e)
            logging.debug(response.text)
            raise ValueError('Could not find any assemblies in ENA updated after {}'.format(cutoff_date))
        return assemblies

    # cutoff_date in format YYYY-MM-DD
    def get_updated_assemblies(self, cutoff_date, fields=None):
        data = get_default_params()
        data['dataPortal'] = 'metagenome'
        data['limit'] = 0
        data['result'] = 'assembly'
        data['fields'] = fields or ASSEMBLY_DEFAULT_FIELDS
        data['query'] = 'last_updated>={}'.format(cutoff_date)
        response = self.post_request(data)
        status_code = str(response.status_code)
        if status_code[0] != '2':
            logging.debug('Error retrieving assemblies, response code: {}'.format(response.status_code))
            logging.debug('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve assemblies.')
        elif status_code == '204':
            logging.warning('No updated assemblies found since {}'.format(cutoff_date))
            return []
        try:
            assemblies = json.loads(response.text)
        except (IndexError, TypeError, ValueError) as e:
            logging.debug(e)
            logging.debug(response.text)
            raise ValueError('Could not find any assemblies in ENA updated after {}'.format(cutoff_date))
        return assemblies


def flatten(l):
    return [item for sublist in l for item in sublist]


def download_runs(runs):
    urls = flatten(r['fastq_ftp'].split(';') for r in runs)
    download_jobs = [(url, os.path.basename(url)) for url in urls]
    results = ThreadPool(8).imap_unordered(fetch_url, download_jobs)

    for path in results:
        logging.info('Downloaded file: {}'.format(path))


FNULL = open(os.devnull, 'w')


def fetch_url(entry):
    uri, path = entry
    if 'ftp://' not in uri and 'http://' not in uri and 'https://' not in uri:
        uri = 'http://' + uri
    if not os.path.exists(path):
        r = requests.get(uri, stream=True)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in r:
                    f.write(chunk)
    return path
