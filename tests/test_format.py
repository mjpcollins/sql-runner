from unittest import TestCase
from utils.format import (
    enrich_request,
    combine_request_and_job_settings
)


class TestFormat(TestCase):

    def test_enrich_request(self):
        request = {'repo': 'a_repo',
                   'params': 'some_params'}
        expected_enriched_request = {'repo': 'a_repo',
                                     'params': 'some_params',
                                     'params_import_path': 'a_repo.sql.some_params',
                                     'sql_file_path': 'a_repo/sql/',
                                     'sql_validation_tests_file_path': 'a_repo/sql_tests/'}
        enrich_request(request)
        self.assertEqual(expected_enriched_request, request)

    def test_combine_request_and_job_settings(self):
        job_settings = {'DESCRIPTION': 'Table to create a unique joining column',
                        'OVERWRITE': 'False',
                        'PARTITION': 'True',
                        'PARTITION_BY': 'DATE'}
        request = {'repo': 'a_repo',
                   'params': 'some_params',
                   'project': 'national-rail-247416',
                   'dataset': 'sql_runner_pilot',
                   'params_import_path': 'a_repo.sql.some_params',
                   'sql_file_path': 'a_repo/sql/'}
        expected_combined_settings = {'repo': 'a_repo',
                                      'params': 'some_params',
                                      'params_import_path': 'a_repo.sql.some_params',
                                      'project': 'national-rail-247416',
                                      'sql_file_path': 'a_repo/sql/',
                                      'DESCRIPTION': 'Table to create a unique joining column',
                                      'validation_table': 'national-rail-247416.sql_runner_pilot.validation_tests',
                                      'OVERWRITE': 'False',
                                      'PARTITION': 'True',
                                      'PARTITION_BY': 'DATE',
                                      'dataset': 'sql_runner_pilot'}
        expected_request = {'repo': 'a_repo',
                            'params': 'some_params',
                            'project': 'national-rail-247416',
                            'params_import_path': 'a_repo.sql.some_params',
                            'dataset': 'sql_runner_pilot',
                            'sql_file_path': 'a_repo/sql/'}
        expected_job_settings = {'DESCRIPTION': 'Table to create a unique joining column',
                                 'OVERWRITE': 'False',
                                 'PARTITION': 'True',
                                 'PARTITION_BY': 'DATE'}
        actual_combined_settings = combine_request_and_job_settings(request, job_settings)
        self.assertEqual(expected_request, request)
        self.assertEqual(expected_job_settings, job_settings)
        self.assertEqual(expected_combined_settings, actual_combined_settings)
