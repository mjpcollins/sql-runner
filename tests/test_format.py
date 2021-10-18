from unittest import TestCase
from utils.format import (
    enrich_request,
    combine_request_and_job_settings,
    get_output_table_name
)


class TestFormat(TestCase):

    def test_enrich_request(self):
        request = {'repo': 'a_repo',
                   'params': 'some_params'}
        expected_enriched_request = {'repo': 'a_repo',
                                     'params': 'some_params',
                                     'params_import_path': 'a_repo.sql.some_params',
                                     'sql_file_path': 'a_repo/sql/'}
        enrich_request(request)
        self.assertEqual(expected_enriched_request, request)

    def test_combine_request_and_job_settings(self):
        job_settings = {'DESCRIPTION': 'Table to create a unique joining column',
                        'OVERWRITE': 'False',
                        'PARTITION': 'True',
                        'PARTITION_BY': 'DATE',
                        'DATASET': 'sql_runner_pilot'}
        request = {'repo': 'a_repo',
                   'params': 'some_params',
                   'params_import_path': 'a_repo.sql.some_params',
                   'sql_file_path': 'a_repo/sql/'}
        expected_combined_settings = {'repo': 'a_repo',
                                      'params': 'some_params',
                                      'params_import_path': 'a_repo.sql.some_params',
                                      'sql_file_path': 'a_repo/sql/',
                                      'DESCRIPTION': 'Table to create a unique joining column',
                                      'OVERWRITE': 'False',
                                      'PARTITION': 'True',
                                      'PARTITION_BY': 'DATE',
                                      'DATASET': 'sql_runner_pilot'}
        expected_request = {'repo': 'a_repo',
                            'params': 'some_params',
                            'params_import_path': 'a_repo.sql.some_params',
                            'sql_file_path': 'a_repo/sql/'}
        expected_job_settings = {'DESCRIPTION': 'Table to create a unique joining column',
                                 'OVERWRITE': 'False',
                                 'PARTITION': 'True',
                                 'PARTITION_BY': 'DATE',
                                 'DATASET': 'sql_runner_pilot'}
        actual_combined_settings = combine_request_and_job_settings(request, job_settings)
        self.assertEqual(expected_request, request)
        self.assertEqual(expected_job_settings, job_settings)
        self.assertEqual(expected_combined_settings, actual_combined_settings)

    def test_get_output_table_name(self):
        input_file_path = 'example-repo/sql/3/300_joined_data.sql'
        expected_table_name = '300_joined_data'
        actual_table_name = get_output_table_name(input_file_path)
        self.assertEqual(expected_table_name, actual_table_name)
