from google.cloud.bigquery import (
    QueryJobConfig,
    SchemaField,
    Table,
    TimePartitioning
)
from unittest import TestCase, mock
from utils.big_query import (
    full_output_table,
    configure_query_settings,
    configure_table,
    get_table_clustering_fields
)
from tests.misc import expected_loaded_query_string


class TestSQL(TestCase):

    def test_configure_query_settings(self):
        job_settings = {'repo': 'a_repo',
                        'params': 'some_params',
                        'params_import_path': 'a_repo.sql.some_params',
                        'sql_file_path': 'a_repo/sql/',
                        'DESCRIPTION': 'Table to create a unique joining column',
                        'OVERWRITE': 'False',
                        'PARTITION': 'True',
                        'PARTITION_BY': 'DATE',
                        'table_name': '300_apples',
                        'project': 'example_project',
                        'dataset': 'sql_runner_pilot'}
        conf = {'clustering_fields': None,
                'create_disposition': 'CREATE_IF_NEEDED',
                'write_disposition': 'WRITE_APPEND',
                'destination': 'example_project.sql_runner_pilot.300_apples',
                'default_dataset': None,
                'priority': 'INTERACTIVE'}
        expected_api_repr = QueryJobConfig(**conf).to_api_repr()
        actual_api_repr = configure_query_settings(job_settings).to_api_repr()
        self.assertEqual(expected_api_repr, actual_api_repr)

    def test_full_output_table(self):
        combined_settings = {'repo': 'a_repo',
                             'params': 'some_params',
                             'params_import_path': 'a_repo.sql.some_params',
                             'sql_file_path': 'a_repo/sql/',
                             'DESCRIPTION': 'Table to create a unique joining column',
                             'OVERWRITE': 'False',
                             'PARTITION': 'True',
                             'PARTITION_BY': 'DATE',
                             'table_name': '300_apples',
                             'project': 'example_project',
                             'dataset': 'sql_runner_pilot'}
        expected_table_name = 'example_project.sql_runner_pilot.300_apples'
        actual_table_name = full_output_table(combined_settings)
        self.assertEqual(expected_table_name, actual_table_name)

    def test_configure_load_job_settings(self):
        query_data = {'job_settings': {'repo': 'a_repo',
                                       'params': 'some_params',
                                       'params_import_path': 'a_repo.sql.some_params',
                                       'sql_file_path': 'a_repo/sql/',
                                       'DESCRIPTION': 'Table to create a unique joining column',
                                       'OVERWRITE': 'False',
                                       'PARTITION': 'True',
                                       'CLUSTERING_FIELDS': 'date_of_run, specification_make',
                                       'PARTITION_BY': 'date_of_run',
                                       'table_name': '300_apples',
                                       'project': 'example_project',
                                       'dataset': 'sql_runner_pilot'},
                      'schema': [SchemaField(name='registration',
                                             field_type='STRING',
                                             mode='NULLABLE',
                                             description='Field to describe the registration of the car. E.g. "LT15 HFG"'),
                                 SchemaField(name='specification_int',
                                             field_type='INT64',
                                             mode='NULLABLE')]}
        schema = [SchemaField(name='registration',
                              field_type='STRING',
                              mode='NULLABLE',
                              description='Field to describe the registration of the car. E.g. "LT15 HFG"'),
                  SchemaField(name='specification_int',
                              field_type='INT64',
                              mode='NULLABLE')]
        expected_table = Table(table_ref='example_project.sql_runner_pilot.300_apples')
        expected_table.description = 'Table to create a unique joining column'
        expected_table.schema = schema
        expected_table.time_partitioning = TimePartitioning(field='date_of_run')
        expected_table.clustering_fields = ['date_of_run', 'specification_make']
        actual_table = configure_table(query_data)
        self.assertEqual(expected_table.to_api_repr(),
                         actual_table.to_api_repr())

    def test_get_table_clustering_fields(self):
        input_settings = {}
        expected_clusters = None
        actual_clusters = get_table_clustering_fields(input_settings)
        self.assertEqual(expected_clusters, actual_clusters)