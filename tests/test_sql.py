from google.cloud.bigquery import (
    QueryJobConfig,
    SchemaField,
    Table,
    TimePartitioning
)
from unittest import TestCase, mock
from utils.sql import (
    load_query_with_params,
    extract_sql_config,
    full_output_table,
    configure_query_settings,
    extract_schema,
    extract_schema_settings_from_comment,
    configure_table,
    get_table_clustering_fields
)
from tests.misc import expected_loaded_query_string


class TestSQL(TestCase):

    @mock.patch("builtins.open", new_callable=mock.mock_open, read_data=expected_loaded_query_string)
    def test_load_query_with_params(self, _):
        input_file_path = 'example-repo/sql/1/100_autotrader_filter.sql'
        input_params = {'dataset': 'sql_runner_pilot',
                        'sql_variables': {'autotrader_source': '`national-rail-247416.data_scrape.100_autotrader`',
                                          '100_autotrader_filter': '`national-rail-247416.sql_runner_pilot.100_autotrader_filter`',
                                          'partition_date': '2021-10-12'}}
        expected_job_settings = {'DESCRIPTION': 'Table to create a unique joining column',
                                 'OVERWRITE': 'False',
                                 'PARTITION': 'True',
                                 'PARTITION_BY': 'DATE',
                                 'DATASET': 'sql_runner_pilot'}
        expected_query_string = "\n-- DESCRIPTION: Table to create a unique joining column" \
                                "\n-- OVERWRITE: False" \
                                "\n-- PARTITION: True" \
                                "\n-- PARTITION_BY: DATE" \
                                "\n\nSELECT\n" \
                                "    registration,\n" \
                                "    specification_make,\n" \
                                "    specification_model,\n" \
                                "    dealer_location_postcode,\n" \
                                "    CONCAT(registration, '-',\n" \
                                "           specification_make, '-',\n" \
                                "           specification_model, '-',\n" \
                                "           dealer_location_postcode) AS joining_column\n" \
                                "FROM `national-rail-247416.sql_runner_pilot.100_autotrader_filter`\n" \
                                "WHERE DATE(_PARTITIONTIME) = \"2021-10-12\"\n"
        expected_loaded_query = {'schema': [],
                                 'job_settings': expected_job_settings,
                                 'sql': expected_query_string}
        actual_loaded_query = load_query_with_params(input_file_path,
                                                     input_params)
        self.assertEqual(expected_loaded_query, actual_loaded_query)

    @mock.patch("builtins.open", new_callable=mock.mock_open, read_data=expected_loaded_query_string)
    def test_extract_sql_config(self, _):
        input_file_lines = ["-- DESCRIPTION:  Table to create a unique joining column",
                            "-- OVERWRITE : False ",
                            "--  PARTITION: True",
                            "  -- PARTITION_BY: DATE",
                            "",
                            "SELECT",
                            "    registration,",
                            "    specification_make,",
                            "    specification_model,",
                            "    dealer_location_postcode,",
                            "    CONCAT(registration, '-',",
                            "           specification_make, '-',",
                            "           specification_model, '-',",
                            "           dealer_location_postcode) AS joining_column",
                            "-- Comment about the weather which isn't important right now",
                            "FROM `national-rail-247416.sql_runner_pilot.100_autotrader_filter`",
                            "WHERE DATE(_PARTITIONTIME) = '2021-10-12'"]
        expected_file_config = {'DESCRIPTION': 'Table to create a unique joining column',
                                'OVERWRITE': 'False',
                                'PARTITION': 'True',
                                'PARTITION_BY': 'DATE'}
        actual_file_config, _ = extract_sql_config(input_file_lines)
        self.assertEqual(expected_file_config, actual_file_config)

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

    def test_extract_schema(self):
        input_file_lines = ["-- DESCRIPTION:  Table to create a unique joining column",
                            "-- OVERWRITE : False ",
                            "--  PARTITION: True",
                            "  -- PARTITION_BY: DATE",
                            "-- SCHEMA:",
                            "--  name=registration, field_type=STRING, description=Field to describe the registration of the car. E.g. \"LT15 HFG\"",
                            "--  name=specification_int, field_type=INT64",
                            "--  name=specification_model, field_type=STRING",
                            "--  name=dealer_location_postcode, field_type=STRING",
                            "--  name=joining_column, field_type=STRING",
                            "-- END_SCHEMA:",
                            "-- Comment about the weather which isn't important right now"]
        expected_schema = [SchemaField(name='registration',
                                       field_type='STRING',
                                       mode='NULLABLE',
                                       description='Field to describe the registration of the car. E.g. "LT15 HFG"'),
                           SchemaField(name='specification_int',
                                       field_type='INT64',
                                       mode='NULLABLE'),
                           SchemaField(name='specification_model',
                                       field_type='STRING',
                                       mode='NULLABLE'),
                           SchemaField(name='dealer_location_postcode',
                                       field_type='STRING',
                                       mode='NULLABLE'),
                           SchemaField(name='joining_column',
                                       field_type='STRING',
                                       mode='NULLABLE')]
        actual_schema = extract_schema(input_file_lines)
        for idx, exp_schema in enumerate(expected_schema):
            self.assertEqual(exp_schema.to_api_repr(),
                             actual_schema[idx].to_api_repr())

    def test_extract_schema_settings_from_comment(self):
        input_comment = '--  name=registration, field_type=STRING, description=Field to describe the registration of the car. E.g. "LT15 HFG"'
        expected_schema = SchemaField(name='registration',
                                      field_type='STRING',
                                      mode='NULLABLE',
                                      description='Field to describe the registration of the car. E.g. "LT15 HFG"')
        actual_schema = extract_schema_settings_from_comment(input_comment)
        self.assertEqual(expected_schema, actual_schema)

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
        expected_clusters = []
        actual_clusters = get_table_clustering_fields(input_settings)
        self.assertEqual(expected_clusters, actual_clusters)