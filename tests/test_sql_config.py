from google.cloud.bigquery import SchemaField
from unittest import TestCase, mock
from utils.sql_config import (
    extract_sql_config,
    extract_schema,
    extract_schema_settings_from_comment,
)
from tests.misc import expected_loaded_query_string


class TestSQLConfig(TestCase):

    @mock.patch("builtins.open", new_callable=mock.mock_open, read_data=expected_loaded_query_string)
    def test_extract_sql_config(self, _):
        input_file_lines = ["-- DESCRiPTION:  Table to create a unique joining column",
                            "-- OVERWRITE : False ",
                            "--  PARTITION: True",
                            "  -- PARTItION BY: DATE",
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

    def test_extract_schema(self):
        input_file_lines = ["-- DESCRIPTION:  Table to create a unique joining column",
                            "-- OVERWRITE : False ",
                            "--  PARTITION: True",
                            "  -- PARTITION BY: DATE",
                            "-- SCHEMA:",
                            "--  name=registration, field_type=STRING, description=Field to describe the registration of the car. E.g. \"LT15 HFG\"",
                            "--  name=specification_int, field_type=INT64",
                            "--  name=specification_model, field_type=STRING",
                            "--  name=dealer_location_postcode, field_type=STRING",
                            "--  name=joining_column, field_type=STRING",
                            "-- END SCHEMA:",
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
        self.assertEqual(len(expected_schema), len(actual_schema))
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
