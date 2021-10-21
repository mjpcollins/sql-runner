from unittest import TestCase, mock
from utils.sql_task import SQLTask
from tests.misc import expected_loaded_query_string


class TestDependencyGraph(TestCase):

    @mock.patch("builtins.open", new_callable=mock.mock_open, read_data=expected_loaded_query_string)
    def setUp(self, _):
        request = {'repo': 'a_repo',
                   'project': 'national-rail-247416',
                   'params': 'some_params'}
        filename = 'example-repo/sql/1/100_autotrader_filter.sql'
        params = {'dataset': 'sql_runner_pilot',
                  'sql_variables': {'autotrader_source': '`national-rail-247416.data_scrape.100_autotrader`',
                                    '100_autotrader_filter': '`national-rail-247416.sql_runner_pilot.100_autotrader_filter`',
                                    'partition_date': '2021-10-12'}}
        self.sql_task = SQLTask(filename=filename,
                                params=params,
                                request=request)
        self.maxDiff = None

    def test_load_query_with_params(self):
        expected_job_settings = {'DESCRIPTION': 'Table to create a unique joining column',
                                 'OVERWRITE': 'False',
                                 'PARTITION': 'True',
                                 'PARTITION_BY': 'DATE',
                                 'DATASET': 'sql_runner_pilot',
                                 'params': 'some_params',
                                 'project': 'national-rail-247416',
                                 'repo': 'a_repo',
                                 'table_name': 'national-rail-247416.sql_runner_pilot.100_autotrader_filter'}
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
                                "FROM `national-rail-247416.data_scrape.100_autotrader`\n" \
                                "WHERE DATE(_PARTITIONTIME) = \"2021-10-12\"\n"
        expected_loaded_query = {'schema': [],
                                 'job_settings': expected_job_settings,
                                 'sql': expected_query_string}
        self.assertEqual(expected_loaded_query, self.sql_task._query_data)

    def test_get_output_table_name(self):
        expected_table_name = 'national-rail-247416.sql_runner_pilot.100_autotrader_filter'
        actual_table_name = self.sql_task._get_output_table_name()
        self.assertEqual(expected_table_name, actual_table_name)
        self.assertEqual(self.sql_task.output_table_name, actual_table_name)

    def test_get_dependencies(self):
        expected_dependencies = {'output_table': 'national-rail-247416.sql_runner_pilot.100_autotrader_filter',
                                 'source_tables': {'national-rail-247416.data_scrape.100_autotrader'}}
        actual_dependencies = self.sql_task.get_dependencies()
        self.assertEqual(expected_dependencies, actual_dependencies)
