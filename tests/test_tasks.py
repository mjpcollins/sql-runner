from unittest import TestCase, mock
from utils.tasks import (
    get_scripts_to_run,
    get_folder_structure,
    get_parameters
)


class TestTasks(TestCase):

    def setUp(self):
        self.request = {'repo': 'example-repo',
                        'params': 'params',
                        'params_import_path': 'example-repo.sql.params',
                        'sql_file_path': 'example-repo/sql/'}

    @mock.patch('glob.glob', return_value=['example-repo/sql/', 'example-repo/sql/0', 'example-repo/sql/1',
                                           'example-repo/sql/1/101_other_raw_data.sql',
                                           'example-repo/sql/0/100_raw_data.sql', 'example-repo/sql/3',
                                           'example-repo/sql/3/300_joined_data.sql', 'example-repo/sql/2',
                                           'example-repo/sql/2/200_clean_data.sql',
                                           'example-repo/sql/2/201_other_clean.sql',
                                           'example-repo/sql/params.py'])
    def test_scan_file_structure(self, mock_glob):
        expected_file_run_order = [['example-repo/sql/0/100_raw_data.sql'],
                                   ['example-repo/sql/1/101_other_raw_data.sql'],
                                   ['example-repo/sql/2/200_clean_data.sql', 'example-repo/sql/2/201_other_clean.sql'],
                                   ['example-repo/sql/3/300_joined_data.sql']]
        actual_file_run_order = get_scripts_to_run(self.request)
        self.assertEqual(expected_file_run_order, actual_file_run_order)

    def test_get_parameters_no_input(self):
        expected_dynamic_import = {'data': 'that_is_imported_dynamically',
                                   'maths': 100}
        actual_dynamic_import = get_parameters(self.request)
        self.assertEqual(expected_dynamic_import, actual_dynamic_import)

    def test_get_folder_structure_dict(self):
        root_folder = 'example-repo/sql/'
        all_files = ['example-repo/sql/0', 'example-repo/sql/1', 'example-repo/sql/',
                     'example-repo/sql/1/101_other_raw_data.sql',
                     'example-repo/sql/0/100_raw_data.sql', 'example-repo/sql/3',
                     'example-repo/sql/3/300_joined_data.sql', 'example-repo/sql/2',
                     'example-repo/sql/2/200_clean_data.sql',
                     'example-repo/sql/2/201_other_clean.sql',
                     'example-repo/sql/params.py']
        expected_all_files = ['example-repo/sql/0', 'example-repo/sql/1', 'example-repo/sql/',
                              'example-repo/sql/1/101_other_raw_data.sql',
                              'example-repo/sql/0/100_raw_data.sql', 'example-repo/sql/3',
                              'example-repo/sql/3/300_joined_data.sql', 'example-repo/sql/2',
                              'example-repo/sql/2/200_clean_data.sql',
                              'example-repo/sql/2/201_other_clean.sql',
                              'example-repo/sql/params.py']
        expected_file_list = [['example-repo/sql/0/100_raw_data.sql'],
                              ['example-repo/sql/1/101_other_raw_data.sql'],
                              ['example-repo/sql/2/200_clean_data.sql', 'example-repo/sql/2/201_other_clean.sql'],
                              ['example-repo/sql/3/300_joined_data.sql']]
        actual_file_dict = get_folder_structure(all_files, root_folder)
        self.assertEqual(expected_file_list, actual_file_dict)
        # Check that the all_files list isn't altered.
        self.assertEqual(expected_all_files, all_files)
