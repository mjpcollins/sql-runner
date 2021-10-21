from unittest import TestCase, mock
from utils.tasks import get_parameters


class TestTasks(TestCase):

    def setUp(self):
        self.request = {'repo': 'example-repo',
                        'params': 'params',
                        'params_import_path': 'example-repo.sql.params',
                        'sql_file_path': 'example-repo/sql/'}

    def test_get_parameters_no_input(self):
        expected_dynamic_import = {'data': 'that_is_imported_dynamically',
                                   'maths': 100}
        actual_dynamic_import = get_parameters(self.request)
        self.assertEqual(expected_dynamic_import, actual_dynamic_import)
