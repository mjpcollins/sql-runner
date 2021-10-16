from unittest import TestCase
from utils.format import (
    enrich_request
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
