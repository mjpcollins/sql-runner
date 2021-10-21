import datetime
from utils.base_sql_task import BaseSQLTask
from utils.big_query import (
    run_validation_script,
    insert_into_table
)


class ValidationTask(BaseSQLTask):

    def __init__(self, filename, params, request):
        super().__init__(filename, params, request)
        self.latest_test_result = None

    def run_test(self):
        self.latest_test_result = run_validation_script(self._query_data)
        return self.latest_test_result

    def write_test_result_to_validation_table(self):
        file = self._filename.split('/')[-1]
        test_name = file.split('.')[0]
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row = [
            {
                'repository': self._request['repo'],
                'test_name': test_name,
                'description': self._query_data['job_settings'].get('TEST_DESC', ''),
                'test_file_path': self._filename,
                'result': self.latest_test_result,
                'write_time': now
            }
        ]
        insert_into_table(request=self._request,
                          params=self._params,
                          row=row)
