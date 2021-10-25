import re
from utils.big_query import run_script
from utils.base_sql_task import BaseSQLTask
from utils.validation_task import ValidationTask

table_regex = re.compile(r'[FfJj][RrOo][OoIi][MmNn] `(.*\..*\..*)`')


class SQLTask(BaseSQLTask):

    def __init__(self, filename, params, request):
        super().__init__(filename, params, request)
        self.output_table_name = None
        self._validation_tasks = []
        self._get_output_table_name()
        self._get_validation_tasks()

    def run(self):
        for validation_task in self._validation_tasks:
            if not validation_task.run_test():
                run_script(self._query_data)
                break
        test_results = []
        for validation_task in self._validation_tasks:
            test_results.append(validation_task.run_test())
            validation_task.write_test_result_to_validation_table()
        if False in test_results:
            validation_table = f'{self._request["project"]}.{self._params["dataset"]}.validation_tests'
            raise RuntimeError(f'Validation tests fail! Please check {validation_table} for more information. '
                               f'Request that caused the failure: {self._request}')

    def get_dependencies(self):
        tables = table_regex.findall(self._query_data['sql'])
        dependencies = {
            'output_table': self.output_table_name,
            'source_tables': set(tables)
        }
        return dependencies

    def _get_output_table_name(self):
        file = self._filename.split('/')[-1]
        table = file.split('.')[0]
        dataset = self._params['dataset']
        project = self._request['project']
        self.output_table_name = f'{project}.{dataset}.{table}'
        self._query_data['job_settings']['table_name'] = self.output_table_name
        return self.output_table_name

    def _get_validation_tasks(self):
        js = self._query_data['job_settings'].get('VALIDATION', None)
        if js:
            for validation_name in js.split(','):
                validation_file_path = self._request['validation_files'][validation_name]
                validation_task = ValidationTask(filename=validation_file_path,
                                                 params=self._params,
                                                 request=self._request)
                self._validation_tasks.append(validation_task)
