from utils.sql_config import extract_sql_config
from utils.format import combine_request_and_job_settings


class BaseSQLTask:

    def __init__(self, filename, params, request):
        self._filename = filename
        self._params = params
        self._request = request
        self._query_data = {}
        self._load_query()

    def _load_query(self):
        with open(self._filename, 'r') as F:
            f = F.readlines()
        job_settings, schema = extract_sql_config(f)
        formatted_script = ''.join(f).format(**self._params['sql_variables'])
        job_settings['dataset'] = self._params['dataset']
        full_job_settings = combine_request_and_job_settings(request=self._request,
                                                             job_settings=job_settings)
        self._query_data = {
            'sql': formatted_script,
            'job_settings': full_job_settings,
            'schema': schema
        }
