

def enrich_request(request):
    repo = request['repo']
    sql_folder = request.get('sql', 'sql')
    tests_folder = request.get('tests', 'sql_tests')
    params_file = request.get('params', 'params')
    request['params_import_path'] = f'{repo}.{sql_folder}.{params_file}'
    request['sql_file_path'] = f'{repo}/{sql_folder}/'
    request['sql_validation_tests_file_path'] = f'{repo}/{tests_folder}/'


def combine_request_and_job_settings(request, job_settings):
    new_dict = request.copy()
    new_dict.update(job_settings)
    new_dict['validation_table'] = get_validation_table_name(new_dict)
    return new_dict


def get_validation_table_name(job_settings):
    p = job_settings['project']
    d = job_settings['dataset']
    return f'{p}.{d}.validation_tests'
