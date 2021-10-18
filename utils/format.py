

def enrich_request(request):
    repo = request['repo']
    sql_folder = request.get('sql', 'sql')
    params_file = request.get('params', 'params')
    request['params_import_path'] = f'{repo}.{sql_folder}.{params_file}'
    request['sql_file_path'] = f'{repo}/{sql_folder}/'


def combine_request_and_job_settings(request, job_settings):
    new_dict = request.copy()
    new_dict.update(job_settings)
    return new_dict


def get_output_table_name(script_path):
    file_name = script_path.split('/')[-1]
    table_name = file_name.split('.')[0]
    return table_name
