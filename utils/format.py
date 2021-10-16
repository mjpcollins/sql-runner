

def enrich_request(request):
    repo = request['repo']
    sql_folder = request.get('sql', 'sql')
    params_file = request.get('params', 'params')
    request['params_import_path'] = f'{repo}.{sql_folder}.{params_file}'
    request['sql_file_path'] = f'{repo}/{sql_folder}/'
