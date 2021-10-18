from utils.fetch import (
    download_to_local,
    delete_local_repo
)
from utils.tasks import (
    get_scripts_to_run,
    get_parameters
)
from utils.format import (
    enrich_request,
    combine_request_and_job_settings,
    get_output_table_name
)
from utils.sql import (
    load_query_with_params,
    run_script
)


def run_process(request):
    download_to_local(request)
    enrich_request(request)
    all_tasks = get_scripts_to_run(request)
    params = get_parameters(request)

    for folder in all_tasks:
        for script_path in folder:
            query_data = load_query_with_params(script_path=script_path,
                                                params=params)
            full_job_settings = combine_request_and_job_settings(request=request,
                                                                 job_settings=query_data['job_settings'])
            full_job_settings['table_name'] = get_output_table_name(script_path)
            query_data['job_settings'] = full_job_settings
            run_script(query_data)

    delete_local_repo(request)

    return {'status': 'OK'}, 200
