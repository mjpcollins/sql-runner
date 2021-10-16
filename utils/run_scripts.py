from utils.tasks import (
    get_scripts_to_run,
    get_parameters
)
from utils.format import enrich_request
from utils.sql import (
    load_query_with_params,
    run_script
)


def run_process(request):
    enrich_request(request)
    all_tasks = get_scripts_to_run(request)
    params = get_parameters(request)

    for folder in all_tasks:
        for script_path in folder:
            query, job_settings = load_query_with_params(script_path=script_path,
                                                         params=params)
            run_script(query=query,
                       job_settings=job_settings)