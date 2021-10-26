from utils.fetch import (
    download_to_local,
    delete_local_repo
)
from utils.tasks import get_tasks


def run_process(request):
    delete_local_repo(request)
    download_to_local(request)
    all_tasks = get_tasks(request)
    for task_name in all_tasks['task_order']:
        all_tasks['tasks'][task_name].run()
    delete_local_repo(request)
    return {'status': 'OK'}, 200


if __name__ == '__main__':
    req = {'repo': 'example-sql-for-sql-runner',
           'project': 'national-rail-247416'}
    run_process(req)
