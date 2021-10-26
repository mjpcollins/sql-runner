import os
import shutil


def download_to_local(request):
    repo = request['repo']
    project = request['project']
    command_text = f'gcloud source repos clone {repo} --project={project}'
    os.system(command_text)


def delete_local_repo(request):
    repo = request['repo']
    try:
        shutil.rmtree(repo)
    except FileNotFoundError:
        pass
