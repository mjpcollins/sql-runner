import os


def download_to_local(request):
    repo = request['repo']
    project = request['project']
    command_text = f'gcloud source repos clone {repo} --project={project}'
    os.system(command_text)

