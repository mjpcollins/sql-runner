

def download_to_local(request):
    repo = request['repo']
    project = request['project']
    # TODO MC 2021-10-15 look up full command text
    command_text = 'gcloud clone {repo} --project={project}'


