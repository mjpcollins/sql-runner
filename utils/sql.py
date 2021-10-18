import re
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import (
    Client,
    QueryJobConfig,
    SchemaField,
    Table,
    TimePartitioning
)

config_key_regex = re.compile(r'([A-Za-z_]+) *:')
config_value_regex = re.compile(r': *([A-Za-z_ ]+)')
schema_config_key_regex = re.compile(r'([A-Za-z_]+) *=')
schema_config_value_regex = re.compile(r'= *([0-9A-Za-z_ \.\'\"]+)')


def load_query_with_params(script_path, params):
    with open(script_path, 'r') as F:
        f = F.readlines()
    job_settings, schema = extract_sql_config(f)
    formatted_script = ''.join(f).format(**params['sql_variables'])
    job_settings['DATASET'] = params['dataset']
    loaded_query = {'sql': formatted_script,
                    'job_settings': job_settings,
                    'schema': schema}
    return loaded_query


def extract_sql_config(file_lines):
    comments = [line.strip() for line in file_lines
                if line.strip()[:2] == '--']
    schema = extract_schema(comments)
    config = extract_settings(comments)
    return config, schema


def extract_settings(comments):
    config = {}
    for c in comments:
        k = config_key_regex.findall(c)
        v = config_value_regex.findall(c)
        if k and v:
            config[k[0]] = v[0]
    return config


def extract_schema(comments):
    in_schema_params = False
    schema = []
    for c in comments:

        k = config_key_regex.findall(c)
        if k:
            if k[0] == 'SCHEMA':
                in_schema_params = True
                continue
            elif k[0] == 'END_SCHEMA':
                in_schema_params = False

        if in_schema_params:
            schema.append(extract_schema_settings_from_comment(c))

    return schema


def extract_schema_settings_from_comment(comment):
    configs = [c.strip() for c in comment.split(',')]
    config_dict = {}
    for c in configs:
        k = schema_config_key_regex.findall(c)[0]
        v = schema_config_value_regex.findall(c)[0]
        config_dict[k] = v
    return SchemaField(**config_dict)


def run_script(query_data):
    job_settings = query_data['job_settings']
    client = Client(job_settings['project'])
    if not table_exists(job_settings):
        table = configure_table(query_data)
        client.create_table(table)
    job_config = configure_query_settings(job_settings)
    query_job = client.query(query=query_data['sql'],
                             job_config=job_config)
    print("Started job: {}".format(query_job.job_id))
    query_job.result()
    print("Job done: {}".format(query_job.job_id))


def configure_table(query_data):
    job_settings = query_data['job_settings']
    table = Table(schema=query_data['schema'],
                  table_ref=full_output_table(job_settings))
    table.description = job_settings.get('DESCRIPTION', None)
    table.clustering_fields = get_table_clustering_fields(job_settings)
    time_partition = get_time_partition(job_settings)
    if time_partition:
        table.time_partitioning = time_partition
    return table


def get_table_clustering_fields(job_settings):
    clusters = job_settings.get("CLUSTERING_FIELDS", None)
    if clusters:
        return [c.strip()
                for c in clusters.split(',')
                if c.strip()]


def get_time_partition(job_settings):
    if job_settings.get('PARTITION', 'False').upper() == 'TRUE':
        partition_type = job_settings.get('PARTITION_TYPE', 'DAY')
        partition_field = job_settings.get('PARTITION_BY', '_PARTITIONTIME')
        return TimePartitioning(type_=partition_type,
                                field=partition_field)


def configure_query_settings(job_settings):
    jc = QueryJobConfig()
    jc.destination = full_output_table(job_settings)
    jc.create_disposition = job_settings.get('CREATE_DISPOSITION', 'CREATE_IF_NEEDED')
    jc.write_disposition = job_settings.get('WRITE_DISPOSITION', 'WRITE_APPEND')
    jc.default_dataset = job_settings.get('DEFAULT_DATASET', None)
    jc.priority = job_settings.get('PRIORITY', 'INTERACTIVE')
    return jc


def full_output_table(job_settings):
    p = job_settings['project']
    d = job_settings['dataset']
    t = job_settings['table_name']
    return f'{p}.{d}.{t}'


def table_exists(job_settings):
    client = Client(job_settings['project'])
    table_id = full_output_table(job_settings)
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False
