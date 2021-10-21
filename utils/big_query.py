import time
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import (
    Client,
    QueryJobConfig,
    Table,
    TimePartitioning
)
from schema.validation_table import schema as validation_schema


def insert_into_table(request, params, row):
    client = Client(request['project'])
    table_ref = f'{request["project"]}.{params["dataset"]}.validation_tests'
    table = client.get_table(table_ref)
    if table:
        try:
            client.insert_rows(
                table=table,
                rows=row
            )
            print(f'Inserted {row} into {table.table_id}')
        except NotFound:
            print(f'{table.table_id} *apparently* not found. Waiting 5 seconds then trying again')
            time.sleep(5)
            insert_into_table(request, params, row)


def create_validation_output_table(request, params):
    client = Client(request['project'])
    table_ref = f'{request["project"]}.{params["dataset"]}.validation_tests'
    if not table_exists(table_ref, client):
        table = Table(
            table_ref=table_ref,
            schema=validation_schema
        )
        client.create_table(table=table)
        print(
            "Created validation table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )


def run_validation_script(query_data):
    job_settings = query_data['job_settings']
    client = Client(job_settings['project'])
    try:
        result = client.query(query=query_data['sql']).result()
        for row in result:
            return row.get('test_result')
    except NotFound:
        return False


def run_script(query_data):
    job_settings = query_data['job_settings']
    client = Client(job_settings['project'])
    if not table_exists(job_settings['table_name'], client):
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
                  table_ref=job_settings['table_name'])
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
    jc.destination = job_settings['table_name']
    jc.create_disposition = job_settings.get('CREATE_DISPOSITION', 'CREATE_IF_NEEDED')
    jc.write_disposition = job_settings.get('WRITE_DISPOSITION', 'WRITE_APPEND')
    jc.default_dataset = job_settings.get('DEFAULT_DATASET', None)
    jc.priority = job_settings.get('PRIORITY', 'INTERACTIVE')
    return jc


def table_exists(table_id, client):
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False
