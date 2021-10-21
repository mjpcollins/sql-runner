from google.cloud.bigquery import SchemaField


schema = [
    SchemaField(name='repository',
                field_type='STRING',
                description='Repository of SQL that was run'),
    SchemaField(name='test_name',
                field_type='STRING',
                description='Summary name of the test'),
    SchemaField(name='description',
                field_type='STRING',
                description='Full description of the test (if given in the validation SQL file)'),
    SchemaField(name='test_file_path',
                field_type='STRING',
                description='File path location of the test SQL within the repository'),
    SchemaField(name='result',
                field_type='BOOL',
                description='1 for test passed, 0 for test failed'),
    SchemaField(name='write_time',
                field_type='STRING',
                description='Time that this row of data was written to BigQuery')
]

