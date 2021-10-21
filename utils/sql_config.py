import re
from google.cloud.bigquery import SchemaField


config_key_regex = re.compile(r'([A-Za-z_0-9]+) *:')
config_value_regex = re.compile(r': *([A-Za-z_ 0-9,]+)')
schema_config_key_regex = re.compile(r'([A-Za-z_]+) *=')
schema_config_value_regex = re.compile(r'= *([0-9A-Za-z_ \.\'\"]+)')


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


