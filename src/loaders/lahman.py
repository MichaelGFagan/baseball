import re
import os
from google.cloud import bigquery as bq

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

client = bq.Client.from_service_account_json("../bigquery_credentials.json", project="baseball-source")

project_id = "baseball-source"
dataset_id = "lahman"
dataset_ref = client.dataset(dataset_id)

job_config = bq.LoadJobConfig()
job_config.source_format = bq.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = True

file_path = "../../data/lahman/"

for file in os.listdir(file_path):
    if file.endswith(".csv"):
        table_name = camel_to_snake(file[:-4])
        table_ref = dataset_ref.table(table_name)
        file = file_path + file
        with open(file, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
