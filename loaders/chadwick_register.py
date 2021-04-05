import pandas as pd
import pandas_gbq
from google.oauth2 import service_account


url = 'https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv'
df = pd.read_csv(url)


credentials = service_account.Credentials.from_service_account_file(
    '../bigquery_credentials.json'
)
project_id = 'baseball-source'
table_id = 'chadwick.register'
pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace')



# import datetime
# import os
# import requests
# from google.cloud import bigquery as bq
#
# print("Downloading Chadwick register...")
#
# url = "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv"
# data = requests.get(url)
#
# filename = "/Users/michaelfagan-cc/baseball/data/chadwick/register.csv"
# os.makedirs(os.path.dirname(filename), exist_ok=True)
# with open(filename, "wb") as f:
#     f.write(data.content)
#
# client = bq.Client.from_service_account_json("../../bigquery_credentials.json", project="baseball-source")
#
# project_id = "baseball-source"
# dataset_id = "chadwick"
# dataset_ref = client.dataset(dataset_id)
#
# job_config = bq.LoadJobConfig()
# job_config.source_format = bq.SourceFormat.CSV
# job_config.skip_leading_rows = 1
# job_config.autodetect = True
#
# table_name = "register"
#
# table_ref = dataset_ref.table(table_name)
# with open(filename, "rb") as source_file:
#     job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
#
# import pandas
# import pandas_gbq
# from google.oauth2 import service_account
# from pybaseball import chadwick_register
#
# credentials = service_account.Credentials.from_service_account_file(
#     '../bigquery_credentials.json'
# )
# project_id = 'baseball-source'
# table_id = 'chadwick.register'
#
# register = chadwick_register()
#
# pandas_gbq.to_gbq(register, table_id, project_id=project_id, if_exists='replace')
