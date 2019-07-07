import argparse
import pandas as pd
from pybaseball import retrosheet as rs
from google.cloud import bigquery as bq

parser = argparse.ArgumentParser(description="Load Retrosheet data into Google BigQuery")
parser.add_argument('-s', '--start', type=int, default=1871, help="first year to extract from Retrosheet")
parser.add_argument('-e', '--end', type=int, default=2018, help="last year to extract from Retrosheet")

args = parser.parse_args()

start = args.start
end = args.end + 1

client = bq.Client.from_service_account_json("../bigquery_credentials.json", project="baseball-source")

project_id = "baseball-source"
dataset_id = "retrosheet"
dataset_ref = client.dataset(dataset_id)

years = range(start, end)

for year in years:
    table_name = "retrosheet_" + str(year)
    table_id = project_id + "." + dataset_id + "." + table_name
    table = bq.Table(table_id)
    table = client.create_table(table)
    df = rs.season_game_logs(year)
    df.rename(columns={"visiting_2_id.1": "visiting_3_id"}, inplace=True)
    table_ref = dataset_ref.table(table_name)
    client.load_table_from_dataframe(df, table_ref).result()
