import argparse
import datetime
import pandas as pd
from pybaseball import statcast
from google.cloud import bigquery as bq

today = datetime.date.today()

parser = argparse.ArgumentParser(description="Load Statcast data into Google BigQuery")
parser.add_argument('-s', '--start', type=int, default=today.year, help="first year to extract from Statcast")
parser.add_argument('-e', '--end', type=int, help="last year to extract from Statcast")

args = parser.parse_args()
start_year = args.start
if args.end is None:
    end_year = args.start
else:
    end_year = args.end

client = bq.Client.from_service_account_json("../bigquery_credentials.json", project="baseball-source")

project_id = "baseball-source"
dataset_id = "statcast"
dataset_ref = client.dataset(dataset_id)

def statcast_data(year, end_month_range):
    df_dict = {}
    for i in range(3, end_month_range):
        start_date = datetime.date(year, i, 1).strftime("%Y-%m-%d")
        if year == today.year and i == today.month:
            end_date = today.strftime("%Y-%m-%d")
        else:
            end_date = datetime.date(year, i, month_end_dates[i]).strftime("%Y-%m-%d")
        df = statcast(start_date, end_date)
        df_dict[i] = df
    return df_dict

def dataframe_collate(df_dict):
    for month in list(df_dict.keys()):
        if month == 3:
            data = df_dict[month]
        else:
            data = pd.concat([data, df_dict[month]])
    return data

month_end_dates = {3: 31, 4: 30, 5: 31, 6: 30, 7:31, 8: 31, 9: 30, 10: 31, 11: 30}

years = range(start_year, end_year + 1)

for year in years:
    if year == today.year:
        # TODO: handle December - February
        month_end_dates[today.month] = today.day

    end_month_range = 12
    if year == today.year:
        end_month_range = today.month + 1

    table_name = "statcast_" + str(year)
    table_id = project_id + "." + dataset_id + "." + table_name
    table = bq.Table(table_id)
    df_dict = statcast_data(year, end_month_range)
    df = dataframe_collate(df_dict)
    df.rename(columns={"pitcher.1": "pitcher_1", "fielder_2.1": "fielder_2_1"}, inplace=True)
    table_ref = dataset_ref.table(table_name)
    client.load_table_from_dataframe(df, table_ref).result()
