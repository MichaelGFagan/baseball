import argparse
import datetime
import pandas as pd
import pandas_gbq
from pybaseball_test import statcast
from google.oauth2 import service_account

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

credentials = service_account.Credentials.from_service_account_file(
    '../bigquery_credentials.json'
)
project_id = "baseball-source"

years = range(start_year, end_year + 1)
month_end_dates = {3: 31, 4: 30, 5: 31, 6: 30, 7:31, 8: 31, 9: 30, 10: 31, 11: 30}

def statcast_data(year, end_month_range):
    df_dict = {}
    for i in range(3, end_month_range):
        start_date = datetime.date(year, i, 1).strftime("%Y-%m-%d")
        if year == today.year and i == today.month:
            end_date = today.strftime("%Y-%m-%d")
        else:
            end_date = datetime.date(year, i, month_end_dates[i]).strftime("%Y-%m-%d")
        try:
            df = statcast(start_date, end_date)
        except:
            pass
        df_dict[i] = df
    return df_dict

def dataframe_collate(df_dict):
    data = pd.DataFrame()
    for df in list(df_dict.keys()):
        if len(df_dict[df].index) > 0:
            data = pd.concat([data, df_dict[df]])
    return data

for year in years:
    if year == today.year:
        # TODO: handle December - February
        month_end_dates[today.month] = today.day

    end_month_range = 12
    if year == today.year:
        end_month_range = today.month + 1

    table_name = "statcast_" + str(year)
    year_df_dict = statcast_data(year, end_month_range)
    df = dataframe_collate(year_df_dict)
    df.rename(columns={"pitcher.1": "pitcher_1", "fielder_2.1": "fielder_2_1"}, inplace=True)
    table_id = "statcast." + table_name
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace')
