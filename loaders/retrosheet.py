import argparse
import pandas
import pandas_gbq
from pybaseball import retrosheet as rs
from google.oauth2 import service_account

parser = argparse.ArgumentParser(description="Load Retrosheet data into Google BigQuery")
parser.add_argument('-s', '--start', type=int, default=1871, help="first year to extract from Retrosheet")
parser.add_argument('-e', '--end', type=int, default=2020, help="last year to extract from Retrosheet")

args = parser.parse_args()

start = args.start
end = args.end + 1

credentials = service_account.Credentials.from_service_account_file(
    '../bigquery_credentials.json'
)
project_id = 'baseball-source'

years = range(start, end)

for year in years:
    table_name = "retrosheet_" + str(year)
    table_id = "retrosheet." + table_name
    try:
        df = rs.season_game_logs(year)
        df.rename(columns={"visiting_2_id.1": "visiting_3_id"}, inplace=True)
        pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace')
    except:
        pass
