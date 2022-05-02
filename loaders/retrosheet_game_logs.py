import argparse
import pandas
import pandas_gbq
from pybaseball_test import retrosheet as rs
from google.oauth2 import service_account

parser = argparse.ArgumentParser(description="Load Retrosheet game log data into Google BigQuery")
parser.add_argument('-s', '--start', type=int, default=1871, help="first year to extract from Retrosheet")
parser.add_argument('-e', '--end', type=int, default=2020, help="last year to extract from Retrosheet")
parser.add_argument('-p', '--playoffs', action='store_true', help="include playoffs and all-star game logs from Retrosheet")

args = parser.parse_args()

start = args.start
end = args.end + 1

playoff_game_log_functions = {
    'world_series_games': rs.world_series_logs, 
    'all_star_games': rs.all_star_game_logs, 
    'wild_card_games': rs.wild_card_logs, 
    'division_series_games': rs.division_series_logs, 
    'league_championship_games': rs.lcs_logs
}

credentials = service_account.Credentials.from_service_account_file(
    '../bigquery_credentials.json'
)
project_id = 'baseball-source'

years = range(start, end)

def transform_and_load(df):
    df.rename(columns={"visiting_2_id.1": "visiting_3_id"}, inplace=True)
    table_schema = []
    for column in df.columns:
        table_schema.append({'name': column, 'type': 'STRING'})
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace', table_schema=table_schema, chunksize=10000)

for year in years:
    table_name = 'retrosheet_' + str(year)
    table_id = 'retrosheet.' + table_name    
    try:
        df = rs.season_game_logs(year)
        transform_and_load(df)
    except:
        print(f'Could not load Retrosheet {year}.')
        pass

if args.playoffs:
    for games in playoff_game_log_functions:
        table_name = 'retrosheet_' + games
        table_id = 'retrosheet.' + table_name
        try:
            df = playoff_game_log_functions[games]()
            transform_and_load(df)
        except:
            print(f'Could not load Retrosheet {games}.')
            pass
    
    

