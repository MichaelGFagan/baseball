import pandas
import pandas_gbq
import pybaseball_test.pybaseball.lahman
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    '../bigquery_credentials.json'
)
project_id = 'baseball-source'
tables = [
    'parks',
    'all_star_full',
    'appearances',
    'awards_managers',
    'awards_players',
    'awards_share_managers',
    'awards_share_players',
    'batting',
    'batting_post',
    'college_playing',
    'fielding',
    'fielding_of',
    'fielding_of_split',
    'fielding_post',
    'hall_of_fame',
    'home_games',
    'managers',
    'managers_half',
    'people',
    'pitching',
    'pitching_post',
    'salaries',
    'schools',
    'series_post',
    'teams_core',
    'teams_upstream',
    'teams_franchises',
    'teams_half'
]

for table in tables:
    table_df = getattr(pybaseball_test.pybaseball.lahman, table)()
    cleaned_columns = []
    for column in table_df.columns:
        if column[0].isnumeric():
            column = '_' + column
        column = column.replace('.', '_')
        cleaned_columns.append(column)
    table_df.columns = cleaned_columns
    table_id = 'lahman.' + table
    pandas_gbq.to_gbq(table_df, table_id, project_id=project_id, if_exists='replace')
