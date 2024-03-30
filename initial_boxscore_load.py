from nba_api.stats.endpoints import BoxScoreTraditionalV3
from nba_api.stats.endpoints.teamgamelogs import TeamGameLogs
from sqlalchemy import create_engine
import pandas as pd
import time

df = TeamGameLogs(season_nullable='2023-24').get_data_frames()[0]
df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
df['GAME_DATE'] = df['GAME_DATE'].dt.strftime('%Y-%m-%d')
game_log_list = df['GAME_ID'].drop_duplicates()

accumulated_data = []
count = 1

for game_id in game_log_list:
    box_score_team_data = BoxScoreTraditionalV3(game_id=game_id).get_data_frames()[2]
    accumulated_data.append(box_score_team_data)
    print(f"{count} of {len(game_log_list)}")
    count += 1
    time.sleep(1)

all_box_scores = pd.concat(accumulated_data, ignore_index=True)

all_box_scores.columns = [c.lower() for c in all_box_scores.columns]

all_box_scores.rename(columns={'gameid': 'game_id',
                                'teamid': 'team_id',
                                'teamname': 'team_name'},
                                inplace=True)

connection_string = "postgresql+psycopg2://airflow:airflow@localhost/postgres"
engine = create_engine(connection_string)

all_box_scores.to_sql('team_boxscore', engine, if_exists='replace', index=False)

# Start collecting every day beginning Mar. 29 (data up till Mar. 28)
