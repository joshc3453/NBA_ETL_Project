from nba_api.stats.endpoints import CommonTeamRoster
from nba_api.stats.static import teams
from sqlalchemy import create_engine
import time
from datetime import datetime, timedelta
import pandas as pd

teams_list = teams.get_teams()

team_list_ids = [team['id'] for team in teams_list]

# This will be used to dynamically create roster tables for each team
ids_and_names_list = [{'id': team['id'], 'full_name': team['full_name']} for team in teams_list]

connection_string = "postgresql+psycopg2://airflow:airflow@localhost/postgres"
engine = create_engine(connection_string)

for team in teams_list:
    team_name = team['full_name'].lower().replace(' ', '_')
    df = CommonTeamRoster(team_id=team['id']).get_data_frames()[0]
    df.columns = [c.lower() for c in df.columns]
    df['birth_date'] = pd.to_datetime(df['birth_date']).dt.date
    df.rename(columns={'teamid': 'team_id', 'leagueid': 'league_id'})
    table_name = f"{team_name}_roster_{df['season'][0]}"
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"{table_name} loaded successfully.")
    time.sleep(0.6)