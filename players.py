import pandas as pd
from sqlalchemy import create_engine
from nba_api.stats.static import players

'''
Importing the players dataframe into the Postgres DB.
'''

player_list = players.get_players()

df = pd.DataFrame(player_list)
df.rename(columns={'id': 'player_id'}, inplace=True)

connection_string = "postgresql+psycopg2://airflow:airflow@localhost/postgres"
engine = create_engine(connection_string)

df.to_sql('players', engine, if_exists='replace', index=False)