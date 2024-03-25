import pandas as pd
from sqlalchemy import create_engine
from nba_api.stats.endpoints.drafthistory import DraftHistory

'''
Importing the DraftHistory dataframe into the Postgres DB.
This script only needs to run once per year for each new draft class.
'''

df = DraftHistory().get_data_frames()[0]
df.columns = [c.lower() for c in df.columns]
df.rename(columns={'person_id': 'player_id'}, inplace=True)

connection_string = "postgresql+psycopg2://airflow:airflow@localhost/postgres"

engine = create_engine(connection_string)

df.to_sql('draft_history', engine, if_exists='replace', index=False)

