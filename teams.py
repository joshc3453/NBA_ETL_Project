import pandas as pd
from sqlalchemy import create_engine
from nba_api.stats.static import teams

teams_list = teams.get_teams()

df = pd.DataFrame(teams_list)
df.rename(columns={'id': 'team_id'}, inplace=True)

# teams_list shows the Golden State Warriors city to be Golden State. Golden State is not a city in California. The correct city should be San Francisco.
df['city'].replace('Golden State', 'San Francisco', inplace=True)

connection_string = "postgresql+psycopg2://airflow:airflow@localhost/postgres"
engine = create_engine(connection_string)

df.to_sql('teams', engine, if_exists='replace', index=False)