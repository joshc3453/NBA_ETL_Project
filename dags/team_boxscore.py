from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from nba_api.stats.endpoints import BoxScoreTraditionalV3
from nba_api.stats.endpoints.teamgamelogs import TeamGameLogs
import pandas as pd
from io import StringIO
import tempfile
import sqlalchemy.types
import os
import time

default_args = {
    'owner': 'jmc',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_yesterday_games(bucket_name, execution_date, **kwargs):
    df = TeamGameLogs(season_nullable='2023-24').get_data_frames()[0]
    df.columns = [c.lower() for c in df.columns]
    game_id_and_date = df[['game_id', 'game_date']]
    game_id_and_date['game_date'] = pd.to_datetime(game_id_and_date['game_date'])
    game_id_and_date['game_date'] = game_id_and_date['game_date'].dt.strftime('%Y-%m-%d')
    game_id_and_date = game_id_and_date.drop_duplicates()

    game_id_and_date['game_date']


    # Issue: TeamGameLogs not bringing in yesterday's games? No records for 2024-03-28 as of 2024-03-29
    # When revisiting script, make sure df variable is getting most recent games.


    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    yesterday_game_ids = game_id_and_date[game_id_and_date['game_date']==yesterday_str][['game_id']]
    yesterday_game_ids = yesterday_game_ids['game_id'].tolist()

    accumulated_data = []

    for game_id in yesterday_game_ids:
        box_score_team_data = BoxScoreTraditionalV3(game_id=game_id).get_data_frames()[2]
        accumulated_data.append(box_score_team_data)
        print(f"{id}")
        time.sleep(1)
    
    all_box_scores = pd.concat(accumulated_data, ignore_index=True)

    # Add error handling if blank yesterday_game_ids
    # Continue building out dag