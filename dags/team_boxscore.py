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
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d')

    df = TeamGameLogs(season_nullable='2023-24').get_data_frames()[0]
    df.columns = [c.lower() for c in df.columns]
    game_id_and_date = df[['game_id', 'game_date']]
    game_id_and_date['game_date'] = pd.to_datetime(game_id_and_date['game_date'])
    game_id_and_date['game_date'] = game_id_and_date['game_date'].dt.strftime('%Y-%m-%d')
    game_id_and_date = game_id_and_date.drop_duplicates()

    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    yesterday_game_ids = game_id_and_date[game_id_and_date['game_date']==yesterday_str][['game_id']]
    #yesterday_game_ids = game_id_and_date[game_id_and_date['game_date']=='2024-03-30'][['game_id']]
    yesterday_game_ids = yesterday_game_ids['game_id'].tolist()

    accumulated_data = []

    for game_id in yesterday_game_ids:
        box_score_team_data = BoxScoreTraditionalV3(game_id=game_id).get_data_frames()[2]
        accumulated_data.append(box_score_team_data)
        print(f"{game_id} loaded successfully.")
        time.sleep(1)
    
    if len(accumulated_data) > 0:
        all_box_scores = pd.concat(accumulated_data, ignore_index=True)
        all_box_scores.columns = [c.lower() for c in all_box_scores.columns]
        all_box_scores.rename(columns={'gameid': 'game_id',
                                       'teamid': 'team_id',
                                       'teamname': 'team_name'},
                                       inplace=True)

        file_name = f"team_boxscore_{execution_date.strftime('%Y%m%d')}.csv"
        s3_key = f"nba_logs/{file_name}"
    
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmpfile:
            all_box_scores.to_csv(tmpfile.name, index=False)
            tmpfile.flush()

            s3_hook = S3Hook()
            s3_hook.load_file(filename=tmpfile.name, bucket_name=bucket_name, key=s3_key, replace=True)
    
        return s3_key

    else:
        print('No games played yesterday.')
        return None

def load_data_to_postgres(bucket_name, file_key, **kwargs):
    
    s3_hook = S3Hook()
    file_content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)
    df = pd.read_csv(StringIO(file_content), dtype={'game_id': str} )
    print(df)

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        df.to_sql('team_boxscore', conn, if_exists='append', index=False)

with DAG(
    'get_yesterday_box_score',
    default_args=default_args,
    description="Automatically updates team box scores in a PostgreSQL database.",
    schedule_interval='0 13 * * *',
    start_date=datetime(2024, 3, 29),
    catchup=False,
    tags=['nba', 'jmc'],
) as dag:
    
    extract_yesterday_games = PythonOperator(
        task_id='get_yesterday_games',
        python_callable=get_yesterday_games,
        op_kwargs={'bucket_name': 'airflow-project-jmc',
                   'execution_date': '{{ ds }}'
                   },
    )

    add_to_table = PythonOperator(
        task_id='add_to_table',
        python_callable=load_data_to_postgres,
        op_kwargs={
            'bucket_name': 'airflow-project-jmc',
            'file_key': '{{ ti.xcom_pull(task_ids="get_yesterday_games") }}',
        }
    )

    extract_yesterday_games >> add_to_table

    # Next steps - adjust to handle times where there are no games yesterday