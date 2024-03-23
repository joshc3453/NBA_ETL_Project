from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from nba_api.stats.endpoints import LeagueStandings
import pandas as pd
from io import StringIO
import tempfile
import os

default_args = {
    'owner': 'jmc',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_team_standings(bucket_name, execution_date, **kwargs):
    team_stats = LeagueStandings().get_data_frames()[0]
    full_team_names = team_stats['TeamCity'] + ' ' + team_stats['TeamName']
    team_stats['TeamName'] = full_team_names
    team_stats_clean = team_stats[[
        'TeamID', 'TeamName', 'Conference', 'Record', 'WINS', 'LOSSES', 'Division',
        'DivisionRecord', 'DivisionRank', 'WinPCT', 'L10', 'OT',
        'CurrentStreak', 'PointsPG', 'OppPointsPG', 'DiffPointsPG'
    ]]

    team_stats_clean.columns = [c.lower() for c in team_stats_clean.columns]
    
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d')

    file_name = f"team_standings_{execution_date.strftime('%Y%m%d')}.csv"
    s3_key = f"nba_logs/{file_name}"

    # Use a temporary file to avoid directory issues
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmpfile:
        team_stats_clean.to_csv(tmpfile.name, index=False)
        tmpfile.flush()
    
        s3_hook = S3Hook()
        s3_hook.load_file(filename=tmpfile.name, bucket_name=bucket_name, key=s3_key, replace=True)

    return s3_key

def load_data_to_postgres(bucket_name, file_key, **kwargs):
    
    s3_hook = S3Hook()
    file_content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)
    df = pd.read_csv(StringIO(file_content))

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    engine = pg_hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        df.to_sql('team_standings', conn, if_exists='replace', index=False)

with DAG(
    'get_team_standings',
    default_args=default_args,
    description="Automatically updates team standings in a PostgreSQL database.",
    schedule_interval='0 13 * * *',
    start_date=datetime(2024, 3, 15),
    catchup=False,
    tags=['nba', 'jmc'],
) as dag:
    
    extract_team_standings = PythonOperator(
        task_id='extract_team_standings',
        python_callable=get_team_standings,
        op_kwargs={'bucket_name': 'airflow-project-jmc',
                   'execution_date': '{{ ds }}'
                   },
    )

    create_postgres_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS team_standings (
                teamid TEXT,
                teamname TEXT, 
                conference TEXT, 
                record TEXT, 
                wins INTEGER,
                losses INTEGER, 
                division TEXT, 
                divisionrecord TEXT,
                divisionrank INTEGER, 
                winpct FLOAT, 
                l10 TEXT, 
                ot TEXT,
                currentstreak INTEGER, 
                pointspg FLOAT, 
                opppointspg FLOAT,
                diffpointspg FLOAT,
                primary key (teamid)
            );
        """
    )

    add_to_table = PythonOperator(
        task_id='add_to_table',
        python_callable=load_data_to_postgres,
        op_kwargs={
            'bucket_name': 'airflow-project-jmc',
            'file_key': '{{ ti.xcom_pull(task_ids="extract_team_standings") }}',
        },
    )

    extract_team_standings >> create_postgres_table >> add_to_table
