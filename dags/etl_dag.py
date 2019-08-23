from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'js',
    'start_date': datetime(2019, 1, 12),
    'catchup': False
}

dag = DAG(
          'etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval = '@monthly',
          max_active_runs = 1
)

# Dag start - create tables
start_operator = PostgresOperator(
                  task_id='Begin_execution',
                  dag=dag,
                  postgres_conn_id='redshift',
                  sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_event",
    table="staging_events", 
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_key="log_data",
    json = "s3://udacity-dend/log_json_path.json",     
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs", 
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_key="song_data",
    json = 'auto',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    sql_load=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    sql_load=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    sql_load=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    sql_load=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    sql_load=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table_list=["users", "songs", "artists", "time"],
    dag=dag
    )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator