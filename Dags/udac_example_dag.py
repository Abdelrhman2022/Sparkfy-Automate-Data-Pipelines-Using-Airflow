from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                PostgresOperator)
import create_tables
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 9, 15),
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'catchup': False,
    'schedule_interval': '@hourly'
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#         schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create_redshift_tables To Create Table In DataBase
# Sql Query From create_tables.py
create_redshift_tables = PostgresOperator(
    task_id="create_redshift_table",
    dag=dag,
    postgres_conn_id="redshift",  # connection we created in airflow ui admin connection
    # SQL Query To Create All Tables From create_tables.py
    sql=create_tables.CREATE_TABLES_SQL
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",  
    region="us-west-2", 
    s3_key="log_data", 
    s3_bucket="udacity-dend",  
    file_formate='json', 
    provide_context=True,  
    redshift_conn_id="redshift",  #
    aws_credentials_id="aws_credentials"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",  
    region="us-west-2",  
    s3_key="log_data",  
    s3_bucket="udacity-dend",  
    file_formate='json', 
    provide_context=True,  
    redshift_conn_id="redshift",  
    aws_credentials_id="aws_credentials"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql_query=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",  # Table Name
    truncat=True,  # To Execute Truncate Query
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.user_table_insert,
    redshift_conn_id="redshift",  
    aws_credentials_id="aws_credentials"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",  # Table Name
    truncat=True,  # To Execute Truncate Query
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.song_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",  # Table Name
    truncat=True,  # To Execute Truncate Query
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.song_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",  # Table Name
    truncat=True,  # To Execute Truncate Query
    # SQL Query From plugins/helper/sl_ueries.py
    sql_query=SqlQueries.time_table_insert,
    redshift_conn_id="redshift",  # connection we created in airflow ui admin connection
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    # connection we created in airflow ui admin connection
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',  # connection we created in airflow ui admin connection
    # Tables We Create Check For It
    tables=["public.songplays", "public.users",
            "public.songs", "public.artists", "public.time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]

load_songplays_table >> \
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,load_time_dimension_table] \
>> run_quality_checks

run_quality_checks >> end_operator
