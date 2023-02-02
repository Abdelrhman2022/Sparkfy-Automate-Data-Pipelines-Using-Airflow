from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql_query="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # Connect To Postgre On RedShift
        redshift_hook =PostgresHook(self.redshift_conn_id)
        # Run Query
        redshift_hook.run(str(self.sql_query))
