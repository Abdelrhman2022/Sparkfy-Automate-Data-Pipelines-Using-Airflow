from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 truncat="",
                 sql_query="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 *args, **kwargs):
        

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncat = truncat
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        # Connect To readShift To Run Query
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Truncate Table 
        if self.truncat:
            redshift.run(f"TRUNCATE TABLE {self.table}") #Truncate Table Query
        # SQL Query        
        formatted_sql = self.sql_query.format(self.table)
        redshift.run(formatted_sql) #Run Sql
        self.log.info(f"Success: {self.task_id}")
