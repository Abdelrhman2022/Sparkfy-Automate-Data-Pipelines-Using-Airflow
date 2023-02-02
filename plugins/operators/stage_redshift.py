from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
     '''Operator loads any JSON formatted files from S3 to Amazon Redshift.
   
    redshift_conn_id (string):  Airflow conn_id of Redshift connection
    aws_credentials_id (string): Airflow conn_id of the aws_credentials granting access to s3 (Default: 'aws_credentials')  
    table (string):  The name of the Amazon Redshift table where the data should be loaded
    s3_path (string): The data source where to get data from          
    
    '''
    ui_color = '#358140'
    

    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 s3_key="",
                 region="",
                 s3_bucket="",
                 file_formate="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_key = s3_key
        self.region = region
        self.s3_bucket = s3_bucket
        self.file_formate = file_formate
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')
        
        
    def execute(self, context):
        # Connect To Aws 
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        #Connect to RedShift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Delete Data From Table
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        # Copy Data From S3
        self.log.info("Copying data from S3 to Redshift")
        # Formation Data Url
        s3_path = "s3://{}".format(self.s3_bucket)
        
        if self.execution_date:
            # Backfill a specific date
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            s3_path = '/'.join([s3_path, str(year), str(month), str(day)])

        s3_path = s3_path+'/'+self.s3_key
        additional = ""
        # Check Data Format
        if self.file_formate == 'csv':
            additional = " DELIMETER ',' IGNOREHEADER 1 "

        # Formation Copy Sql Query
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_formate,
            additional
        )
        # Run Query
        redshift.run(formatted_sql)

        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")





