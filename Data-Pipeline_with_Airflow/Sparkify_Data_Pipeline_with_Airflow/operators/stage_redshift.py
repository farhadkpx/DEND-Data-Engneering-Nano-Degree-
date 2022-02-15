from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    template_fields = ("s3_key",) 
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):
        # initializing arguments of the class
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        """
           creating connection to S3 bucket and loading data to staging tables in Redshift
        """
        self.log.info("creating all the needed connections and credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Emptying table first before data insertion from S3 bucket
        redshift.run(f"DELETE FROM {self.table}")
        
        self.s3_key = self.s3_key.format(**context)
        
        s3_path = (f"s3://{self.s3_bucket}/{self.s3_key}")
        
        redshift.run(f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' \
                            SECRET_ACCESS_KEY '{credentials.secret_key}' FORMAT AS JSON '{self.json_path}'")
        
        #-----------------------------------------------------------------------------------------------------
        # if self.file_format == "json":
        #    file_processing = "JSON '{}'".format(self.json_path)
        #---------+++++++++++++++++++++++++++++++++++++++++++++++++++
        # formatted_sql = StageToRedshiftOperator.copy_sql.format(
        #    self.table,
        #    s3_path,
        #    credentials.access_key,
        #    credentials.secret_key,
        #   file_processing
        # )
        # redshift.run(formatted_sql)





