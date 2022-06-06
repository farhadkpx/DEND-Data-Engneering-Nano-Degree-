from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    #template_fields = ("s3_key",) 
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id= "",
                 aws_credentials_id= "",
                 destination_table= "",
                 s3_bucket= "",
                 s3_key= "",
                 *args, **kwargs):
        
        # initializing arguments of the class
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.destination_table = destination_table
        #self.s3_path = f"s3://{self.s3_bucket}/{self.s3_key}".. /?
        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key        
        s3_path = (f"s3://{self.s3_bucket}/{self.s3_key}") # /?
        
        # self.s3_key = self.s3_key.format(**context) /?
#----------------------------------------------------------------------------        
        # >>>  def build_cmd(self, credentials):
    def copy_query(self, credentials):
        copy_cmd = f"""
        COPY {self.destination_table}
        FROM '{self.s3_path}'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        FORMAT AS PARQUET
        COMPUPDATE OFF;
        """
        self.log.info(f"Copy command: {copy_cmd}")
        return copy_cmd
    
#-------------------------------------------------------------------------        
    def execute(self, context):
        """
           creating connection to S3 bucket and loading data to staging tables in Redshift
        """
        self.log.info("creating all the needed connections and credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # getting redshift credentials
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Emptying table first before data insertion from S3 bucket
        #redshift.run(f"DELETE FROM {self.destination_table}")..?
        # redshift.run("Delete from {}".format(self.destination_table))
        
        #self.s3_key = self.s3_key.format(**context)       
        #s3_path = (f"s3://{self.s3_bucket}/{self.s3_key}")
        
        #redshift.run(f"Copying data from {self.s3_path} to Redshift {self.destination_table} table")
        self.log.info(
            f"Copying data from {self.s3_path} to Redshift {self.destination_table} table"
        )
        
        redshift.run(self.copy_query(credentials)) #../?
        
        #.....>>>   redshift.run(self.build_cmd(credentials))../?
        # redshift.run(f"Copying data {self.destination_table} from {self.s3_source_path} with  
        # {credentials.access_key} and {credentials.secret_key} format as_parquet")
        
        self.log.info("*Completing Stage To RedshiftOperator: ***\n")
        
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





