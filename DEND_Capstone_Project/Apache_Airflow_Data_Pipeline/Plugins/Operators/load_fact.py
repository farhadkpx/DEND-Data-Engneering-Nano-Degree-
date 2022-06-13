from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",                                       
                 destination_table = "", 
                 
                 sql_query = "",
                 sql_columns = "",
                 
                 delete = "False", 
                 *args, **kwargs):
        
        # initializing constructors
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id         
        self.destination_table = destination_table
        
        self.sql_query = sql_query
        self.sql_columns = sql_columns 
        
        self.delete = delete  

    def execute(self, context):
        """
           loading data into the fact table from staging tables
        """
        self.log.info("Creating connection to Redshift to start Load_Fact_Operator")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        #if self.delete:
        #    self.log.info("Clearing data from fact table")
        #    redshift.run("DELETE FROM {}".format(self.destination_table))                                                     #redshift.run(f"DELETE FROM {self.destination_table}")..?   
        sql_query = (f"\nINSERT INTO {self.destination_table} {self.sql_columns} {self.sql_query}")
#------------------------------------------------------------------------------------------------------------             #redshift.run(f"INSERT INTO {self.destination_table} {self.sql_columns} {selft.sql_query}")       
        #redshift.run(f"INSERT INTO {self.destination_table} {self.columns} {selft.sql_query}")
        #redshift.run("Insert into {} {} {}".format(self.destination_table, self.columns, self.sql_query)")
        # redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
#-------------------------------------------------------------------------------------------------------------            
        self.log.info(f"Loading data on fact table: {self.destination_table} table")
        self.log.info(f"Running SQL query: {sql_query}")
        redshift.run(sql_query)
        self.log.info("Loading FactOperator is Complete.\n")
        
        
