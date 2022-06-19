from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",                                                                            
                 destination_table = "", 
                 sql_query = "",
                 columns_sql = "",                 
                 delete = "False",   
                 *args, **kwargs):
        
        # initializing class constructors
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id        
        self.destination_table = destination_table
        self.sql_query = sql_query
        self.columns_sql = columns_sql 
        self.delete = delete
        
        
    def execute(self, context):
        """
           loading data to dimension tables after truncation
        """
        self.log.info("Creating connection to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        if self.delete:
            self.log.info("Clearing data from destination table")
            redshift.run(f"DELETE FROM {self.destination_table}")          
        
        sql_query = (f"\nINSERT INTO {self.destination_table} {self.columns_sql} {self.sql_query}")
        
        self.log.info(f"Loading data on dimension table: {self.destination_table} table")
        self.log.info(f"Running SQL query: {sql_query}")
        redshift.run(sql_query)
        self.log.info("Loading Dimension Operator is Complete.\n")
        
        
        #-------------------------------------------------------------------------------------------#
        # self.log.info("Emptying data before insertion into the table")                            #
        # if self.truncate:                                                                         #
        #    redshift.run(f"TRUNCATE TABLE {self.destination_table}")                               #
        # sql_query = f"\nINSERT INTO {self.destination_table} {self.columns_sql} {self.sql_query}" #         
        #-------------------------------------------------------------------------------------------#
        #  sql_query = f"\nINSERT INTO {self.destination_table} {self.columns} ({self.sql})"        #
        # redshift.run(f"INSERT INTO {self.destination_table} {self.sql_query}")                    #     
        # redshift.run("Insert into {} {}".format(self.table,  self.sql_query))                     #
        #-------------------------------OR----------------------------------------------------------#
        # if self.delete:                                                                           #
        #    self.log.info("Clearing data from destination table")                                  #
        #    redshift.run("Delete from {}".format(self.destination_table))                          #         
        # sql_query = f"\nINSERT INTO {self.destination_table} {self.columns_sql} {self.sql_query}" #           
        #-------------------------------------------------------------------------------------------#
        
        
