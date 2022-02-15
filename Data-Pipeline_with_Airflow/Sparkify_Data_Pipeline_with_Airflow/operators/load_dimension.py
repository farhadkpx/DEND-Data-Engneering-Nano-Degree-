from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate = False,
                 *args, **kwargs):
        
        # initializing class constructors
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        """
           loading data to dimension tables after truncation
        """
        self.log.info("Creating connection to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Emptying data before insertion into the table")
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
        # redshift.run("Insert into {} {}".format(self.table,  self.sql_query))