from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 data_quality_checks = "",...?
                 tables = "",
                 *args, **kwargs):
        
        # initializing class constructors
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks..?
        self.tables = tables

    def execute(self, context):
        """
           Running data quality checks on tables
        """
        redshift = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failed_tests = []
        
        self.log.info('Starting to Data Quality checks:')
        # looping through 'data quality checks
        for check in self.data_quality_checks:
            sql_check = check.get('check_sql')...?
            exp_result = check.get('expected_result')...?
            # saving any null value with the primary key
            num_records = redshift.get_records(sql_check)[0]..?
            # comparing expected result and sql result
            if num_records[0] != exp_result:
                 error_count += 1
                 failed_tests.append(sql_check)
            # if error_count is more than 0 show data quality failings    
            if error_count > 0:
                self.log.info('Test has failed')
                self.log.info(failed_tests)
                raise ValueError('Data Quality Check has failed')
            else:
                self.log.info('Data Quality check has Passed')
            
        
#==============================================================================================
   # @apply_defaults
   # def __init__(self,
    #             redshift_conn_id = "",
     #            tables = None,
      #           *args, **kwargs):

      #  super(DataQualityOperator, self).__init__(*args, **kwargs)
      #  self.tables = tables
      #  self.redshift_conn_id = redshift_conn_id
    
    
    
    #   def execute(self, context):
       # self.log.info('Starting to Data Quality checks:')
       # redshift_hook = PostgresHook(self.redshift_conn_id)
        
       # for table in self.tables:
       #     records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
       #     if len(records) < 1 or len(records[0]) < 1:
       #         raise ValueError(f"Data quality check failed. {table} returned no results")
       #     num_records = records[0][0]
       #     if num_records < 1:
       #        raise ValueError(f"Data quality check failed. {table} returned 0 records")
       #    self.log.info(
       #        f"Data Quality check on table {table} passed with {records[0][0]} records")
    
    
    
    