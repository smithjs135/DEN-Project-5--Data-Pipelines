from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

""" 
Data verification scripts
Check for NULL values
Last modified: JS 8/14/2019
"""


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_list=table_list

    def execute(self, context):
        self.log.info("Data quality checks")
        pg_hook = PostgresHook(self.redshift_conn_id)

        for i in self.table_list:
            records = pg_hook.get_records(f"SELECT COUNT(*) FROM {i}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {i} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {i} contained 0 rows")
            self.log.info(f"Data quality on table {i} check passed with {records[0][0]} records")

