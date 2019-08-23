from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {}
        ;
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_load="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_load=sql_load
  """
    Insert data into dimension tables sourced from staging_events and staging songs table.  
    Tables: artists, songs, time, users
    Parameters -
        - redshift_conn_id (string):  Airflow connection to the database cluster
        - sql_load (strint): SQL insert statements
        - table (string): Redshift database 
     """
    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from Redshift from prior run")
        pg.run("DELETE FROM {}".format(self.table))        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table, 
            self.sql_load
        )
        self.log.info(f"Executing {formatted_sql} ...")
        pg.run(formatted_sql)