from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class BulkInsertOperator(BaseOperator):
    def __init__(self, pg_conn_name: str, table_name: str, local_csv_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pg_conn_name = pg_conn_name
        self.table_name = table_name
        self.local_csv_path = local_csv_path

    def execute(self, context):
        postgres_hook = PostgresHook.get_hook(self.pg_conn_name)
        postgres_hook.run('truncate table ' + self.table_name)
        postgres_hook.bulk_load(self.table_name, self.local_csv_path)
