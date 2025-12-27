from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class CheckBucketHelperAvailableOperator(BaseOperator):
    def __init__(self, connection, bucket_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.connection = connection
        self.bucket_name = bucket_name

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.connection)
        bucket_exists = s3_hook.check_for_bucket(bucket_name=self.bucket_name)

        assert bucket_exists

        print(f"S3 bucket '{self.bucket_name}' exists.")
