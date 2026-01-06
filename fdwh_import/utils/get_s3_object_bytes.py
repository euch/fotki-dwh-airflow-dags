from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_s3_object_bytes(s3: S3Hook, key: str, bucket: str) -> BytesIO:
    resp_obj = s3.get_object(Bucket=bucket, Key=key)
    data = resp_obj['Body'].read()
    return BytesIO(data)

