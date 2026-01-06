from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_s3_object_size(s3: S3Hook, key: str, bucket: str) -> int:
    response = s3.head_object(Bucket=bucket, Key=key)
    length = response['ContentLength']
    return length
