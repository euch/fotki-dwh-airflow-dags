from io import BytesIO


def get_s3_object_bytes(s3, key: str, bucket: str) -> BytesIO:
    resp_obj = s3.get_object(Bucket=bucket, Key=key)
    data = resp_obj['Body'].read()
    return BytesIO(data)

