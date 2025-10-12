import hashlib
from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from fdwh_import.utils.get_s3_object_bytes import get_s3_object_bytes


def run_duplicate_check(s3: S3Hook, landing_bucket_key: str, landing_bucket: str,
                        preloaded_obj_bytes: BytesIO | None, pg_hook: PostgresHook) -> (bool, BytesIO):
    md5_hash, obj_bytes = get_md5_hash(s3, landing_bucket_key, landing_bucket, preloaded_obj_bytes)
    duplicate = hash_already_used(md5_hash, pg_hook)

    return duplicate, obj_bytes


def get_md5_hash(s3, landing_bucket_key: str, landing_bucket: str,
                 preloaded_obj_bytes: BytesIO | None) -> (str, BytesIO):
    if preloaded_obj_bytes:
        obj_bytes = preloaded_obj_bytes
    else:
        obj_bytes = get_s3_object_bytes(s3, landing_bucket_key, landing_bucket)
    md5_hash = hashlib.file_digest(obj_bytes, "md5").hexdigest()
    assert md5_hash
    return md5_hash, obj_bytes


def hash_already_used(md5_hash, pg_hook: PostgresHook) -> bool:
    records = pg_hook.get_records(
        "select 1 "
        "from core.metadata m "
        "join core.tree t on t.abs_filename = m.abs_filename "
        f"where t.\"type\" = 'collection' and m.hash = '{md5_hash}' "
    )
    return len(records) > 0