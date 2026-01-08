from pytz import timezone

dag_args_noretry = {
    'retries': 0,
}

dag_args_retry = {
    'retries': 10,
}

server_tz_name = 'Europe/Samara'
server_timezone = timezone(server_tz_name)


class Conn:
    POSTGRES = 'postgres'
    MINIO = 'minio'

    SMB_ARCHIVE = "storage_smb_archive"
    SMB_COLLECTION = "storage_smb_collection"
    SMB_TRASH = "storage_smb_trash"

    SSH_STORAGE = 'storage_ssh'


class DagName:
    ADD_MISSING_CAPTION = 'add_missing_caption'
    ADD_MISSING_METADATA = 'add_missing_metadata'
    FIND_COLLECTION_DUPLICATES = 'find_collection_duplicates'
    IMPORT_LANDING_FILES_S3 = 'import_landing_files_s3'
    PING_SERVICES = 'ping_services'
    REFRESH_CORE_TREE = 'refresh_core_tree'
    REFRESH_DATAMARTS = 'refresh_datamarts'
    REFRESH_FLAT_SYMLINKS_BIRDS = 'refresh_flat_symlinks_birds'
    REFRESH_FLAT_SYMLINKS_VIDEO = 'refresh_flat_symlinks_video'
    REFRESH_RAW_TREES = 'refresh_raw_trees'
    REFRESH_WORKCOPY_SYMLINKS = 'refresh_workcopy_symlinks'
    RM_COLLECTION_DUPLICATES = 'rm_collection_duplicates'
    CREATE_STORAGE_ZFS_SNAPSHOTS = 'create_storage_zfs_snapshots'


class DagTag:
    CLEANUP = 'cleanup'
    MONITORING = 'monitoring'
    PG = 'pg'
    S3 = 's3'
    SMB = 'smb'
    SSH = 'ssh'
    BACKUP = 'backup'

    DWH_RAW = 'dwh_raw'
    DWH_CORE = 'dwh_core'
    DWH_MARTS = 'dwh_marts'

    DUPLICATES = 'duplicates'
    HELPERS = 'helpers'
    STORAGE_IO = 'storage_io'


class AssetName:
    IMPORT_COMPLETE = 'import_complete'
    RAW_TREES_UPDATED = 'raw_trees_updated'
    CORE_TREE_UPDATED = 'core_tree_updated'
    CORE_METADATA_UPDATED = 'core_metadata_updated'
    CORE_CAPTION_UPDATED = 'core_caption_updated'


class ImportSettings:
    COMPANION_FILE_EXTENSIONS = ['.mov', '.MOV', '.mp4', '.MP4', ]
    TIMESTAMP_FMT, TIMESTAMP_LEN = '%Y-%m-%d-auto', 15


class VariableName:
    BUCKET_LANDING = 'BUCKET_LANDING'
    BUCKET_REJECTED_DUPLICATES = 'BUCKET_REJECTED_DUPLICATES'
    BUCKET_REJECTED_UNSUPPORTED = 'BUCKET_REJECTED_UNSUPPORTED'
    BUCKETS = [BUCKET_LANDING, BUCKET_REJECTED_DUPLICATES, BUCKET_REJECTED_UNSUPPORTED]

    STORAGE_PATH_ARCHIVE = 'RP_ARCHIVE'
    STORAGE_PATH_COLLECTION = 'RP_COLLECTION'
    STORAGE_PATH_TRASH = 'RP_TRASH'
    STORAGE_PATH_WORKCOPY = 'RP_WORKCOPY'

    STORAGE_ZFS_DATASET_ARCHIVE = 'STORAGE_ZFS_DATASET_ARCHIVE'
    STORAGE_ZFS_DATASET_COLLECTION = 'STORAGE_ZFS_DATASET_COLLECTION'
    STORAGE_ZFS_DATASET_TRASH = 'STORAGE_ZFS_DATASET_TRASH'
    STORAGE_ZFS_DATASET_WORKCOPY = 'STORAGE_ZFS_DATASET_WORKCOPY'

    OLLAMA_ENDPOINT = 'OLLAMA_ENDPOINT'
    METADATA_ENDPOINT = 'METADATA_ENDPOINT'
    EXIF_TS_ENDPOINT = 'EXIF_TS_ENDPOINT'
