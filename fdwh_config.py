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
    _conn_prefix = 'fdwh_'

    POSTGRES = _conn_prefix + 'postgres'
    MINIO = _conn_prefix + 'minio'

    SMB_ARCHIVE = _conn_prefix + "storage_smb_archive"
    SMB_COLLECTION = _conn_prefix + "storage_smb_collection"
    SMB_TRASH = _conn_prefix + "storage_smb_trash"

    SSH_STORAGE = _conn_prefix + 'storage_ssh'


class DagName:
    _prefix = 'fdwh_'
    ADD_MISSING_CAPTION = _prefix + 'add_missing_caption'
    ADD_MISSING_METADATA = _prefix + 'add_missing_metadata'
    FIND_COLLECTION_DUPLICATES = _prefix + 'find_collection_duplicates'
    IMPORT_LANDING_FILES_S3 = _prefix + 'import_landing_files_s3'
    PING_SERVICES = _prefix + 'ping_services'
    REFRESH_CORE_TREE = _prefix + 'refresh_core_tree'
    REFRESH_DATAMARTS = _prefix + 'refresh_datamarts'
    REFRESH_FLAT_SYMLINKS_BIRDS = _prefix + 'refresh_flat_symlinks_birds'
    REFRESH_FLAT_SYMLINKS_VIDEO = _prefix + 'refresh_flat_symlinks_video'
    REFRESH_RAW_TREES = _prefix + 'refresh_raw_trees'
    REFRESH_WORKCOPY_SYMLINKS = _prefix + 'refresh_workcopy_symlinks'
    RM_COLLECTION_DUPLICATES = _prefix + 'rm_collection_duplicates'
    CREATE_STORAGE_ZFS_SNAPSHOTS = _prefix + 'create_storage_zfs_snapshots'


class DagTag:
    CLEANUP = 'cleanup'
    MONITORING = 'monitoring'
    PG = 'pg'
    S3 = 's3'
    SMB = 'smb'
    SSH = 'ssh'
    BACKUP = 'backup'

    _prefix = 'fdwh_'
    FDWH_RAW = _prefix + 'raw'
    FDWH_CORE = _prefix + 'core'
    FDWH_MARTS = _prefix + 'marts'

    FDWH_DUPLICATES = _prefix + 'duplicates'
    FDWH_HELPERS = _prefix + 'helpers'
    FDWH_STORAGE_IO = _prefix + 'storage_io'


class AssetName:
    NEW_FILES_IMPORTED = 'fdwh_new_files_imported'
    RAW_TREES_UPDATED = 'fdwh_raw_trees_updated'
    CORE_TREE_UPDATED = 'fdwh_core_tree_updated'
    CORE_METADATA_UPDATED = 'fdwh_core_metadata_updated'
    CORE_AI_DESCR_UPDATED = 'fdwh_core_ai_description_updated'


class ImportSettings:
    COMPANION_FILE_EXTENSIONS = ['.mov', '.MOV', '.mp4', '.MP4', ]
    TIMESTAMP_FMT, TIMESTAMP_LEN = '%Y-%m-%d-auto', 15


class VariableName:
    _var_prefix = 'FDWH_'

    BUCKET_LANDING = _var_prefix + 'BUCKET_LANDING'
    BUCKET_REJECTED_DUPLICATES = _var_prefix + 'BUCKET_REJECTED_DUPLICATES'
    BUCKET_REJECTED_UNSUPPORTED = _var_prefix + 'BUCKET_REJECTED_UNSUPPORTED'

    STORAGE_PATH_ARCHIVE = _var_prefix + 'RP_ARCHIVE'
    STORAGE_PATH_COLLECTION = _var_prefix + 'RP_COLLECTION'
    STORAGE_PATH_TRASH = _var_prefix + 'RP_TRASH'
    STORAGE_PATH_WORKCOPY = _var_prefix + 'RP_WORKCOPY'

    STORAGE_ZFS_DATASET_ARCHIVE = _var_prefix + 'STORAGE_ZFS_DATASET_ARCHIVE'
    STORAGE_ZFS_DATASET_COLLECTION = _var_prefix + 'STORAGE_ZFS_DATASET_COLLECTION'
    STORAGE_ZFS_DATASET_TRASH = _var_prefix + 'STORAGE_ZFS_DATASET_TRASH'
    STORAGE_ZFS_DATASET_WORKCOPY = _var_prefix + 'STORAGE_ZFS_DATASET_WORKCOPY'

    AI_DESCR_ENDPOINT = _var_prefix + 'AI_DESCR_ENDPOINT'
    OLLAMA_ENDPOINT = _var_prefix + 'OLLAMA_ENDPOINT'
    METADATA_ENDPOINT = _var_prefix + 'METADATA_ENDPOINT'
    EXIF_TS_ENDPOINT = _var_prefix + 'EXIF_TS_ENDPOINT'
