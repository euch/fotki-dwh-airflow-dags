dag_args_noretry = {
    'retries': 0,
}

dag_args_retry = {
    'retries': 10,
}


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
    FIND_COLLECTION_DUPLICATES = _prefix + 'find_collection_duplicates'
    FIND_MISSING_AI_DESCR = _prefix + 'find_missing_ai_descr'
    FIND_MISSING_METADATA = _prefix + 'find_missing_metadata'
    IMPORT_LANDING_FILES_S3 = _prefix + 'import_landing_files_s3'
    PING_SERVICES = _prefix + 'ping_services'
    REFRESH_CORE_TREE = _prefix + 'refresh_core_tree'
    REFRESH_DATAMARTS = _prefix + 'refresh_datamarts'
    REFRESH_RAW_TREES = _prefix + 'refresh_raw_trees'
    REFRESH_WORKCOPY_SYMLINKS = _prefix + 'refresh_workcopy_symlinks'
    RM_COLLECTION_DUPLICATES = _prefix + 'rm_collection_duplicates'


class DagTag:
    CLEANUP = 'cleanup'
    MONITORING = 'monitoring'
    PG = 'pg'
    S3 = 's3'
    SMB = 'smb'
    SSH = 'ssh'

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
    AI_HELPER_AVAILABLE = 'fdwh_ai_helper_available'

class ImportSettings:
    COMPANION_FILE_EXTENSIONS = ['.mov', '.MOV', '.mp4', '.MP4', ]
    TIMESTAMP_FMT, TIMESTAMP_LEN = '%Y-%m-%d-auto', 15


class VariableName:
    _var_prefix = 'FDWH_'

    BUCKET_LANDING = _var_prefix + 'BUCKET_LANDING'
    BUCKET_REJECTED_DUPLICATES = _var_prefix + 'BUCKET_REJECTED_DUPLICATES'
    BUCKET_REJECTED_UNSUPPORTED = _var_prefix + 'BUCKET_REJECTED_UNSUPPORTED'

    RP_ARCHIVE = _var_prefix + 'RP_ARCHIVE'
    RP_COLLECTION = _var_prefix + 'RP_COLLECTION'
    RP_TRASH = _var_prefix + 'RP_TRASH'
    RP_WORKCOPY = _var_prefix + 'RP_WORKCOPY'

    AI_DESCR_ENDPOINT = _var_prefix + 'AI_DESCR_ENDPOINT'
    METADATA_ENDPOINT = _var_prefix + 'METADATA_ENDPOINT'
    EXIF_TS_ENDPOINT = _var_prefix + 'EXIF_TS_ENDPOINT'
