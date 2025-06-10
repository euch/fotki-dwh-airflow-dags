SCHEDULE_DAILY = '@daily'
SCHEDULE_MANUAL = None

dag_default_args = {
    'retries': 0,
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
    IMPORT_LANDING_FILES_SMB = _prefix + 'import_landing_files_smb'
    REFRESH_RAW_TREES = _prefix + 'refresh_raw_trees'
    REFRESH_EDM_TREE = _prefix + 'refresh_edm_tree'
    RM_COLLECTION_DUPLICATES = _prefix + 'rm_collection_duplicates'
    UPDATE_DATAMARTS = _prefix + 'update_datamarts'


class AssetName:
    NEW_FILES_IMPORTED = 'fdwh_as_new_files_imported'

    RAW_TREES_UPDATE_TICK = 'fdwh_as_raw_trees_update_tick'
    RAW_TREES_UPDATED = 'fdwh_as_raw_trees_updated'
    EDM_TREE_UPDATED = 'fdwh_as_edm_tree_updated'

    MISSING_METADATA_ARCHIVE = 'fdwh_missing_ma'
    MISSING_METADATA_COLLECTION = 'fdwh_missing_mc'
    MISSING_METADATA_TRASH = 'fdwh_missing_mt'
    MISSING_AI_DESCR_COLLECTION = 'fdwh_missing_aidc'

    METADATA_HELPER_AVAIL = 'fdwh_metadata_helper_avail'
    EXIF_TS_HELPER_AVAIL = 'fdwh_exif_ts_helper_avail'
    AI_DESCR_HELPER_AVAIL = 'fdwh_ai_descr_helper_avail'

    ADD_METADATA_ARCHIVE = 'fdwh_add_ma'
    ADD_METADATA_COLLECTION = 'fdwh_add_mc'
    ADD_METADATA_TRASH = 'fdwh_add_mt'
    ADD_AI_DESCR_COLLECTION = 'fdwh_add_aidc'


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

    AI_DESCR_ENDPOINT = _var_prefix + 'AI_DESCR_ENDPOINT'
    METADATA_ENDPOINT = _var_prefix + 'METADATA_ENDPOINT'
    EXIF_TS_ENDPOINT = _var_prefix + 'EXIF_TS_ENDPOINT'

    LP_LANDING = _var_prefix + 'LP_LANDING'
    LP_REJECTED = _var_prefix + 'LP_REJECTED'
