SCHEDULE_DAILY = '@daily'
SCHEDULE_MANUAL = None


class Conn:
    _conn_prefix = 'fdwh_'

    POSTGRES = _conn_prefix + 'postgres'

    MINIO = 'fdwhminio' # todo change to _conn_prefix + 'minio'

    SMB_ARCHIVE = _conn_prefix + "storage_smb_archive"
    SMB_COLLECTION = _conn_prefix + "storage_smb_collection"
    SMB_TRASH = _conn_prefix + "storage_smb_trash"

    SSH_STORAGE = _conn_prefix + 'storage_ssh'


class DagName:
    _prefix = 'fdwh_'
    CHECK_HELPER_SERVICES = _prefix + '_check_helper_services'
    UPDATE_DATAMARTS = _prefix + 'update_datamarts'
    FIND_COLLECTION_DUPLICATES = _prefix + 'find_collection_duplicates'
    RM_COLLECTION_DUPLICATES = _prefix + 'rm_collection_duplicates'
    IMPORT_FROM_LOCAL_MEM_CARD = _prefix + 'import_from_local_mem_card'
    PROCESS_SMB_LANDING_FILES = _prefix + 'process_smb_landing_files'
    REFRESH_STORAGE_TREE_INDEX = _prefix + 'refresh_storage_tree_index'


class AssetName:
    IMPORT_RESULT = 'fdwh_import_result'

    MISSING_METADATA_ARCHIVE = 'fdwh_missing_ma'
    MISSING_METADATA_COLLECTION = 'fdwh_missing_mc'
    MISSING_METADATA_TRASH = 'fdwh_missing_mt'
    MISSING_AI_DESCR_COLLECTION = 'fdwh_missing_aidc'

    METADATA_HELPER_AVAIL = 'fdwh_metadata_helper_avail'
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

    TG_TOKEN = _var_prefix + 'TG_BOT_TOKEN'
    TG_USER_IDS = _var_prefix + 'TG_USER_IDS'

    AI_DESCR_ENDPOINT = _var_prefix + 'AI_DESCR_ENDPOINT'
    METADATA_ENDPOINT = _var_prefix + 'METADATA_ENDPOINT'
    EXIF_TS_ENDPOINT = _var_prefix + 'EXIF_TS_ENDPOINT'
