_conn_prefix = 'fdwh_'
_var_prefix = 'FDWH_'

SCHEDULE_DAILY='@daily'
SCHEDULE_MANUAL=None

CONN_POSTGRES = _conn_prefix + 'postgres'

CONN_SMB_ARCHIVE = _conn_prefix + "storage_smb_archive"
CONN_SMB_COLLECTION = _conn_prefix + "storage_smb_collection"
CONN_SMB_TRASH = _conn_prefix + "storage_smb_trash"

CONN_SSH_STORAGE = _conn_prefix + 'storage_ssh'

DAG_NAME_DWH_FIND_COLLECTION_DUPLICATES = 'dwh_find_collection_duplicates'
DAG_NAME_DWH_RM_COLLECTION_DUPLICATES = 'dwh_rm_collection_duplicates'
DAG_NAME_DWH_INITIAL_IMPORT = 'dwh_initial_import'
DAG_NAME_DWH_IMPORT = 'dwh_import'
DAG_NAME_DWH_REFRESH = 'dwh_refresh'

IMPORT_COMPANION_FILE_EXTENSIONS = ['.mov', '.MOV', '.mp4', '.MP4', ]
IMPORT_TIMESTAMP_FMT, IMPORT_TIMESTAMP_LEN = '%Y-%m-%d-auto', 15

VAR_LP_LANDING = _var_prefix + 'LP_LANDING'
VAR_LP_REJECTED = _var_prefix + 'LP_REJECTED'

VAR_RP_ARCHIVE = _var_prefix + 'RP_ARCHIVE'
VAR_RP_COLLECTION = _var_prefix + 'RP_COLLECTION'
VAR_RP_TRASH = _var_prefix + 'RP_TRASH'

VAR_TG_TOKEN = _var_prefix + 'TG_BOT_TOKEN'
VAR_TG_USER_IDS = _var_prefix + 'TG_USER_IDS'

VAR_AI_DESCR_ENDPOINT = _var_prefix + 'AI_DESCR_ENDPOINT'
VAR_METADATA_ENDPOINT = _var_prefix + 'METADATA_ENDPOINT'
VAR_EXIF_TS_ENDPOINT = _var_prefix + 'EXIF_TS_ENDPOINT'
