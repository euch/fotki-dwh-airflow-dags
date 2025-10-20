from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import Asset, DAG, Variable, TaskGroup

from fdwh_config import *

schedule = (Asset(AssetName.NEW_FILES_IMPORTED))
tags = {
    DagTag.SSH,
    DagTag.FDWH_STORAGE_IO,
}

refresh_latest_collection_subdir_symlink_cmd = f'''

COLLECTION_DIR="{Variable.get(VariableName.STORAGE_PATH_COLLECTION)}"
# Check if we found a directory
if [ -z "COLLECTION_DIR" ]; then
    echo "Error: COLLECTION_DIR is empty"
    exit 1
fi

WORKCOPY_DIR="{Variable.get(VariableName.STORAGE_PATH_WORKCOPY)}"
# Check if we found a directory
if [ -z "WORKCOPY_DIR" ]; then
    echo "Error: WORKCOPY_DIR is empty"
    exit 1
fi

cd "$COLLECTION_DIR"

# Find the latest directory
LATEST_SUBDIR=$(ls -d [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*/ 2>/dev/null | sort -r | head -n 1)

# Remove trailing slash if present
LATEST_SUBDIR=${{LATEST_SUBDIR%/}}

# Check if we found a directory
if [ -z "LATEST_SUBDIR" ]; then
    echo "Error: No directories matching YYYY-MM-DD pattern found"
    exit 1
fi

# Attaching path
LATEST_DIR="$COLLECTION_DIR/$LATEST_SUBDIR"

cd "$WORKCOPY_DIR"

# Define the symlink name
SYMLINK_NAME="auto-latest"

# Remove existing symlink if it exists
if [ -L "$SYMLINK_NAME" ]; then
    echo "Removing existing symlink $SYMLINK_NAME"
    rm "$SYMLINK_NAME"
fi

# Create new symlink
echo "Creating symlink $SYMLINK_NAME -> $LATEST_DIR"
ln -s "$LATEST_DIR" "$SYMLINK_NAME"
'''

with DAG(dag_id=DagName.REFRESH_WORKCOPY_SYMLINKS, max_active_runs=1, default_args=dag_args_retry, schedule=schedule,
         tags=tags) as dag:
    _ = SSHOperator(
        task_id="refresh_latest_collection_subdir_symlink",
        ssh_conn_id=Conn.SSH_STORAGE,
        command=refresh_latest_collection_subdir_symlink_cmd,
        do_xcom_push=False)

    with TaskGroup(group_id="refresh_custom_symlinks"):
        _ = SSHOperator(
            task_id='закат',
            do_xcom_push=False,
            ssh_conn_id=Conn.SSH_STORAGE,
            command=f'''find "{Variable.get(VariableName.STORAGE_PATH_COLLECTION)}" -iname "*закат*" -exec ln -s {{}} /"{Variable.get(VariableName.STORAGE_PATH_WORKCOPY)}"/закат \;''')

        with TaskGroup(group_id='птицовы'):
            _ = SSHOperator(
                task_id='сускан',
                do_xcom_push=False,
                ssh_conn_id=Conn.SSH_STORAGE,
                command=f'''find "{Variable.get(VariableName.STORAGE_PATH_COLLECTION)}" -iname "*сускан*" -exec ln -s {{}} /"{Variable.get(VariableName.STORAGE_PATH_WORKCOPY)}"/птицовы \;''')
            _ = SSHOperator(
                task_id='птиц',
                do_xcom_push=False,
                ssh_conn_id=Conn.SSH_STORAGE,
                command=f'''find "{Variable.get(VariableName.STORAGE_PATH_COLLECTION)}" -iname "*птиц*" -exec ln -s {{}} /"{Variable.get(VariableName.STORAGE_PATH_WORKCOPY)}"/птицовы \;''')

        with TaskGroup(group_id='жена'):
            _ = SSHOperator(
                task_id='катя',
                do_xcom_push=False,
                ssh_conn_id=Conn.SSH_STORAGE,
                command=f'''find "{Variable.get(VariableName.STORAGE_PATH_COLLECTION)}" -iname "*катя*" -exec ln -s {{}} /"{Variable.get(VariableName.STORAGE_PATH_WORKCOPY)}"/жена \;''')
            _ = SSHOperator(
                task_id='кате',
                do_xcom_push=False,
                ssh_conn_id=Conn.SSH_STORAGE,
                command=f'''find "{Variable.get(VariableName.STORAGE_PATH_COLLECTION)}" -iname "*кате*" -exec ln -s {{}} /"{Variable.get(VariableName.STORAGE_PATH_WORKCOPY)}"/жена \;''')
