import re

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sdk import Asset, dag, task

from fdwh_config import *


# extract date from path, i.e.:
# '/storage/fotki/collection/2023-05-15 Vacation photos',
# '/storage/fotki/collection/2021-12-25 Christmas'
# '/storage/fotki/collection/2021-12-23 What.jpg'
def _find_rightmost_date(path: str):
    """
    Find the date pattern YYYY-MM-DD that appears closest to the end of the path.
    Returns the rightmost date found, or None if no date exists.
    """
    pattern = re.compile(r'\b(\d{4}-\d{2}-\d{2})\b')

    # Find all date matches with their positions
    matches = []
    for match in pattern.finditer(path):
        matches.append({
            'date': match.group(1),
            'position': match.start(),
            'distance_from_end': len(path) - match.end()
        })

    if not matches:
        return None

    # Return the match with the smallest distance from the end (rightmost)
    rightmost_match = min(matches, key=lambda x: x['distance_from_end'])
    return rightmost_match['date']


def _exec_remote_cmd(cmd: str):
    try:
        ssh_hook = SSHHook(ssh_conn_id=Conn.SSH_STORAGE)
        ssh_client = ssh_hook.get_conn()
        stdin, stdout, stderr = ssh_client.exec_command(cmd)
        # Read the output
        remote_output = stdout.read().decode('utf-8')
        remote_error = stderr.read().decode('utf-8')
        print(f"Command executed: {cmd}")
        print(f"Remote Output:\n{remote_output}")
        if remote_error:
            print(f"Remote Error:\n{remote_error}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 3. Close the Connection
        if 'ssh_client' in locals() and ssh_client:
            ssh_client.close()
            print("SSH connection closed.")


schedule = (Asset(AssetName.CORE_AI_DESCR_UPDATED) | Asset(AssetName.CORE_TREE_UPDATED))
tags = {
    DagTag.SSH,
    DagTag.FDWH_STORAGE_IO,
}


@dag(dag_id=DagName.REFRESH_FLAT_SYMLINKS_BIRDS, max_active_runs=1, default_args=dag_args_retry, schedule=schedule,
     tags=tags, max_active_tasks=1)
def refresh_flat_symlinks_birds():
    @task
    def find_bird_dirs() -> list[(str, str)]:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        sql = 'select distinct directory from dm.col_images_birds'
        records = pg_hook.get_records(sql)
        return list(map(lambda row: row[0], records))

    @task
    def create_symlink(dir: str) -> None:
        cmds = []
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        sql = 'select abs_filename, short_filename from dm.col_images_birds where directory = %s'
        for row in pg_hook.get_records(sql, parameters=[dir]):
            abs_filename, short_filename = row[0], row[1]
            timestamp = _find_rightmost_date(abs_filename)
            if timestamp:
                root_dir = Variable.get(VariableName.RP_WORKCOPY)
                cmds.append(f'ln -s "{abs_filename}" /"{root_dir}"/плоские_птицы/"{timestamp}"_"{short_filename}"')
            else:
                print(f"timestamp not found in {abs_filename}")
        _exec_remote_cmd('; '.join(cmds))

    @task
    def rm_dead_symlinks() -> None:
        root_dir = Variable.get(VariableName.RP_WORKCOPY)
        cmd = f'cd /"{root_dir}"/плоские_птицы/ ' + '&&' + ' find . -type l ! -exec test -e {} \; -print -delete'
        _exec_remote_cmd(cmd)

    _bird_dirs = find_bird_dirs()
    _commands = create_symlink.expand(dir=_bird_dirs)

    rm_dead_symlinks()


@dag(dag_id=DagName.REFRESH_FLAT_SYMLINKS_VIDEO, max_active_runs=1, default_args=dag_args_retry, schedule=schedule,
     tags=tags, max_active_tasks=1)
def refresh_flat_symlinks_video():
    @task
    def find_video_dirs() -> list[(str, str)]:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        sql = 'select distinct directory from dm.col_images_video '
        records = pg_hook.get_records(sql)
        return list(map(lambda row: row[0], records))

    @task
    def create_symlink(dir: str) -> None:
        cmds = []
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        sql = 'select abs_filename, short_filename from dm.col_images_video where directory = %s'
        for row in pg_hook.get_records(sql, parameters=[dir]):
            abs_filename, short_filename = row[0], row[1]
            timestamp = _find_rightmost_date(abs_filename)
            if timestamp:
                root_dir = Variable.get(VariableName.RP_WORKCOPY)
                cmds.append(f'ln -s "{abs_filename}" /"{root_dir}"/плоские_видео/"{timestamp}"_"{short_filename}"')
            else:
                print(f"timestamp not found in {abs_filename}")
        _exec_remote_cmd('; '.join(cmds))

    @task
    def rm_dead_symlinks() -> None:
        root_dir = Variable.get(VariableName.RP_WORKCOPY)
        cmd = f'cd /"{root_dir}"/плоские_видео/ ' + '&&' + ' find . -type l ! -exec test -e {} \; -print -delete'
        _exec_remote_cmd(cmd)

    _video_dirs = find_video_dirs()
    _commands = create_symlink.expand(dir=_video_dirs)

    rm_dead_symlinks()


refresh_flat_symlinks_birds()
refresh_flat_symlinks_video()
