from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from airflow.sdk import Asset, dag, task, TaskGroup, Variable
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.types import DagRunType
from pendulum import Timezone

from fdwh_common import exec_remote_cmd
from fdwh_config import (
    DagName, server_tz_name, AssetName, DagTag,
    dag_args_retry, VariableName, Conn, server_timezone
)

SCHEDULE = AssetOrTimeSchedule(
    timetable=CronTriggerTimetable('0 0 * * *', timezone=Timezone(server_tz_name)),
    assets=Asset(AssetName.CORE_TREE_UPDATED)
)
TAGS = {DagTag.FDWH_STORAGE_IO, DagTag.BACKUP}


class SnapshotType(StrEnum):
    WEEKLY = "weekly"
    DAILY_DIFF = "daily_diff"
    ON_ASSET_CHANGE = "on_asset_change"
    MANUAL = "manual"


check_diff_snapshot_types: set[SnapshotType] = {
    SnapshotType.WEEKLY,
    SnapshotType.MANUAL
}

NEW_SNAPSHOT_TS_FMT = '%y%m%d_%H%M'


@dataclass
class SnapshotConfig:
    alias: str
    dataset_variable_name: str
    ignored_snapshot_types: set[SnapshotType] = ()


snapshot_configs = [
    SnapshotConfig('archive', VariableName.STORAGE_ZFS_DATASET_ARCHIVE),
    SnapshotConfig('collection', VariableName.STORAGE_ZFS_DATASET_COLLECTION),
    SnapshotConfig('trash', VariableName.STORAGE_ZFS_DATASET_TRASH),
    SnapshotConfig('workcopy', VariableName.STORAGE_ZFS_DATASET_WORKCOPY, {SnapshotType.ON_ASSET_CHANGE}),
]


def create_dataset_snapshot_group(config: SnapshotConfig, snapshot_type):
    dataset_name = Variable.get(config.dataset_variable_name)

    with TaskGroup(group_id=config.alias):

        @task.short_circuit()
        def filter_snapshot_type(snapshot_type):
            return snapshot_type not in config.ignored_snapshot_types

        @task
        def get_latest_snapshot() -> str:
            cmd = f'zfs list -t snapshot -o name -s creation -r "{dataset_name}" | tail -1'
            res, err = exec_remote_cmd(cmd, ssh_conn_id=Conn.SSH_STORAGE)
            assert res.startswith(dataset_name + '@')
            assert len(err) == 0
            return res

        @task()
        def find_changes(snapshot_to_compare: str) -> list[str]:
            assert snapshot_to_compare
            cmd = f'zfs diff -h {snapshot_to_compare}'
            res, err = exec_remote_cmd(cmd, ssh_conn_id=Conn.SSH_STORAGE)
            assert len(err) == 0
            res_lines = res.splitlines()
            for line in res_lines:
                assert line[1] == '\t'
            return res_lines

        @task()
        def log_changes(changes: list[str]) -> int:
            # TODO: Implement change logging
            return 1

        @task.short_circuit()
        def evaluate_snapshot_need(snapshot_type, changes: list[str]) -> bool:
            if snapshot_type not in check_diff_snapshot_types:
                return True
            else:
                return len(changes) > 0

        @task()
        def create_snapshot(snapshot_type) -> str:
            ts = datetime.now(tz=server_timezone).strftime(NEW_SNAPSHOT_TS_FMT)
            new_snapshot_name = f'{dataset_name}@{ts}_{snapshot_type}'
            cmd = f'zfs snapshot {new_snapshot_name}'
            res, err = exec_remote_cmd(cmd, ssh_conn_id=Conn.SSH_STORAGE)
            assert len(err) == 0
            return new_snapshot_name

        @task()
        def log_snapshot_creation(log_id: int, snapshot_name: str) -> None:
            # TODO: Implement snapshot logging
            pass

        filter_snapshot_type = filter_snapshot_type(snapshot_type)
        latest_snapshot = get_latest_snapshot()
        filter_snapshot_type >> latest_snapshot
        changes = find_changes(latest_snapshot)
        log_id = log_changes(changes)
        should_create = evaluate_snapshot_need(snapshot_type, changes)
        snapshot_name = create_snapshot(snapshot_type)
        should_create >> snapshot_name
        log_snapshot_creation(log_id, snapshot_name)


@dag(
    dag_id=DagName.CREATE_STORAGE_ZFS_SNAPSHOTS,
    max_active_runs=1,
    default_args=dag_args_retry,
    schedule=SCHEDULE,
    tags=TAGS
)
def dag():
    @task
    def find_snapshot_type(**context):
        run_type = context['dag_run'].run_type
        if run_type == DagRunType.ASSET_TRIGGERED:
            return SnapshotType.ON_ASSET_CHANGE
        elif run_type == DagRunType.MANUAL:
            return SnapshotType.MANUAL
        elif context['logical_date'].weekday() == 0:
            return SnapshotType.WEEKLY
        else:
            return SnapshotType.DAILY_DIFF

    snapshot_type = find_snapshot_type()

    for config in snapshot_configs:
        create_dataset_snapshot_group(config, snapshot_type)


dag()
