import telebot
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from fdwh_config import *

TG_USER_IDS = list(map(int, Variable.get(VariableName.TG_USER_IDS).split(',')))


@dag(dag_id=DagName.IMPORT_FROM_LOCAL_MEM_CARD, max_active_runs=1, schedule=None)
def create_dag():
    mount_sd_card = SSHOperator(
        task_id="mount_sd_card",
        ssh_conn_id="fdwh_landing_ssh",
        command="sudo mount /dev/mmcblk0p1 ~/mount/sd/",
    )
    copy_from_sd_to_landing = SSHOperator(
        task_id="copy_from_sd_to_landing",
        ssh_conn_id="fdwh_landing_ssh",
        command="cp -R ~/mount/sd/* /fotki/input/landing/",
        cmd_timeout=None,
    )
    umount_sd_card = SSHOperator(
        task_id="umount_sd_card",
        ssh_conn_id="fdwh_landing_ssh",
        command="sudo umount ~/mount/sd/",
    )

    @task
    def send_tg_notif():
        bot = telebot.TeleBot(Variable.get(VariableName.TG_TOKEN))
        for user_id in TG_USER_IDS:
            bot.send_message(user_id,
                             "All files were successfully copied from the SD card to the landing directory. Running dwh_import.")

    trigger_dwh_import = TriggerDagRunOperator(
        task_id="trigger_" + DagName.PROCESS_SMB_LANDING_FILES,
        trigger_dag_id=DagName.PROCESS_SMB_LANDING_FILES,
        wait_for_completion=False),

    mount_sd_card >> copy_from_sd_to_landing >> umount_sd_card >> send_tg_notif() >> trigger_dwh_import


dag = create_dag()
