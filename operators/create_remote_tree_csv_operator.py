from airflow.providers.ssh.operators.ssh import SSHOperator


def cmd_create_tree(location_rp: str, remote_csv_name) -> str:
    return f'''
        find "{location_rp}" -type f -print0 | xargs -0r stat -f '%N	%m	%z' > "{location_rp}"/"{remote_csv_name}"
    '''


class CreateRemoteTreeCsvOperator(SSHOperator):
    def __init__(self, ssh_conn_id, location_rp, remote_csv_name, **kwargs) -> None:
        super().__init__(
            ssh_conn_id=ssh_conn_id,
            cmd_timeout=None,
            command=cmd_create_tree(location_rp, remote_csv_name),
            do_xcom_push=False,
            **kwargs)
