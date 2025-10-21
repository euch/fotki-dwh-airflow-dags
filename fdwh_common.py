from airflow.providers.ssh.hooks.ssh import SSHHook


def exec_remote_cmd(cmd: str, ssh_conn_id: str) -> (str, str):
    try:
        ssh_hook = SSHHook(ssh_conn_id)
        ssh_client = ssh_hook.get_conn()
        stdin, stdout, stderr = ssh_client.exec_command(cmd)
        # Read the output
        remote_output = stdout.read().decode('utf-8')
        remote_error = stderr.read().decode('utf-8')
        print(f"Command executed: {cmd}")
        print(f"Remote Output:\n{remote_output}")
        if remote_error:
            print(f"Remote Error:\n{remote_error}")
        return remote_output, remote_error
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 3. Close the Connection
        if 'ssh_client' in locals() and ssh_client:
            ssh_client.close()
            print("SSH connection closed.")
