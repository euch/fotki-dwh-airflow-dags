FROM apache/airflow:3.1.5-python3.12

# Switch to root user to install system-level dependencies if necessary
USER root

# Install any required system packages (e.g., git, gcc)
# Example: RUN apt-get update && apt-get install -yqq git build-essential && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install --disable-pip-version-check -r requirements.txt

COPY dags/dwh                  /opt/airflow/dags/
COPY dags/storage              /opt/airflow/dags/
COPY dags/config.py            /opt/airflow/dags/
COPY dags/dag_ping_services.py /opt/airflow/dags/
#
COPY plugins/ /opt/airflow/plugins/
