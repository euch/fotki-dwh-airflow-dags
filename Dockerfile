FROM apache/airflow:3.1.5-python3.12

USER airflow

COPY requirements.txt .
RUN pip install --disable-pip-version-check -r requirements.txt

COPY dags/ /opt/airflow/dags/
