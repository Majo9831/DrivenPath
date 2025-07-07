from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import random
import csv
import logging
import uuid
import polars as pl
from faker import Faker
from datetime import date, timedelta

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)

# ---------------- FUNCIONES AUXILIARES ----------------

def create_data(locale: str) -> Faker:
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)

def generate_record(fake: Faker) -> list:
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = fake.ssn()
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ")
    phone_number = fake.phone_number()
    mac_address = fake.mac_address()
    ip_address = fake.ipv4()
    iban = fake.iban()
    accessed_at = fake.date_time_between("-1y")
    session_duration = random.randint(0, 36000)
    download_speed = random.randint(0, 1000)
    upload_speed = random.randint(0, 800)
    consumed_traffic = random.randint(0, 2000000)

    return [
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic
    ]

def write_to_csv(file_path: str, rows: int) -> None:
    fake = create_data("es_MX")
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]
    with open(file_path, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for _ in range(rows):
            writer.writerow(generate_record(fake))
    logging.info(f"Written {rows} records to the CSV file.")

def add_id(file_name) -> None:
    df = pl.read_csv(file_name)
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    df.write_csv(file_name)
    logging.info("Added UUID to the dataset.")

def update_datetime(file_name: str, run: str) -> None:
    if run == "next":
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        df = pl.read_csv(file_name)
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        df.write_csv(file_name)
        logging.info("Updated accessed timestamp.")

def save_raw_data():
    file_path = "/opt/airflow/data/raw_data.csv"
    write_to_csv(file_path, 100)
    add_id(file_path)
    update_datetime(file_path, run="next")

# ---------------- CONFIGURACIÓN DEL DAG ----------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'extract_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline.',
    schedule_interval="* 7 * * *",
    start_date=datetime(2024, 9, 22),
    catchup=False,
)

# ---------------- DEFINICIÓN DE TAREAS ----------------

extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)

create_raw_schema_task = SQLExecuteQueryOperator(
    task_id='create_raw_schema',
    conn_id='postgres_conn',
    sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag=dag,
)

create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
    CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
        person_name VARCHAR(150),
        user_name VARCHAR(150),
        email VARCHAR(150),
        personal_number VARCHAR(150),
        birth_date VARCHAR(150),
        address VARCHAR(150),
        phone VARCHAR(150),
        mac_address VARCHAR(150),
        ip_address VARCHAR(150),
        iban VARCHAR(150),
        accessed_at TIMESTAMP,
        session_duration INT,
        download_speed INT,
        upload_speed INT,
        consumed_traffic INT,
        unique_id VARCHAR(150)
    );
    """,
    dag=dag,
)

load_raw_data_task = SQLExecuteQueryOperator(
    task_id='load_raw_data',
    conn_id='postgres_conn',
    sql="""
    COPY driven_raw.raw_batch_data(
        person_name, user_name, email, personal_number, birth_date,
        address, phone, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic,
        unique_id
    )
    FROM '/opt/data/raw_data.csv'
    DELIMITER ','
    CSV HEADER;
    """,
    dag=dag,
)

run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='cd /opt/airflow/dbt && dbt run --select tag:staging',
    dag=dag,
)

run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command='cd /opt/airflow/dbt && dbt run --select tag:trusted',
    dag=dag,
)


# ---------------- DEPENDENCIAS ENTRE TAREAS ----------------

[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task