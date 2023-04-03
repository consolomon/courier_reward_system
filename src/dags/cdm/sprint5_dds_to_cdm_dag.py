from airflow import DAG
from airflow.operators.python import PythonOperator
from cdm.report_loader import ReportLoader

import datetime
import logging
from lib import ConnectionBuilder


###POSTGRESQL settings###
#set postgresql connection from basehook
POSTGRES_CONN_ID = 'PG_WAREHOUSE_CONNECTION'
PATH_TO_SCRIPT = '/lessons/dags/cdm/dds_to_cdm_dml.sql'
pg_conn = ConnectionBuilder.pg_conn(POSTGRES_CONN_ID)
log = logging.getLogger(__name__)

def reports_etl():
    reports_loader = ReportLoader(pg_conn, log, PATH_TO_SCRIPT)
    reports_loader.load_reports()

with DAG(
    dag_id='sprint5_dds_to_cdm_dag',
    schedule_interval='0 0 2 * *',
    start_date=datetime.datetime.today() - datetime.timedelta(days=31),
    catchup=False,
    dagrun_timeout=datetime.timedelta(seconds=60),
    tags=['sprint5', 'dds', 'cdm']
) as dag:

    update_dm_settlement_report = PythonOperator(
        task_id='update_dm_courier_ledger',
        python_callable=reports_etl
    )
    
    update_dm_settlement_report




