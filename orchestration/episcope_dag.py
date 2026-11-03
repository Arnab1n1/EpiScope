"""ENG-4: Airflow DAG with retries (alternative to the Makefile DAG). Copy into $AIRFLOW_HOME/dags/.  Tasks mirror `make all`."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"retries": 2, "retry_delay": timedelta(minutes=2)}
with DAG("episcope_daily", start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False, default_args=default_args) as dag:
    bronze = BashOperator(task_id="bronze", bash_command="cd $EPISCOPE_HOME && python -m pipeline.run --stages bronze")
    silver = BashOperator(task_id="silver", bash_command="cd $EPISCOPE_HOME && python -m pipeline.run --stages silver")
    gold = BashOperator(task_id="gold", bash_command="cd $EPISCOPE_HOME && python -m pipeline.run --stages gold")
    analytics = BashOperator(task_id="analytics", bash_command="cd $EPISCOPE_HOME && python -m analytics.run_all --skip-backtest")
    bronze >> silver >> gold >> analytics
