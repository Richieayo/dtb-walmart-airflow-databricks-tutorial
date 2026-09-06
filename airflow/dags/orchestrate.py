import os
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from pendulum import datetime


@dag(
        dag_id = 'orchestrate',
        schedule = "0 11 * * *",
        catchup = False,
        start_date = datetime(year = 2026, month = 9, day = 7, tz = 'UTC')
)
def orchestrate():

    @task
    def ingest_cdc(**context):
        ws = WorkspaceClient(
            host = os.environ["DTK_HOST"],
            token = os.environ["DTK_TOKEN"]
        )

        job_trigger = ws.jobs.run_now(job_id = int(os.environ["JOB_ID"]),
                                      idempotency_token=f"cd-{context['dag_run'].run_id}")

        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")

            time.sleep(5)
        return "CDC Ingestion completed"

    @task.bash
    def source_freshness():
        #Manually set the working directory using cd
        return "rm -rf /opt/airflow/walmart_project/target && cd /opt/airflow/walmart_project && dbt source freshness"


    silver_technical = BashOperator(
        task_id = 'silver_technical',
        cwd = '/opt/airflow/walmart_project',
        bash_command = 'dbt run --select silver_t'
    )

    silver_technical_tests = BashOperator(
            task_id = 'silver_technical_tests',
            cwd = '/opt/airflow/walmart_project',
            bash_command = 'dbt test --select silver_t'
        )

    silver_business = BashOperator(
                task_id = 'silver_business',
                cwd = '/opt/airflow/walmart_project',
                bash_command = 'dbt run --select silver_b'
            )

    silver_business_tests = BashOperator(
                    task_id = 'silver_business_tests',
                    cwd = '/opt/airflow/walmart_project',
                    bash_command = 'dbt test --select silver_b'
                )

    gold_ephemeral = BashOperator(
        task_id = 'gold_ephemeral',
        cwd = '/opt/airflow/walmart_project',
        bash_command = 'dbt run --select gold/ephemeral'
    )

    gold_dimensions = BashOperator(
        task_id = 'gold_dimensions',
        cwd = '/opt/airflow/walmart_project',
        bash_command = 'dbt snapshot'
    )

    gold_facts = BashOperator(
        task_id = 'gold_facts',
        cwd = '/opt/airflow/walmart_project',
        bash_command = 'dbt run --select gold/fact'
    )

    ingest_cdc() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()