from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from _shared import DEFAULT_ENV, repo_bash


with DAG(
    dag_id="jato_msrp_low_concurrency",
    description="Run MSRP dry-run then ingest with low concurrency controls.",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 17, tz="UTC"),
    catchup=False,
    tags=["jato", "msrp", "scraping"],
    params={
        "countries": "all",
        "pause_seconds": 20,
        "stop_on_failure": "true",
    },
):
    dryrun = BashOperator(
        task_id="dryrun_country_batch",
        env=DEFAULT_ENV,
        bash_command=repo_bash(
            """
            {% set conf = dag_run.conf if dag_run and dag_run.conf else {} %}
            JATO_MSRP_MODE=dryrun \
            JATO_MSRP_EXECUTION_CONTEXT=airflow_scheduled \
            JATO_MSRP_COUNTRIES="{{ conf.get('countries', params.countries) }}" \
            JATO_MSRP_PAUSE_SECONDS="{{ conf.get('pause_seconds', params.pause_seconds) }}" \
            JATO_MSRP_STOP_ON_FAILURE="{{ conf.get('stop_on_failure', params.stop_on_failure) }}" \
            JATO_MSRP_PYTHON=/usr/local/bin/python \
            bash 03_Scripts/run_msrp_low_concurrency.sh
            """
        ),
    )

    ingest = BashOperator(
        task_id="ingest_country_batch",
        env=DEFAULT_ENV,
        bash_command=repo_bash(
            """
            {% set conf = dag_run.conf if dag_run and dag_run.conf else {} %}
            JATO_MSRP_MODE=ingest \
            JATO_MSRP_EXECUTION_CONTEXT=airflow_scheduled \
            JATO_MSRP_COUNTRIES="{{ conf.get('countries', params.countries) }}" \
            JATO_MSRP_PAUSE_SECONDS="{{ conf.get('pause_seconds', params.pause_seconds) }}" \
            JATO_MSRP_STOP_ON_FAILURE="{{ conf.get('stop_on_failure', params.stop_on_failure) }}" \
            JATO_MSRP_PYTHON=/usr/local/bin/python \
            bash 03_Scripts/run_msrp_low_concurrency.sh
            """
        ),
    )

    dryrun >> ingest
