from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from _shared import DEFAULT_ENV, repo_bash


with DAG(
    dag_id="jato_scraping_toolkit_manual",
    description="Run the generic scraping toolkit entrypoint with manual args.",
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 17, tz="UTC"),
    catchup=False,
    tags=["jato", "toolkit", "scraping"],
    params={"tool_args": "--help"},
):
    BashOperator(
        task_id="run_scraping_toolkit",
        env=DEFAULT_ENV,
        bash_command=repo_bash(
            """
            {% set conf = dag_run.conf if dag_run and dag_run.conf else {} %}
            PYTHON_BIN=/usr/local/bin/python \
            bash 03_Scripts/run_scraping_tool.sh {{ conf.get('tool_args', params.tool_args) }}
            """
        ),
    )
