from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from _shared import DEFAULT_ENV, repo_bash


with DAG(
    dag_id="jato_country_news_sync",
    description="Trigger backend country news refresh jobs through the API.",
    schedule="15 6 * * *",
    start_date=pendulum.datetime(2026, 4, 17, tz="UTC"),
    catchup=False,
    tags=["jato", "news"],
    params={
        "countries": "SE,FI,NO,DK,AT,CZ,HR,HU",
        "limit": 5,
    },
):
    BashOperator(
        task_id="refresh_country_news_snapshots",
        env=DEFAULT_ENV,
        bash_command=repo_bash(
            """
            {% set conf = dag_run.conf if dag_run and dag_run.conf else {} %}
            python - <<'PY'
            import json
            import os
            import requests

            api_base = os.environ["JATO_API_BASE"].rstrip("/")
            token = os.environ["JATO_API_TOKEN"]
            countries = "{{ conf.get('countries', params.countries) }}"
            limit = int("{{ conf.get('limit', params.limit) }}")
            headers = {
                "Content-Type": "application/json",
                "X-Auth-Token": token,
                "X-User-Role": "editor",
                "X-User-Name": "airflow",
            }
            for country in [item.strip() for item in countries.split(",") if item.strip()]:
                response = requests.post(
                    f"{api_base}/assistant/country/news/refresh",
                    headers=headers,
                    json={"country": country, "limit": limit, "persist": True},
                    timeout=120,
                )
                response.raise_for_status()
                print(f"[airflow-news] {country}: {response.status_code}")
                print(json.dumps(response.json(), ensure_ascii=False)[:800])
            PY
            """
        ),
    )
