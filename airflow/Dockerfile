FROM apache/airflow:2.9.3-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/jato-airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/jato-airflow-requirements.txt
