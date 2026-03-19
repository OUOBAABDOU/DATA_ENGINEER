FROM apache/airflow:2.10.5-python3.12

COPY requirements.txt /requirements.txt

USER root

RUN rm -rf /var/lib/apt/lists/* && apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --quiet --no-cache-dir -r /requirements.txt

USER root
