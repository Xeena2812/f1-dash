FROM apache/airflow:3.0.1

COPY ./dag-requirements.txt /
RUN pip install --no-cache-dir -r /dag-requirements.txt
USER root
RUN apt-get update && \
	apt-get install -y --no-install-recommends unzip && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*
USER airflow