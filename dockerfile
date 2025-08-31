FROM apache/airflow:slim-3.0.6-python3.11

USER root
RUN apt-get update && \
    apt-get -y install git && \ 
    apt-get clean s

USER airflow 

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Add the project's root directory to the PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"