FROM apache/airflow:2.9.3

EXPOSE 8080

WORKDIR /opt/airflow

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

COPY . .
