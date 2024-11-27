from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    'weather-data-processor',
    description='Runs Weather Data Processor Sprak job',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    spark_task = DockerOperator(
        task_id='run_spark_job',
        image='weather-data-processor',
        command='--master local[*] --name weather-data-processor --class org.goraj.weatherapp.wdp.Entrypoint /app/weather-data-processor-assembly-0.1.jar -d 2020-07-10',
        api_version='auto',
        auto_remove=True,  # Remove container after execution
        docker_url='unix://var/run/docker.sock',  # Docker socket for local Docker
        network_mode='spark-minio-network',  # Ensure it uses the proper network to access Minio
        tty=True,
    )
