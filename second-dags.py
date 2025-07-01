from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from jinja2 import StrictUndefined

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.now().subtract(days=1),
}

dag = DAG(
    "test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    template_undefined=StrictUndefined,
)

class CustomSparkKubernetesOperator(SparkKubernetesOperator):
    template_ext = ('.yaml', '.yaml.j2')
    template_fields = ('application_file',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

submit_spark_job = CustomSparkKubernetesOperator(
    task_id="test",
    namespace="devops",
    application_file="test.yaml.j2",
    kubernetes_conn_id="kubernetes_default",
    in_cluster=True,
    do_xcom_push=True,
    get_logs=True,
    log_events_on_failure=True,
    params={"job_name": "test"},
    dag=dag,
)

submit_spark_job
