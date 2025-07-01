from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from jinja2 import StrictUndefined
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

class CustomSparkKubernetesOperator(SparkKubernetesOperator):
    template_ext = ('.yaml', '.yaml.j2')
    template_fields = ('application_file',)

# Default args for all DAGs
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

# Load job definitions
etl_jobs = Variable.get("etl_jobs", default_var="[]", deserialize_json=True)

# Generate DAGs dynamically
for config in etl_jobs:
    dag_id = config["spark_dag_name"]
    namespace = config["spark_namespace"]
    tasks_config = config.get("tasks", [])
    task_order = config.get("task_order", [])
    schedule = config.get("schedule_interval", None)

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
        template_undefined=StrictUndefined,
        tags=["dynamic", "spark"]
    )

    task_map = {}
    for task_conf in tasks_config:
        task = CustomSparkKubernetesOperator(
            task_id=task_conf["task_id"],
            namespace=namespace,
            application_file=task_conf['params']['spark_application_file'],
            #application_file=task_conf['spark_application_file'],
            kubernetes_conn_id="kubernetes_default",
            in_cluster=True,
            get_logs=True,
            do_xcom_push=False,
            log_events_on_failure=True,
            params=task_conf["params"],
            dag=dag
        )
        task_map[task_conf["task_id"]] = task

    def get_task_objs(stage):
        if isinstance(stage, list):
            return [task_map[tid] for tid in stage]
        else:
            return [task_map[stage]]

    for i in range(len(task_order) - 1):
        current_tasks = get_task_objs(task_order[i])
        next_tasks = get_task_objs(task_order[i + 1])
        for current in current_tasks:
            for next_ in next_tasks:
                current >> next_

    globals()[dag_id] = dag