import airflow

from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime
import os


# Define variables for later use
args = {
    "owner": "Bradley Bonitatibus",
    "start_date": "2020-02-01",
}

BUCKET_NAME = "{your_bucket_name}"
S3_KEY = "{your_s3_key}/example.py"
BOOTSTRAP_SCRIPT_URL = "{your_bootstrap_script}"
EC2_SUBNET_ID = "{your_ec2_subnet_id}"
S3_URL = "s3://{0}/{1}".format(BUCKET_NAME, S3_KEY)
S3_CONN_ID = "{your_s3_conn_id}"
AWS_CONN_ID = "{your_aws_conn_id}"
EMR_CONN_ID = "{your_emr_conn_id}"
AWS_REGION = "{your_prefered_aws_region}"

def copy_to_s3(**kwargs):
    hook = S3Hook(kwargs["connection_id"])
    hook.load_file(
        filename=kwargs["filename"],
        key=kwargs["key"],
        bucket_name=kwargs["bucket_name"],
        replace=True
    )
    return

# Define boto3 EMR Job Flow template
JOB_FLOW = {
    "Name": "Example EMR PySpark Job Flow",
    "ReleaseLabel": "emr-4.6.0",
    "Instances": {
        "MasterInstanceType": "m4.xlarge",
        "SlaveInstanceType": "m4.xlarge",
        "InstanceCount": 1,
        "KeepJobFlowAliveWhenNoSteps": True,
        "Ec2SubnetId": EC2_SUBNET_ID,
        "TerminationProtected": False,
    },
    "Applications":[
        {
            "Name": "Spark"
        }
    ],
    "BootstrapActions": [
        {
            "Name": "Maximize Spark Default Config",
            "ScriptBootstrapAction": {
                "Path": BOOTSTRAP_SCRIPT_URL
            }
        },
    ],
    "Steps": [
        {
            "Name": "Setup Debugging",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["state-pusher-script"]
            }
        },
        {
            "Name": "Copy script from S3",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["aws", "s3", "cp", S3_URL, "/home/hadoop"]
            }
        },
        {
            "Name": "Execute Spark Script",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "/home/hadoop/example.py"]
            }
        },
    ],
    "VisibleToAllUsers": True,
}

with DAG(
    dag_id="emr_python_runner",
    default_args=args,
    max_active_runs=1
) as dag:

    copy_python_file_to_s3 = PythonOperator(
        task_id="copy_script_to_s3",
        python_callable=copy_to_s3,
        op_kwargs={
            "filename": os.path.join(os.path.dirname(__file__), "example.py"),
            "key": S3_KEY,
            "bucket_name": BUCKET_NAME,
            "connection_id": S3_CONN_ID
        },
        provide_context=True
    )

    execute_spark_job = EmrCreateJobFlowOperator(
        task_id="execute_spark_job_emr",
        region_name=AWS_REGION,
        aws_conn_id=AWS_CONN_ID,
        emr_conn_id=EMR_CONN_ID,
        job_flow_overrides=JOB_FLOW
    )

    sensor = EmrJobFlowSensor(
        task_id="wait_for_job_to_finish",
        region_name=AWS_REGION,
        job_flow_id="{{ task_instance.xcom_pull('execute_spark_job_emr', key='return_value') }}",
        aws_conn_id="brad_aws",
    )

    delete_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        region_name=AWS_REGION,
        job_flow_id="{{ task_instance.xcom_pull('execute_spark_job_emr', key='return_value') }}",
        aws_conn_id="brad_aws"
    )

    copy_python_file_to_s3 >> execute_spark_job >> sensor >> delete_cluster

if __name__ == "__main__":
    dag.cli()