from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

# Step 1: Look for a file on S3 [DONE]
# Step 2: Once the file is present, spin up an EMR cluster
# Step 3: Once the EMR cluster is spun up, have it run some PySpark script (this script writes out something new)

# Step 4: Sense that "something new" file
# Step 5: Spin Down EMR

EMR_STEP_1 = [
    {
        "Name": "EMR TEST STEP 1",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://ds-afarrell/hello_world_writer.py"
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "EMR_TEST_1",
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master - 1", "InstanceRole": "MASTER", "InstanceType": "m4.large", "InstanceCount": 1},
            {"Name": "Core - 2", "InstanceCount": 2, "InstanceType": "r4.xlarge", "InstanceRole": "CORE"},
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2SubnetId": "subnet-02f24d64",
        "EmrManagedMasterSecurityGroup": "sg-aae44bd6",
        "EmrManagedSlaveSecurityGroup": "sg-6ce64910",
        "ServiceAccessSecurityGroup": "sg-cce54ab0"
    },
    "BoostrapActions": [
        {
            "Name": "EMR Bootstrap (pip-3.4)",
            "ScriptBootstrapAction": {"Path": "s3://ds-bin/emr_bootstrap_benchmarks.sh"},
        }
    ]
}

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 24),
    "email": ["jbecker@tenable.com"],
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
    "retries": 2
}

dag = DAG(
    'EMR_TEST_1',
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval="0 1 * * *"
)

with dag:
    file_sensor = S3KeySensor(
        task_id='file_sensor',
        poke_interval=600,
        timeout=1000,
        soft_fail=False,
        bucket_name='ds-afarrell',
        bucket_key='manybla.txt'
    )

    create_cluster = EmrCreateJobFlowOperator(
        task_id='create_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_benchmarks_connection'
    )

    run_some_pyspark = EmrAddStepsOperator(
        task_id='run_some_pyspark',
        job_flow_id="{{ task_instance.xcom_pull('create_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=EMR_STEP_1
    )

    output_file_sensor = S3KeySensor(
        task_id='output_file_sensor',
        poke_interval=600,
        timeout=1000,
        soft_fail=False,
        bucket_name='ds-afarrell',
        bucket_key='hello_world_was_written/_SUCCESS'
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="cluster_remover",
        job_flow_id="{{ task_instance.xcom_pull('create_cluster', key='return_value') }}",
        aws_conn_id="aws_default"
    )


    file_sensor >> create_cluster >> run_some_pyspark >> output_file_sensor >> cluster_remover
