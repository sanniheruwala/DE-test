import airflow
from airflow import DAG,macros
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.sensors import S3KeySensor, ExternalTaskSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from airflow.models import TaskInstance
from airflow.utils.state import State
from functools import partial
from datetime import date, datetime, timedelta
import json

dag_name = 'recipe_run'

args = {
    'owner': 'sheruwala',
    'depends_on_past': True,
    'wait_for_downstream' : True,
    'email': 'sam.heruwala@gmail.com',
    'start_date' : datetime(2021,12,06),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

project_path = "s3://<hellofresh-assessment>//main.py"
input_data_path = "s3://<data>.json"
output_data_path = "s3://<path>//"


JOB_FLOW_OVERRIDES = {
    "Name": "hellofresh-assessment",
    "ReleaseLabel": "emr-5.20.0",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "r4.2xlarge",
                "EbsConfiguration": {"EbsBlockDeviceConfigs": [
                    {"VolumeSpecification": {"SizeInGB": 20, "VolumeType": "gp2"}, "VolumesPerInstance": 1}]},
                "InstanceCount": 1
            }
        ],
        "Ec2KeyName": "key",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "HadoopVersion": "Amazon 2.7.3",
        "Ec2SubnetId": "subnet-0928c0defe9f4e4a0",
        "EmrManagedMasterSecurityGroup": "sg-0e342a74d2d9af22e",
        "EmrManagedSlaveSecurityGroup": "sg-0096376d2261b243e",
        "ServiceAccessSecurityGroup": "sg-041ae55a2ecf12672"
    },

    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
    "Configurations": [{
        "Classification": "yarn-site",
        "Properties": {
            "yarn.nodemanager.vmem-check-enabled": "false"
        }
    },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.executor.memory": "5G",
                "spark.yarn.executor.memoryOverhead": "384",
                "spark.driver.memory": "3G",
                "spark.executor.cores": "3",
                "spark.executor.instances": "2"
            }
        }
    ],
    "Tags": [{
        "Key": "costCenter",
        "Value": "PROD"
    }, {
        "Key": "productName",
        "Value": "assessment"
    }]
}

dag = DAG(dag_name, default_args=args, max_active_runs=1, schedule_interval='0 0 * * *')

def spark_step1(project_path):
    spark_step = [{
        "Name": "spark_step",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["/usr/bin/spark-submit", "--driver-class-path",
                     ":/home/hadoop/lib/*:/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/lib/hadoop/client/jersey-client.jar:/usr/lib/hadoop/client/jersey-core.jar:/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
                     "--deploy-mode",
                     "cluster",
                     "--master",
                     "yarn",
                     "--conf",
                     "spark.shuffle.registration.timeout=60m",
                     project_path,
                     "--step preProcess",
                     "--input " + data_path,
                     "--output pre_processed"]
        }
    }]
    return spark_step

def spark_step2(project_path):
    spark_step = [{
        "Name": "spark_step",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["/usr/bin/spark-submit", "--driver-class-path",
                     ":/home/hadoop/lib/*:/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/lib/hadoop/client/jersey-client.jar:/usr/lib/hadoop/client/jersey-core.jar:/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
                     "--deploy-mode",
                     "cluster",
                     "--master",
                     "yarn",
                     "--conf",
                     "spark.shuffle.registration.timeout=60m",
                     project_path,
                     "--step transform",
                     "--input pre_processed",
                     "--output " + output_data_path]
        }
    }]
    return spark_step

###################################################################
# S3 file sensor
s3_sensor_raw = S3KeySensor(
    task_id='s3_key_sensor_raw',
    poke_interval=60,
    timeout=60,
    bucket_key= data_path,
    bucket_name=None,
    wildcard_match=True,
    aws_conn_id='xxx',
    dag=dag)

# Cluster creator to run spark job
cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='xxx',
    emr_conn_id='xxx',
    retries=3,
    dag=dag)

# Spark step to run job on EMR
spark_step1 = EmrAddStepsOperator(
    task_id="spark_step1",
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    steps=spark_step1(project_path),
    trigger_rule=TriggerRule.ALL_SUCCESS,
    aws_conn_id='xxx',
    dag=dag)

# Spark step to run job on EMR
spark_step2 = EmrAddStepsOperator(
    task_id="spark_step2",
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    steps=spark_step2(project_path),
    trigger_rule=TriggerRule.ALL_SUCCESS,
    aws_conn_id='xxx',
    dag=dag)

# Spark step sensor
lastStepChecker = EmrStepSensor(
    task_id='lastStepChecker',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('spark_step', key='return_value')[0] }}",
    poke_interval=300,
    aws_conn_id='xxx',
    dag=dag)

# Cluster remover
cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    trigger_rule=TriggerRule.ALL_DONE,
    aws_conn_id='xxx',
    dag=dag)

# Function to update all step status in case of failure
def _finally(**kwargs):
    tasks = kwargs['dag_run'].get_task_instances()
    for task_instance in tasks:
        if task_instance.current_state() == State.FAILED and task_instance.task_id != kwargs['task_instance'].task_id:
            [i.set_state("failed") for i in tasks]
            raise Exception("Task "+task_instance.task_id+" failed. Failing this DAG run.")

# Last step to make sure all step ran successfully
finally_ = PythonOperator(
    task_id="finally",
    python_callable=_finally,
    trigger_rule=TriggerRule.ALL_DONE,
    provide_context=True,
    dag=dag)

# Job flow
s3_sensor_raw >> cluster_creator >> spark_step1 >> spark_step2 >> lastStepChecker >> cluster_remover >> finally_
