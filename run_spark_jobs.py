
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from datetime import datetime

# Configurations
BUCKET_MAIN = "prosodies"
BUCKET_BRONZE = "prosodies-bronze"
BUCKET_SILVER = "prosodies-silver"
BUCKET_GOLD = "prosodies-gold"
KEY_SILVER = "textSilver.parquet"
KEY_GOLD = "textGold.parquet"
KEY_STRESSDICT = "stress-dict/stressDict.parquet"
SOUNDOUT_FILEPATH = "/usr/local/lib/python3.6/site-packages/soundout.py"
BOOTSTRAP_SCRIPT = "pincelate.sh"
S3_SCRIPT_PATH = "scripts"
S3_JAR_PATH = "jars"
SPARK_ENGINE = "spark-engine_2.11-0.0.1.jar"

def get_dir(**context):
    """ Get dir within bronze bucket from Lambda and push to xcom """
    input_key = context["dag_run"].conf["key"]
    context["ti"].xcom_push(key = "input_key", value = input_key)

# EMR Spark
SPARK_STEPS = [
    {
        "Name": "Copy executable files from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT", 
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_MAIN }}/{{ params.S3_JAR_PATH }}",
                "--dest=/source",
            ],
        },
    },
    {
        "Name": "Convert literary text files to parquets",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "client",
                "--class", "Driver.MainApp",
                "hdfs:///source/{{ params.SPARK_ENGINE }}",
                "-p", "prosodies",
                "-i", "Text",
                "-o", "parquet",
                "-s", "s3://{{ params.BUCKET_BRONZE }}/{{ task_instance.xcom_pull(task_ids='get_dir_task', key='input_key') }}",
                "-d", "s3://{{ params.BUCKET_GOLD }}/{{ params.KEY_GOLD }}",
                "-m", "append",
                "-r", "s3://{{ params.BUCKET_MAIN }}/{{ params.KEY_STRESSDICT }}",
                "-x", "{{ params.SOUNDOUT_FILEPATH }}",
                "-y", "s3://{{ params.BUCKET_SILVER }}/{{ params.KEY_SILVER }}",
                "--input-options", "wholeText=true"
            ],
        },
    }
]


JOB_FLOW_OVERRIDES = {
    "Name": "Prosody",
    "ReleaseLabel": "emr-5.28.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3",  "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"},
                },
            ],

        },
    ],
    'BootstrapActions':[{
        'Name': 'Install',
        'ScriptBootstrapAction': {
            'Path': "s3://{{ params.BUCKET_MAIN }}/{{ params.S3_SCRIPT_PATH }}/{{ params.BOOTSTRAP_SCRIPT }}",
        }
    }],
    "Instances": {
        "Ec2KeyName": "prosody",
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://prosodies/log",
    "VisibleToAllUsers": True
}

# define DAG
dag = DAG("run_spark_jobs",
        schedule_interval=None, 
        start_date=datetime(2022, 5, 26), 
        catchup=False
        )

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)
get_dir_operator = PythonOperator(task_id='get_dir_task', provide_context=True, python_callable=get_dir, dag=dag)

# Instantiate the AWS EMR Cluster 
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    params = {
        "BUCKET_MAIN": BUCKET_MAIN,
        "S3_SCRIPT_PATH": S3_SCRIPT_PATH,
        "BOOTSTRAP_SCRIPT": BOOTSTRAP_SCRIPT
    },
    dag=dag
)

# Add the steps once the cluster is up 
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    provide_context=True,
    params={
        "BUCKET_MAIN": BUCKET_MAIN,
        "BUCKET_BRONZE": BUCKET_BRONZE,
        "BUCKET_SILVER": BUCKET_SILVER,
        "BUCKET_GOLD": BUCKET_GOLD,
        "KEY_SILVER": KEY_SILVER,
        "KEY_GOLD": KEY_GOLD,
        "KEY_STRESSDICT": KEY_STRESSDICT,
        "SOUNDOUT_FILEPATH": SOUNDOUT_FILEPATH,
        "S3_JAR_PATH": S3_JAR_PATH,
        "SPARK_ENGINE": SPARK_ENGINE
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# temrinate the cluster once steps/tasks are completed 
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    trigger_rule="all_done",
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)


start_operator >> get_dir_operator >> create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster >> end_data_pipeline



