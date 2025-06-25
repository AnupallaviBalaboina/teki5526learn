import datetime
import uuid
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context
from dependencies import trigger_helper
from dependencies import variables
from dependencies.dataflow_template import *

# start params
OWNER = 'Surya'
DAG_ID = 'criteo_ssp_tables_bulk_dump_v1'
SCHEDULE_INTERVAL = None
RETRY_EXPONENTIAL_BACKOFF = True
START_DATE = datetime.datetime(2022, 1, 1)
END_DATE = None
DEPENDS_ON_PAST = False
CATCHUP = False
NUM_RETRIES = 5
POOL = 'sponsored'
DESCRIPTION = 'Load json from gcs to BQ in criteo'
# PORTAL_DAG_ID = 'criteo_postgres_ssp_keyword_v1'
VERBOSE = True
# end params

def update_payload():
    context = get_current_context()
    trigger_date = context["dag_run"].conf["trigger_date"]
    start_date = context["dag_run"].conf["trigger_info"]['start_date']
    end_date = context["dag_run"].conf["trigger_info"]['end_date']
    payload = {
        "trigger_date": trigger_date,
        "trigger_info": {
            "dag_id": DAG_ID,
            "now": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "start_date": start_date,
            "end_date": end_date,
        },
    }
    return json.dumps(payload)


def get_params(**kwargs):
    date = trigger_helper.get_trigger_date(kwargs)
    uuid_id = trigger_helper.get_value(kwargs,value = 'uuid_id')
    bucket = variables.values['criteo_keyword_bucket_name']
    params = {
        "keyword" : 'gs://{bucket}/{uuid_id}/keyword.json'.format(bucket = bucket,uuid_id = uuid_id),
        "pagetype" : 'gs://{bucket}/{uuid_id}/product_pagetype_environment.json'.format(bucket = bucket,uuid_id = uuid_id)
    }
    print('params is {p}'.format(p = params))
    return params


default_args = {
            'owner': OWNER,
            'start_date': START_DATE,
            'end_date': END_DATE,
            'retries': NUM_RETRIES,
            'retry_delay': datetime.timedelta(minutes=1),
            'depends_on_past':DEPENDS_ON_PAST,
            'retry_exponential_backoff': RETRY_EXPONENTIAL_BACKOFF,
            'execution_timeout':datetime.timedelta(hours=5),
            }

with DAG(DAG_ID,
                default_args=default_args, 
                schedule_interval = SCHEDULE_INTERVAL,
                description = DESCRIPTION,
                catchup = CATCHUP,
                ) as dag:

    task_get_params = PythonOperator(
            task_id='get_params',
        provide_context=True,
        python_callable=get_params,
        pool = POOL,
        )
    
    task_load_staging_table_keyword = DataflowTemplatedJobStartOperator(
                task_id='load_criteo_keyword_table',
                template =  '{bucket}/ssp_keyword_v1'.format(bucket = variables.values['dataflow_template_bucket']),
                parameters={
            'files':'{{ ti.xcom_pull(task_ids="get_params")["keyword"] }}'
            },
                dataflow_default_options = DATAFLOW_DEFAULT_OPTIONS,
                location=location,
            pool = POOL,
            )
    
    task_load_staging_table_pagetype= DataflowTemplatedJobStartOperator(
                task_id='load_criteo_pagetype_table',
                template =  '{bucket}/ssp_product_pagetype_environment_v1'.format(bucket = variables.values['dataflow_template_bucket']),
                parameters={
            'files':'{{ ti.xcom_pull(task_ids="get_params")["pagetype"] }}'
            },
                dataflow_default_options = DATAFLOW_DEFAULT_OPTIONS,
                location=location,
            pool = POOL,
            )
    
    # task_dag_trigger_payload = PythonOperator(
    #     task_id='dag_trigger_payload',
    #     python_callable=update_payload,
    #     pool = POOL,
    #     dag=dag
    #     )

    # task_trigger_portal_tables = TriggerDagRunOperator(
    #         task_id='trigger_portal_tables',
    #         trigger_dag_id=PORTAL_DAG_ID,
    #         conf = "{{ti.xcom_pull('dag_trigger_payload')}}",
    #         pool = POOL,
    #         )

task_get_params >> [task_load_staging_table_keyword, task_load_staging_table_pagetype]
# task_dag_trigger_payload >> task_trigger_portal_tables
