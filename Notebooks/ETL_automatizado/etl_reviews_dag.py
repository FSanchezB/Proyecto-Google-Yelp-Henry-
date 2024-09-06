from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import storage
from google.cloud import bigquery
from datetime import timedelta
import pandas as pd
from datetime import datetime

def process_and_load_data():
    BUCKET_NAME = 'datos_crudos_review'
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)
    
    # Importamos los archivos desde Cloud Storage
    json_blob = [blob.name for blob in bucket.list_blobs() if '.json' in blob.name]
    df_final = pd.DataFrame()

    for blob_name in json_blob:
        blob = bucket.blob(blob_name)
        with blob.open('rb') as f:
            df = pd.read_json(f, lines=True, nrows=100)
            df_final = pd.concat([df_final, df]).reset_index(drop=True)

    reviews = df_final.drop(['review_id','user_id'],axis=1)
    reviews['date'] = pd.to_datetime(reviews['date'])
    reviews=reviews.loc[reviews['date']>='2013-01-01']
    reviews.drop(['useful','funny','cool'],axis=1,inplace=True)
    reviews['text']=reviews['text'].drop_duplicates()
    reviews['text'] = reviews['text'].astype(str)
    reviews['text'] = reviews['text'].apply(lambda x: x.encode('unicode_escape').decode('utf-8'))
    reviews['text'] = reviews['text'].astype(str)

    # Cargamos el DataFrame a BigQuery
    table_full_id = 'xenon-mantis-431402-j8.datos_crudos.reviews'

    job_config = bigquery.LoadJobConfig( autodetect=True,
                                        source_format=bigquery.SourceFormat.CSV,
                                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = bq_client.load_table_from_dataframe(reviews,
                                            table_full_id,
                                            job_config=job_config)
    job.result()

    print("Carga completada")

def_args = {
    "owner": "fabian",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date":datetime(2023,1,1)
}

with DAG ("ex_reviews",
          default_args = def_args,
          catchup=False) as dag:
    
    start = DummyOperator(task_id = "START")

    e = PythonOperator(
        task_id = "extract_transform_Load",
        python_callable = process_and_load_data
    )

    end = DummyOperator(task_id = "END")

start >> e >> end