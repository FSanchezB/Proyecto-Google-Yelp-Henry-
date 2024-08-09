from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from google.cloud import storage, bigquery
import pandas as pd
import io
import numpy as np

# Parámetros de configuración
BUCKET_NAME = 'datos_crudos'
BIGQUERY_DATASET = 'db'
BIGQUERY_TABLE = 'metadata'


def extract_from_gcs():
    client = storage.Client()  # Crear el cliente
    bucket = client.bucket(BUCKET_NAME)  # Obtener el bucket

    data_frames = []
    
    for i in range(1, 12):  # Archivos del 1.json al 11.json
        blob_name = f'{i}.json'
        blob = bucket.blob(blob_name)
        json_data = blob.download_as_bytes()  # Descargar el archivo como bytes
        
        # Leer el JSON desde los bytes
        df = pd.read_json(io.BytesIO(json_data), lines=True, encoding='latin1')
        data_frames.append(df)

    # Concatenar todos los DataFrames
    df_metadata = pd.concat(data_frames, ignore_index=True)

    return df_metadata

def transform_data(df_metadata):
    # Aquí puedes aplicar transformaciones a tu DataFrame
    df_metadata=df_metadata.drop(['description','price','state', 'hours', 'MISC'],axis=1)
    df_metadata = df_metadata.dropna(subset=['name', 'category'])
    df_metadata['category']=df_metadata['category'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else '')
    df_metadata=df_metadata.fillna('')
    states = {
    'Alabama': (30.223, -88.473, 35.003, -84.889),
    'Alaska': (51.214, -179.148, 71.538, -129.994),
    'Arizona': (31.332, -114.818, 37.004, -109.045),
    'Arkansas': (33.003, -94.617, 36.499, -88.099),
    'California': (32.534, -124.848, 42.009, -114.131),
    'Colorado': (36.993, -109.045, 41.003, -102.041),
    'Connecticut': (40.993, -73.727, 42.046, -71.178),
    'Delaware': (38.451, -75.241, 39.839, -74.845),
    'Florida': (24.396, -87.634, 31.001, -80.031),
    'Georgia': (30.357, -85.605, 35.003, -80.837),
    'Hawaii': (18.776, -155.041, 28.208, -154.452),
    'Idaho': (41.991, -116.916, 49.002, -116.045),
    'Illinois': (36.970, -91.513, 42.508, -87.515),
    'Indiana': (36.577, -88.097, 41.760, -84.848),
    'Iowa': (40.375, -96.639, 43.501, -90.140),
    'Kansas': (36.993, -102.041, 40.003, -94.589),
    'Kentucky': (36.497, -89.571, 39.148, -81.969),
    'Louisiana': (28.928, -94.043, 33.019, -89.099),
    'Maine': (43.065, -71.087, 47.459, -66.934),
    'Maryland': (37.885, -79.487, 39.729, -75.051),
    'Massachusetts': (41.202, -73.508, 42.886, -69.928),
    'Michigan': (41.696, -90.418, 48.306, -82.020),
    'Minnesota': (43.501, -97.239, 49.384, -89.489),
    'Mississippi': (30.189, -91.650, 34.991, -88.097),
    'Missouri': (36.002, -95.774, 40.613, -89.098),
    'Montana': (44.358, -116.048, 49.001, -104.039),
    'Nebraska': (40.001, -104.053, 43.002, -95.308),
    'Nevada': (35.001, -120.005, 42.002, -114.039),
    'New Hampshire': (42.697, -72.557, 45.305, -70.540),
    'New Jersey': (39.733, -75.560, 41.357, -73.893),
    'New Mexico': (31.332, -114.818, 37.004, -106.616),
    'New York': (40.477, -74.259, 45.015, -71.157),
    'North Carolina': (33.842, -84.321, 36.588, -75.457),
    'North Dakota': (36.993, -104.045, 49.001, -96.563),
    'Ohio': (38.403, -84.820, 41.977, -80.518),
    'Oklahoma': (33.628, -103.002, 37.002, -94.430),
    'Oregon': (41.991, -124.848, 46.292, -116.916),
    'Pennsylvania': (39.719, -80.519, 42.272, -74.690),
    'Rhode Island': (41.146, -71.174, 41.822, -70.748),
    'South Carolina': (32.034, -83.354, 35.215, -78.570),
    'South Dakota': (42.461, -104.048, 45.945, -96.574),
    'Tennessee': (35.001, -90.310, 36.681, -81.645),
    'Texas': (25.837, -106.645, 36.500, -93.508),
    'Utah': (36.993, -114.046, 42.001, -102.041),
    'Vermont': (42.726, -73.438, 45.017, -71.382),
    'Virginia': (36.585, -83.675, 39.466, -75.239),
    'Washington': (45.543, -124.848, 49.002, -116.916),
    'West Virginia': (37.201, -82.644, 39.463, -80.518),
    'Wisconsin': (42.491, -92.888, 47.084, -87.026),
    'Wyoming': (41.003, -111.056, 45.001, -104.052),
    'Puerto Rico': (17.835, -68.119, 18.515, -65.402)  
}

    state_names = np.array(list(states.keys()))
    lat_min = np.array([v[0] for v in states.values()])
    lon_min = np.array([v[1] for v in states.values()])
    lat_max = np.array([v[2] for v in states.values()])
    lon_max = np.array([v[3] for v in states.values()])


    def get_state_vectorized(latitudes, longitudes):
        state_indices = np.zeros(len(latitudes), dtype=int)
        for i in range(len(states)):
            mask = (latitudes >= lat_min[i]) & (latitudes <= lat_max[i]) & (longitudes >= lon_min[i]) & (longitudes <= lon_max[i])
            state_indices[mask] = i
        return state_names[state_indices]

    if 'latitude' in df_metadata.columns and 'longitude' in df_metadata.columns:
        df_metadata['state'] = get_state_vectorized(df_metadata['latitude'].values, df_metadata['longitude'].values)
    else:
        raise ValueError("El DataFrame no contiene las columnas 'latitude' y 'longitude'")

    substrings=['Restaurants','Auto','restaurant','Hotel','Health','Dentist','Food','Shopping']
    pattern = '|'.join(substrings)
    google= df_metadata.loc[df_metadata['category'].str.contains(pattern)]
    return google

def load_to_bigquery(data_frame):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema=[
        bigquery.SchemaField("Id_Negocio", "STRING"),
        bigquery.SchemaField("Nombre", "STRING"),
        bigquery.SchemaField("Direccion", "STRING"),
        bigquery.SchemaField("Latitud", "FLOAT"),
        bigquery.SchemaField("Longitud", "FLOAT"),
        bigquery.SchemaField("Calificacion", "FLOAT"),
        bigquery.SchemaField("Id_Categoria", "INTEGER"),
        bigquery.SchemaField("Num_Reviews", "INTEGER"),
        bigquery.SchemaField("URL", "STRING"),
        bigquery.SchemaField("Result_Rel", "STRING"),
        bigquery.SchemaField("Id_Estado", "STRING"),

        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(
        data_frame,
        f'{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
        job_config=job_config,
    )
    job.result()  # Espera a que el trabajo termine

def etl_process():
    # Extraer datos
    data_frame = extract_from_gcs()
    
    # Transformar datos
    transformed_data = transform_data(data_frame)
    
    # Cargar datos
    load_to_bigquery(transformed_data)

# Define el DAG
default_args = {
    'owner': 'fabian',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'gcs_to_bigquery_etl_metadata',
    default_args=default_args,
    description='ETL pipeline from GCS to BigQuery',
    schedule_interval='@daily',  # Ajusta la frecuencia según sea necesario
)

start = DummyOperator(task_id='start', dag=dag)
etl = PythonOperator(
    task_id='etl_process',
    python_callable=etl_process,
    provide_context=True,
    dag=dag,
)
end = DummyOperator(task_id='end', dag=dag)

start >> etl >> end