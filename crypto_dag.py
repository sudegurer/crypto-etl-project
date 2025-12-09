from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from datetime import datetime
from fetch_crypto_data import fetch_data
from transform_data import transform_data
from load_data import load_to_postgres

# 1. DAG ARGÜMANLARI
default_args = {
    'owner': 'sude',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# 2. DAG TANIMLAMA
with DAG(
    dag_id='crypto_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Her gün çalışır
    catchup=False,
    tags=['crypto', 'etl'],
) as dag:
    
    # 3. GÖREVLERİ TANIMLAMA
    
    # a. Extract (Veri Çekme) Görevi
    fetch_task = PythonOperator(
        task_id='fetch_crypto_data_from_api',
        python_callable=fetch_data,  # fetch_crypto_data.py dosyasındaki fonksiyon
    )

    # b. Transform (Dönüştürme) Görevi
    transform_task = PythonOperator(
        task_id='transform_crypto_data',
        python_callable=transform_data,  # transform_data.py dosyasındaki fonksiyon
    )

    # c. Load (Yükleme) Görevi
    load_task = PythonOperator(
        task_id='load_to_postgres_db',
        python_callable=load_to_postgres,  # load_data.py dosyasındaki fonksiyon
    )

    # 4. GÖREV BAĞIMLILIKLARINI TANIMLAMA
    # Akış: Çek -> Dönüştür -> Yükle
    fetch_task >> transform_task >> load_task
