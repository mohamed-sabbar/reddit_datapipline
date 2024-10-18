import praw
import configparser
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajoute le chemin vers ton pipeline ETL
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "etl_piplines"))
from etl import api_connection, subreddit, extract_data, transform_data, load_data
from load_database import insert_the_data

# Arguments par défaut pour le DAG
default_args = {
    'owner': "mohamed_sabbar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Fonction pour débuter le pipeline
def beginning():
    print("Beginning of the pipeline!")

# Fonction pour terminer le pipeline
def end():
    print("End of the pipeline!")

# Fonction pour traiter le subreddit
def subre(**kwargs):
    ti = kwargs['ti']
    resultat = ti.xcom_pull(task_ids="connect_api_task")
    
    if resultat is None:
        raise ValueError("No data received from the API connection.")
    
    # Renvoie les données pour la tâche suivante
    return subreddit(resultat)

# Fonction pour extraire les données
def extract(**kwargs):
    ti = kwargs['ti']
    resultat = ti.xcom_pull(task_ids="subreddit")
    
    if resultat is None:
        raise ValueError("No data received from subreddit processing.")
    
    return extract_data(resultat)

# Fonction pour transformer les données
def transform(**kwargs):
    ti = kwargs['ti']
    resultat = ti.xcom_pull(task_ids="extract_data")
    
    if resultat is None:
        raise ValueError("No data received from data extraction.")
    
    return transform_data(resultat)

# Fonction pour charger les données
def load(**kwargs):
    ti = kwargs['ti']
    resultat = ti.xcom_pull(task_ids="transform_data")
    
    if resultat is None:
        raise ValueError("No data received from data transformation.")
    
    return load_data(resultat)
def inser_data(**kwargs):
    try:
     ti=kwargs['ti']
     resultat=ti.xcom_pull(task_ids="load_data")
     print(f"le chemin {resultat}")
     df=pd.read_excel(resultat)
     print(type(resultat))
     return insert_the_data(df)
    except Exception as e:
        print(e)

# Définition du DAG
with DAG(
    dag_id="data_pipeline",
    default_args=default_args,
    description="Simple DAG for Reddit data pipeline",
    start_date=datetime(2024, 10, 2),
    catchup=False,
    schedule_interval='@daily'
) as dag:
    
    # Définition des tâches
    task_1 = PythonOperator(
        task_id="start_task",
        python_callable=beginning
    )

    task_2 = PythonOperator(
        task_id="connect_api_task",
        python_callable=api_connection
    )

    task_3 = PythonOperator(
        task_id="end_task",
        python_callable=end
    )

    task_4 = PythonOperator(
        task_id="subreddit",
        python_callable=subre,
        provide_context=True
    )

    task_5 = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
        provide_context=True
    )

    task_6 = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
        provide_context=True
    )

    task_7 = PythonOperator(
        task_id="load_data",
        python_callable=load,
        provide_context=True
    )
    task_8=PythonOperator(
        task_id="load_data_in_database",
        python_callable=inser_data,
        provide_context=True

    )

    # Définition des dépendances entre les tâches
    task_1 >> task_2 >> task_4 >> task_5 >> task_6 >> task_7 >> task_8 >> task_3
