from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import subprocess
import os
import pandas as pd
import zipfile
import psycopg2
from urllib import request

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 7),  # Data em que deseja que a DAG comece a ser executada
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

TOKEN = '6762803974:AAGBW61uWEbPUOdloXybxesOMMBYpuKKOiM'
chat_id = '1633695887'

def send_message(chat_id, text):
    url = 'https://api.telegram.org/bot{}/'.format(TOKEN)
    url = url + 'sendMessage?chat_id={}'.format(chat_id)

    r = requests.post(url, json={'text': text})
    print('Status Code {}'.format(r.status_code))

    return None

def download_and_extract_data():
    files_to_delete = ['datatran.zip', '/home/thomas-linux/acidentes_de_transito/datatran2023.csv']

    for file_path in files_to_delete:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"{file_path} excluído com sucesso")
        else:
            print(f"{file_path} não encontrado")

    url = 'https://drive.usercontent.google.com/u/0/uc?id=1-WO3SfNrwwZ5_l7fRTiwBKRw7mi1-HUq&export=download'

    file = 'datatran.zip'

    request.urlretrieve(url, file)

    print("Arquivo Baixado")
    send_message(chat_id, "Arquivo Baixado")

    zip_file = 'datatran.zip'

    extract_dir = '/home/thomas-linux/acidentes_de_transito'

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    print('Arquivo descompactado com sucesso')
    send_message(chat_id, 'Arquivo descompactado com sucesso')

def start_postgres_container():
    docker_command = "docker start banco_acidentes_postgres"
    try:
        subprocess.run(docker_command, shell=True, check=True)
        send_message(chat_id, "Contêiner do banco de dados PostgreSQL iniciado com sucesso!")
    except subprocess.CalledProcessError as e:
        send_message(chat_id, 'Erro ao iniciar o contêiner do banco de dados PostgreSQL:', e)

def populate_postgres_table():
    df = pd.read_csv('/home/thomas-linux/acidentes_de_transito/datatran2023.csv', encoding='latin1', sep=';')

    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="202308",
        host="localhost",
        port="5432"
    )

    cur = conn.cursor()

    print("Acesso ao banco OK")
    send_message(chat_id, "Acesso ao banco OK")

    cur.execute("TRUNCATE TABLE datatran")

    print("Tabela truncada com sucesso")
    send_message(chat_id, "Tabela truncada com sucesso")

    create_table_query = """
        CREATE TABLE IF NOT EXISTS datatran (
            id SERIAL PRIMARY KEY,
            data_inversa DATE,
            dia_semana VARCHAR(100),
            horario TIME,
            uf VARCHAR(100),
            br VARCHAR(100),
            km NUMERIC,
            municipio VARCHAR(1000),
            causa_acidente VARCHAR(1000),
            tipo_acidente VARCHAR(1000),
            classificacao_acidente VARCHAR(1000),
            fase_dia VARCHAR(1000),
            sentido_via VARCHAR(1000),
            condicao_metereologica VARCHAR(1000),
            tipo_pista VARCHAR(1000),
            tracado_via VARCHAR(1000),
            uso_solo VARCHAR(1000),
            pessoas BIGINT,
            mortos BIGINT,
            feridos_leves BIGINT,
            feridos_graves BIGINT,
            ilesos BIGINT,
            ignorados BIGINT,
            feridos BIGINT,
            veiculos BIGINT,
            latitude VARCHAR(100),
            longitude VARCHAR(100),
            regional VARCHAR(1000),
            delegacia VARCHAR(1000),
            uop VARCHAR(1000)
    )
    """

    cur.execute(create_table_query)

    print("Tabela criada com sucesso")
    send_message(chat_id, "Tabela criada com sucesso")

    numeric_columns = ['km', 'latitude', 'longitude']
    df[numeric_columns] = df[numeric_columns].replace(',', '.', regex=True)

    for index, row in df.iterrows():
        cur.execute("""
                    INSERT INTO datatran (id, data_inversa, dia_semana, horario, uf, br, km, municipio, causa_acidente,
                                   tipo_acidente, classificacao_acidente, fase_dia, sentido_via, condicao_metereologica,
                                   tipo_pista, tracado_via, uso_solo, pessoas, mortos, feridos_leves, feridos_graves,
                                   ilesos, ignorados, feridos, veiculos, latitude, longitude, regional, delegacia, uop)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s)
        """, row)

    conn.commit()

    print("Tabela populada com sucesso")
    send_message(chat_id, "Tabela populada com sucesso")

    cur.close()
    conn.close()

with DAG('acidentes_de_transito_dag',
         default_args=default_args,
         schedule_interval='0 16 * * 5',  # Executar às 16:00 hrs nas sextas-feiras
         catchup=False) as dag:

    download_and_extract_data_task = PythonOperator(
        task_id='download_and_extract_data',
        python_callable=download_and_extract_data
    )

    start_postgres_container_task = PythonOperator(
        task_id='start_postgres_container',
        python_callable=start_postgres_container
    )

    populate_postgres_table_task = PythonOperator(
        task_id='populate_postgres_table',
        python_callable=populate_postgres_table
    )

    download_and_extract_data_task >> start_postgres_container_task >> populate_postgres_table_task
