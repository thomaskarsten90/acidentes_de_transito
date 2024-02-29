from urllib import request
import pandas as pd
import zipfile
import psycopg2
import os
import requests
import subprocess

TOKEN = '6762803974:AAGBW61uWEbPUOdloXybxesOMMBYpuKKOiM'
chat_id = '1633695887'

def send_message(chat_id, text):
    url = 'https://api.telegram.org/bot{}/'.format(TOKEN)
    url = url + 'sendMessage?chat_id={}'.format(chat_id)

    r = requests.post(url, json={'text': text})

    return None

files_to_delete = ['datatran.zip', '/home/thomas-linux/acidentes_de_transito/datatran2023.csv']

for file_path in files_to_delete:
    os.path.exists(file_path)
    os.remove(file_path)
    send_message(chat_id, f"{file_path} excluído com sucesso")

url = 'https://drive.usercontent.google.com/u/0/uc?id=1-WO3SfNrwwZ5_l7fRTiwBKRw7mi1-HUq&export=download'

file = 'datatran.zip'

request.urlretrieve(url, file)

send_message(chat_id, "Arquivo Baixado")

zip_file = 'datatran.zip'

extract_dir = '/home/thomas-linux/acidentes_de_transito'

with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

send_message(chat_id, 'Arquivo descompactado com sucesso')

df = pd.read_csv('/home/thomas-linux/acidentes_de_transito/datatran2023.csv', encoding='latin1', sep=';')

docker_command = "docker start datatran"

try:
    subprocess.run(docker_command, shell=True, check=True)
    send_message(chat_id,"Contêiner do banco de dados PostgreSQL iniciado com sucesso!")
except subprocess.CalledProcessError as e:
    send_message(chat_id,'Erro ao iniciar o contêiner do banco de dados PostgreSQL:', e)


conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="202308",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

send_message(chat_id,"Acesso ao banco OK")

cur.execute("TRUNCATE TABLE datatran")

send_message(chat_id,"Tabela truncada com sucesso")

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

send_message(chat_id,"Tabela criada com sucesso")

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

send_message(chat_id,"Tabela populada com sucesso")

cur.close()
conn.close()