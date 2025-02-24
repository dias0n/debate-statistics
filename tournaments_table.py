import os
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

API_URL = os.getenv("API_URL")
TOKEN = os.getenv("TOKEN_AD")

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

cur = conn.cursor()

headers = {'Authorization': f'Token {TOKEN}'}
response = requests.get(f'{API_URL}/tournaments', headers=headers)

if response.status_code == 200:
    tournaments = response.json()

    for tournament in tournaments:
        tournament_id = tournament['id']
        tournament_name = tournament['name']
        tournament_slug = tournament['slug']

        cur.execute("""
            INSERT INTO Tournaments(Tournament_ID, Tournament_Name, Tournament_Slug)
            VALUES(%s, %s, %s)
            ON CONFLICT(Tournament_ID) DO UPDATE 
            SET Tournament_Name = EXCLUDED.Tournament_Name,
                Tournament_Slug = EXCLUDED.Tournament_Slug;
        """, (tournament_id, tournament_name, tournament_slug))

    conn.commit()
    print('Данные успешно сохранены!')


else:
    print(f"Ошибка запроса: {response.status_code}")



cur.close()
conn.close()