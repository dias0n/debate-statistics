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

# Подключение к БД
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

headers = {"Authorization": f"Token {TOKEN}"}

# Список всех турниров
cur.execute("SELECT Tournament_Slug FROM Tournaments;")
tournaments = cur.fetchall()

for tournament in tournaments:
    tournament_slug = tournament[0]

    # Институты турнира
    institutions_url = f"{API_URL}/tournaments/{tournament_slug}/institutions"
    response_institutions = requests.get(institutions_url, headers=headers)

    if response_institutions.status_code == 200:
        institutions = response_institutions.json()

        for institution in institutions:
            institution_id = institution["id"]
            institution_name = institution["name"]
            institution_code = institution["code"]

            # Проверяем, есть ли институт в таблице
            cur.execute("SELECT COUNT(*) FROM Institutions WHERE Institution_ID = %s;", (institution_id,))
            already_exists = cur.fetchone()[0] > 0

            if already_exists:
                print(f"Институт {institution_name} ({institution_code}) уже в базе, пропускаем.")
                continue

            # Добавляем новый институт
            cur.execute("""
                INSERT INTO Institutions (Institution_ID, Institution_Name, Institution_Code)
                VALUES (%s, %s, %s);
            """, (institution_id, institution_name, institution_code))

            print(f"Добавлен институт {institution_name} ({institution_code})")

    else:
        print(f"Ошибка при получении институтов для {tournament_slug}: {response_institutions.status_code}")

    # Сохраняем изменения после каждого турнира
    conn.commit()

cur.close()
conn.close()
print("Обновление институтов завершено!")
