import os
import psycopg2
import requests
from dotenv import load_dotenv

# Загружаем переменные окружения
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

# Получаем список всех турниров
cur.execute("SELECT Tournament_Slug FROM Tournaments;")
tournaments = cur.fetchall()

for tournament in tournaments:
    tournament_slug = tournament[0]

    # Запрашиваем команды турнира
    teams_url = f"{API_URL}/tournaments/{tournament_slug}/teams"
    headers = {"Authorization": f"Token {TOKEN}"}
    teams_response = requests.get(teams_url, headers=headers)

    if teams_response.status_code == 200:
        teams = teams_response.json()

        for team in teams:
            team_id = team["id"]
            team_name = team["reference"]
            speaker_urls = team["speakers"]

            speaker_names = []
            for speaker in speaker_urls[:2]:  # Берём максимум 2 спикера
                speaker_url = speaker["url"]
                speaker_response = requests.get(speaker_url, headers=headers)
                if speaker_response.status_code == 200:
                    speaker_data = speaker_response.json()
                    speaker_names.append(speaker_data["name"])

            # Разделяем спикеров на отдельные колонки
            speaker_1 = speaker_names[0] if len(speaker_names) > 0 else None
            speaker_2 = speaker_names[1] if len(speaker_names) > 1 else None

            # Записываем в базу
            cur.execute("""
                INSERT INTO Teams (Team_ID, Tournament_Slug, Team_Name, Speaker_1, Speaker_2)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (Team_ID) DO NOTHING;
            """, (team_id, tournament_slug, team_name, speaker_1, speaker_2))

        conn.commit()
        print(f"Данные о командах для {tournament_slug} успешно сохранены!")

    else:
        print(f"Ошибка при получении команд {tournament_slug}: {teams_response.status_code}")

cur.close()
conn.close()

