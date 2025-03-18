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

# БД
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

    # Проверяем, есть ли уже команды для этого турнира
    cur.execute("SELECT COUNT(*) FROM Teams WHERE Tournament_Slug = %s;", (tournament_slug,))
    teams_exist = cur.fetchone()[0] > 0

    # Если есть - скипаем
    if teams_exist:
        print(f"Команды для {tournament_slug} уже добавлены, пропускаем...")
        continue

    # Команды турнира
    teams_url = f"{API_URL}/tournaments/{tournament_slug}/teams"
    teams_response = requests.get(teams_url, headers=headers)

    if teams_response.status_code != 200:
        print(f"Ошибка при получении команд {tournament_slug}: {teams_response.status_code}")
        continue

    teams = teams_response.json()

    for team in teams:
        team_id = team["id"]
        team_name = team["reference"]

        # Спикеры команд
        speaker_urls = team.get("speakers", [])
        speaker_names = [None, None]

        for i, speaker in enumerate(speaker_urls[:2]):  # Берём максимум 2 спикера
            if isinstance(speaker, dict) and "name" in speaker:
                speaker_names[i] = speaker["name"]

        speaker_1 = speaker_names[0]
        speaker_2 = speaker_names[1]

        # Логи в консоль
        print(f"Добавляем команду {team_name} ({team_id}) в {tournament_slug}")
        print(f"Спикер 1: {speaker_1}")
        print(f"Спикер 2: {speaker_2}")

        # Если команда уже есть - обновляем, если нет - добавляем
        cur.execute("""
            INSERT INTO Teams (Team_ID, Tournament_Slug, Team_Name, Speaker_1, Speaker_2)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (Team_ID) 
            DO UPDATE SET 
                Team_Name = EXCLUDED.Team_Name,
                Speaker_1 = EXCLUDED.Speaker_1,
                Speaker_2 = EXCLUDED.Speaker_2;
        """, (team_id, tournament_slug, team_name, speaker_1, speaker_2))

    conn.commit()
    print(f"Данные о командах для {tournament_slug} успешно добавлены!")

cur.close()
conn.close()















