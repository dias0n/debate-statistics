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

# БД
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

    # Уже обработанные резолюции
    cur.execute("SELECT Motion_ID FROM Motions WHERE Tournament_Slug = %s;", (tournament_slug,))
    processed_motions = {row[0] for row in cur.fetchall()}  # Множество ID обработанных резолюций

    # Резолюции турнира
    url = f"{API_URL}/tournaments/{tournament_slug}/motions"
    headers = {"Authorization": f"Token {TOKEN}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        motions = response.json()

        for motion in motions:
            motion_id = motion["id"]

            # Пропускаем уже обработанные резолюции
            if motion_id in processed_motions:
                print(f"⚠️ Резолюция {motion_id} уже добавлена, пропускаем...")
                continue

            info_slide_plain = motion["info_slide_plain"]
            motion_text = motion["text"]

            # Иногда эти значения могут быть неизвестными. Избегаем ошибок кода
            round_number = None
            round_name = None

            # Проверяем, есть ли информация о раунде
            if "rounds" in motion and motion["rounds"]:
                round_id = motion["rounds"][0]["round"].split("/")[-1]  # Достаём ID раунда
                round_url = f"{API_URL}/tournaments/{tournament_slug}/rounds/{round_id}"
                round_response = requests.get(round_url, headers=headers)

                if round_response.status_code == 200:
                    round_data = round_response.json()
                    round_number = round_data.get("seq", None)  # Номер раунда
                    round_name = round_data.get("name", None)  # Название раунда

            # Вставляем данные в таблицу
            cur.execute("""
                INSERT INTO Motions (Motion_ID, Tournament_Slug, Info_Slide_Plain, Motion_Text, Round_Number, Round_Name)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (motion_id, tournament_slug, info_slide_plain, motion_text, round_number, round_name))

        conn.commit()
        print(f"Данные о резолюциях для {tournament_slug} успешно сохранены!")

    else:
        print(f"Ошибка запроса ({response.status_code}): {response.text}")

cur.close()
conn.close()








