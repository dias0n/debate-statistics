import os
import psycopg2
import requests
from dotenv import load_dotenv

# 🔹 Загружаем переменные окружения
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

API_URL = os.getenv("API_URL")
TOKEN = os.getenv("TOKEN_AD")

# 🔹 Подключение к БД
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

cur = conn.cursor()

# Получаем список всех турниров из базы
cur.execute("SELECT Tournament_Slug FROM Tournaments;")
tournaments = cur.fetchall()

for tournament in tournaments:
    tournament_slug = tournament[0]  # Достаём slug турнира

    # Делаем запрос к API на получение резолюций
    url = f"{API_URL}/tournaments/{tournament_slug}/motions"
    headers = {"Authorization": f"Token {TOKEN}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        motions = response.json()

        for motion in motions:
            motion_id = motion["id"]
            info_slide_plain = motion["info_slide_plain"]
            motion_text = motion["text"]

            # 🔹 Инициализируем переменные заранее
            round_number = None
            round_name = None

            # 🔹 Проверяем, есть ли информация о раунде
            if "rounds" in motion and motion["rounds"]:
                round_id = motion["rounds"][0]["round"].split("/")[-1]  # Достаём ID раунда
                round_url = f"{API_URL}/tournaments/{tournament_slug}/rounds/{round_id}"
                round_response = requests.get(round_url, headers=headers)

                if round_response.status_code == 200:
                    round_data = round_response.json()
                    round_number = round_data.get("seq", None)  # Получаем номер раунда
                    round_name = round_data.get("name", None)  # Получаем название раунда


            cur.execute("""
                INSERT INTO Motions (Motion_ID, Tournament_Slug, Info_Slide_Plain, Motion_Text, Round_Number, Round_Name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(Motion_ID) DO NOTHING;
            """, (motion_id, tournament_slug, info_slide_plain, motion_text, round_number, round_name))

        conn.commit()
        print(f"✅ Данные о резолюциях для {tournament_slug} успешно сохранены!")

    else:
        print(f"❌ Ошибка запроса ({response.status_code}): {response.text}")

cur.close()
conn.close()







