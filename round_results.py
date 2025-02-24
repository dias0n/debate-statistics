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

# 🔹 Авторизационный заголовок
headers = {"Authorization": f"Token {TOKEN}"}

# 🔹 Подключение к БД
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# 🔹 Получаем список всех турниров
cur.execute("SELECT Tournament_Slug FROM Tournaments;")
tournaments = cur.fetchall()

for tournament in tournaments:
    tournament_slug = tournament[0]

    # 🔹 Получаем список всех раундов **только для этого турнира**
    rounds_url = f"{API_URL}/tournaments/{tournament_slug}/rounds"
    response_rounds = requests.get(rounds_url, headers=headers)

    if response_rounds.status_code != 200:
        print(f"❌ Ошибка запроса раундов {rounds_url}: {response_rounds.status_code}")
        continue

    rounds_data = response_rounds.json()
    round_mapping = {round["seq"]: round["id"] for round in rounds_data}

    for round_seq, round_id in round_mapping.items():  # ✅ Теперь id начинается с 1 для каждого нового турнира
        round_name = next(r["name"] for r in rounds_data if r["id"] == round_id)

        print(f"📊 Обрабатываем {round_name} (Раунд {round_seq}) для турнира {tournament_slug}")

        # 🔹 Получаем пары команд в этом раунде
        pairings_url = f"{API_URL}/tournaments/{tournament_slug}/rounds/{round_seq}/pairings"
        response_pairings = requests.get(pairings_url, headers=headers)

        if response_pairings.status_code != 200:
            print(f"❌ Ошибка запроса пар {pairings_url}: {response_pairings.status_code}")
            continue

        pairings = response_pairings.json()

        for pairing in pairings:
            pairing_id = pairing["id"]
            venue_url = pairing.get("venue", None)
            venue_name = "Неизвестно"

            # 🔹 Получаем название комнаты
            if venue_url:
                venue_id = venue_url.split("/")[-1]
                venue_info_url = f"{API_URL}/tournaments/{tournament_slug}/venues/{venue_id}"
                response_venue = requests.get(venue_info_url, headers=headers)

                if response_venue.status_code == 200:
                    venue_name = response_venue.json().get("name", "Неизвестно")

            # 🔹 Получаем **только судей, судивших этот раунд**
            adjudicators_data = pairing.get("adjudicators", {})
            adjudicator_names = []

            for role in ["chair", "panellists", "trainees"]:
                adj_list = adjudicators_data.get(role, [])
                if isinstance(adj_list, str):
                    adj_list = [adj_list]

                for adj_url in adj_list:
                    adj_id = adj_url.split("/")[-1]
                    adj_info_url = f"{API_URL}/tournaments/{tournament_slug}/adjudicators/{adj_id}"
                    response_adj = requests.get(adj_info_url, headers=headers)

                    if response_adj.status_code == 200:
                        adjudicator_names.append(response_adj.json().get("name", f"Судья {adj_id}"))

            adjudicators_str = ", ".join(adjudicator_names) if adjudicator_names else "N/A"

            print(f"🔹 Обрабатываем пару: {pairing_id} (Комната: {venue_name}, Судьи: {adjudicators_str})")

            # 🔹 Получаем результаты голосования (баллы)
            ballots_url = f"{API_URL}/tournaments/{tournament_slug}/rounds/{round_seq}/pairings/{pairing_id}/ballots"
            response_ballots = requests.get(ballots_url, headers=headers)

            if response_ballots.status_code != 200:
                print(f"❌ Ошибка запроса баллов {ballots_url}: {response_ballots.status_code}")
                continue

            ballots = response_ballots.json()
            if not ballots:
                print(f"⚠️ Нет данных о баллах для пары {pairing_id}")
                continue

            # 🔹 Получаем данные из баллотировки
            for ballot in ballots:
                for sheet in ballot["result"]["sheets"]:
                    for team_result in sheet["teams"]:
                        team_url = team_result["team"]
                        team_id = team_url.split("/")[-1]

                        # 🔹 Получаем название команды
                        team_info_url = f"{API_URL}/tournaments/{tournament_slug}/teams/{team_id}"
                        response_team = requests.get(team_info_url, headers=headers)
                        team_name = response_team.json()["short_name"] if response_team.status_code == 200 else None

                        position = team_result["side"]
                        team_score = team_result["score"]
                        team_points = team_result["points"]

                        # 🔹 Получаем информацию о спикерах и их баллах
                        speaker_1, speaker_2 = None, None
                        speaker_1_score, speaker_2_score = None, None

                        speeches = team_result.get("speeches", [])
                        if len(speeches) >= 1:
                            speaker_1_url = speeches[0].get("speaker")  # 🔹 Используем .get(), чтобы избежать KeyError

                            if speaker_1_url:  # ✅ Проверяем, что ссылка на спикера существует
                                speaker_1_id = speaker_1_url.split("/")[-1]
                                speaker_1_info_url = f"{API_URL}/tournaments/{tournament_slug}/speakers/{speaker_1_id}"
                                response_speaker_1 = requests.get(speaker_1_info_url, headers=headers)

                                if response_speaker_1.status_code == 200:
                                    speaker_1 = response_speaker_1.json().get("name", None)
                                speaker_1_score = speeches[0]["score"]
                            else:
                                speaker_1 = None
                                speaker_1_score = None  # Если первого спикера нет, ставим None


                        if len(speeches) >= 2:
                            speaker_2_url = speeches[1].get("speaker")

                            if speaker_2_url:  # ✅ Проверяем, что ссылка на второго спикера не None
                                speaker_2_id = speaker_2_url.split("/")[-1]
                                speaker_2_info_url = f"{API_URL}/tournaments/{tournament_slug}/speakers/{speaker_2_id}"
                                response_speaker_2 = requests.get(speaker_2_info_url, headers=headers)

                                if response_speaker_2.status_code == 200:
                                    speaker_2 = response_speaker_2.json().get("name", None)
                                speaker_2_score = speeches[1]["score"]
                            else:
                                speaker_2 = None
                                speaker_2_score = None  # Если второго спикера нет, ставим None

                        print(
                            f"🏆 {team_name} ({position}) - {speaker_1}: {speaker_1_score}, {speaker_2}: {speaker_2_score}, Очки: {team_points}, Судьи: {adjudicators_str}, Комната: {venue_name}")

                        # 🔹 Записываем в базу данных
                        cur.execute("""
                            INSERT INTO Round_Results (
                                Tournament_Slug, Round_Seq, Round_Name, 
                                Team_Name, Position, Speaker_1, Speaker_2, 
                                Speaker_1_Score, Speaker_2_Score, Team_Score, Points, 
                                Adjudicators, Venue_Name
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            tournament_slug, round_seq, round_name, team_name, position,
                            speaker_1, speaker_2, speaker_1_score, speaker_2_score,
                            team_score, team_points, adjudicators_str, venue_name
                        ))

        conn.commit()

cur.close()
conn.close()












