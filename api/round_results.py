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

# Список плей-офф-раундов
PLAYOFF_ROUNDS = {
    "1/8 финала", "Четвертьфинал", "Четвертьфиналы", "Semifinals", "Полуфинал", "Полуфиналы",
    "Grand Final", "Финал", "Основной финал", "Финал Молодых", "Финал молодых Finals", "ФМ Finals"
}

# Получаем турниры и их последний обработанный раунд
cur.execute("SELECT Tournament_Slug, Last_Processed_Round FROM Tournaments;")
tournaments = cur.fetchall()

for tournament in tournaments:
    tournament_slug, last_processed_round = tournament

    rounds_url = f"{API_URL}/tournaments/{tournament_slug}/rounds"
    response_rounds = requests.get(rounds_url, headers=headers)

    if response_rounds.status_code != 200:
        print(f"Ошибка запроса раундов {rounds_url}: {response_rounds.status_code}")
        continue

    rounds_data = response_rounds.json()
    round_mapping = {round["seq"]: round for round in rounds_data}

    for round_seq, round_info in round_mapping.items():
        if round_seq <= last_processed_round:
            continue

        round_name = round_info["name"]
        is_playoff = round_name in PLAYOFF_ROUNDS
        print(f"Обрабатываем {round_name} (Раунд {round_seq}) для {tournament_slug}")

        try:
            # Начинаем транзакцию
            cur.execute("BEGIN;")

            cur.execute("""
                SELECT COUNT(*) FROM Round_Results
                WHERE Tournament_Slug = %s AND Round_Seq = %s;
            """, (tournament_slug, round_seq))
            already_exists = cur.fetchone()[0] > 0

            if already_exists:
                print(f"Раунд {round_seq} уже обработан, пропускаем...")
                continue

            pairings_url = f"{API_URL}/tournaments/{tournament_slug}/rounds/{round_seq}/pairings"
            response_pairings = requests.get(pairings_url, headers=headers)

            if response_pairings.status_code != 200:
                print(f"Ошибка запроса пар {pairings_url}: {response_pairings.status_code}")
                continue

            pairings = response_pairings.json()

            for pairing in pairings:
                pairing_id = pairing["id"]
                venue_url = pairing.get("venue")
                venue_name = "Неизвестно"

                if venue_url:
                    venue_id = venue_url.split("/")[-1]
                    venue_info_url = f"{API_URL}/tournaments/{tournament_slug}/venues/{venue_id}"
                    response_venue = requests.get(venue_info_url, headers=headers)

                    if response_venue.status_code == 200:
                        venue_name = response_venue.json().get("name", "Неизвестно")

                # Судьи на раунде
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

                # Баллы в бэллоте
                ballots_url = f"{API_URL}/tournaments/{tournament_slug}/rounds/{round_seq}/pairings/{pairing_id}/ballots"
                response_ballots = requests.get(ballots_url, headers=headers)

                if response_ballots.status_code != 200:
                    print(f"Ошибка запроса баллов {ballots_url}: {response_ballots.status_code}")
                    continue

                ballots = response_ballots.json()
                if not ballots:
                    print(f"Нет данных о баллах для пары {pairing_id}")
                    continue

                for ballot in ballots:
                    for sheet in ballot["result"]["sheets"]:
                        for team_result in sheet["teams"]:
                            team_url = team_result["team"]
                            team_id = team_url.split("/")[-1]

                            # Название команды
                            team_info_url = f"{API_URL}/tournaments/{tournament_slug}/teams/{team_id}"
                            response_team = requests.get(team_info_url, headers=headers)
                            team_name = response_team.json()["short_name"] if response_team.status_code == 200 else None

                            position = team_result["side"]
                            team_score = team_result["score"]
                            team_points = team_result["points"]

                            # Определяем победы
                            points = "win" if is_playoff and team_result.get("win") else team_points

                            # Получаем спикеров и их баллы
                            speakers, scores = [], []
                            for speech in team_result.get("speeches", []):
                                speaker_url = speech.get("speaker")
                                if speaker_url:
                                    speaker_id = speaker_url.split("/")[-1]
                                    speaker_info_url = f"{API_URL}/tournaments/{tournament_slug}/speakers/{speaker_id}"
                                    response_speaker = requests.get(speaker_info_url, headers=headers)

                                    if response_speaker.status_code == 200:
                                        speakers.append(response_speaker.json().get("name"))
                                    else:
                                        speakers.append(None)

                                scores.append(speech.get("score", None))

                            while len(speakers) < 2:
                                speakers.append(None)
                            while len(scores) < 2:
                                scores.append(None)

                            print(f"🏆 {team_name} ({position}) - {speakers[0]}: {scores[0]}, {speakers[1]}: {scores[1]}, Результат: {points}")

                            # Добавляем или обновляем
                            cur.execute("""
                                INSERT INTO Round_Results (
                                    Tournament_Slug, Round_Seq, Round_Name, 
                                    Team_Name, Position, Speaker_1, Speaker_2, 
                                    Speaker_1_Score, Speaker_2_Score, Team_Score, Points, 
                                    Adjudicators, Venue_Name
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (Tournament_Slug, Round_Seq, Team_Name) 
                                DO UPDATE SET
                                    Speaker_1 = EXCLUDED.Speaker_1,
                                    Speaker_2 = EXCLUDED.Speaker_2,
                                    Speaker_1_Score = EXCLUDED.Speaker_1_Score,
                                    Speaker_2_Score = EXCLUDED.Speaker_2_Score,
                                    Team_Score = EXCLUDED.Team_Score,
                                    Points = EXCLUDED.Points,
                                    Adjudicators = EXCLUDED.Adjudicators,
                                    Venue_Name = EXCLUDED.Venue_Name;
                            """, (
                                tournament_slug, round_seq, round_name, team_name, position,
                                speakers[0], speakers[1], scores[0], scores[1],
                                team_score, points, adjudicators_str, venue_name
                            ))

            # Обновляем последний раунд, если вылезла ошибка
            cur.execute("""
                UPDATE Tournaments
                SET Last_Processed_Round = %s
                WHERE Tournament_Slug = %s;
            """, (round_seq, tournament_slug))

            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"❌ Ошибка в {round_name}: {e}")















