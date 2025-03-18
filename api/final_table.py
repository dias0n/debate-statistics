import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# БД
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS final_table;")

# Создаём `final_table`
create_table_query = """
CREATE TABLE final_table AS
SELECT
    t.tournament_id, t.last_processed_round, t.tournament_name, t.tournament_slug,
    r.round_seq, COALESCE(m.round_number, -1) AS round_number, 
    r.round_name, r.team_name, r.position, r.points, 
    COALESCE(r.speaker_1, te.speaker_1) AS speaker_1, 
    COALESCE(r.speaker_1_score, NULL) AS speaker_1_score,
    COALESCE(r.speaker_2, te.speaker_2) AS speaker_2, 
    COALESCE(r.speaker_2_score, NULL) AS speaker_2_score,
    r.team_score, r.adjudicators, r.venue_name,
    COALESCE(m.motion_id, -1) AS motion_id,  
    m.motion_text, m.info_slide_plain
FROM round_results r
LEFT JOIN motions m 
    ON r.round_name = m.round_name 
    AND r.tournament_slug = m.tournament_slug
LEFT JOIN tournaments t 
    ON r.tournament_slug = t.tournament_slug
LEFT JOIN teams te
    ON r.team_name = te.team_name
    AND r.tournament_slug = te.tournament_slug;
"""

cur.execute(create_table_query)
conn.commit()

conn.commit()
print("Таблица final_table успешно создана и обновлена!")

cur.close()
conn.close()