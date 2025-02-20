# Importing libraries
import requests
import os
from dotenv import load_dotenv

# Loading .env
load_dotenv()

# Getting tokens and url from .env file
token1 = os.getenv("TOKEN_AD")
url = os.getenv("API_URL")

# Checking if URL and Tokens are loaded
if not url or not token1:
    raise ValueError("Ошибка: API_URL или TOKEN не загружены из .env")

# Authorizing
headers = {"Authorization": f"Token {token1}"}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    tournaments_data = response.json()

# Getting ID and Name of Tournaments
    for tournament in tournaments_data:
        tournament_id = tournament['id']
        tournament_name = tournament['name']
        print(f"ID: {tournament_id} – Tournament Name: {tournament_name}")

else:
    print(f"Ошибка {response.status_code}: {response.text}")

