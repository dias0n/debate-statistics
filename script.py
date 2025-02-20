import os
from dotenv import load_dotenv

# 🔹 Загружаем .env
load_dotenv()

# 🔹 Получаем токены
token1 = os.getenv("TOKEN_AD")
token2 = os.getenv("TOKEN_UL")

print(f"Токен AD: {token1}")
print(f"Токен UL: {token2}")