import requests

# 🔹 Настройки API
tournament_slug = "Limerence25"
token = "0436ff93b7ccab79d0191c0a718a1dcb5b18b086"

# 🔹 Запрос списка команд
judges_url = f"http://dcmatch.kz:8000/api/v1/tournaments/pantheon2025/speakers/1597"
headers = {"Authorization": f"Token {token}"}

response = requests.get(judges_url, headers=headers)

if response.status_code == 200:
    print(response.json())

else:
    print(f"Ошибка {response.status_code}: {response.text}")







