import requests, json

BOT_TOKEN = "8352337942:AAE_9923PEcYmGQbhA9vqjNUMmqZ40JPf0w"
url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

data = requests.get(url, timeout=30).json()
print(json.dumps(data, indent=2, ensure_ascii=False))

# มองหา: result -> ... -> message -> chat -> id
