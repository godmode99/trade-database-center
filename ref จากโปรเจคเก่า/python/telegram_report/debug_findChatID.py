import requests, json

BOT_TOKEN = "8352337942:AAE_9923PEcYmGQbhA9vqjNUMmqZ40JPf0w"
print(json.dumps(requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=20).json(), indent=2))
