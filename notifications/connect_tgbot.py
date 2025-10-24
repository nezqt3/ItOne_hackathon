from telebot import TeleBot
from dotenv import load_dotenv
import requests
import os

load_dotenv()

# 578814803

ADMINS = ['1108856135']

class Bot(object):
    def __init__(self):
        self.BOT_TOKEN = os.getenv("TOKEN_BOT")
        self.bot = TeleBot(token=self.BOT_TOKEN)
        self.url = f"https://api.telegram.org/bot{self.BOT_TOKEN}/sendMessage"
        
    def _send_message(self, text: str = "Подозрительная транзакция"):
        for admin_id in ADMINS:
            payload = {"chat_id": admin_id, "text": text, "parse_mode": "HTML"}
            try:
                response = requests.post(self.url, json=payload, timeout=5)
            except:
                print("Не удалось")
                