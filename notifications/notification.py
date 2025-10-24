import redis
from dotenv import load_dotenv
from notifications.email_sender import EmailSender
import os
import time
import json
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from notifications.connect_tgbot import Bot

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

class Redis(object):
    def __init__(self):
        self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        self.bot = Bot()
        self.email_sender = EmailSender()
        
    def listener(self):
        last_id = "0" 

        while True:
            try:
                messages = self.redis.xread({"alerts_stream": last_id}, block=0)
                for stream, entries in messages:
                    for message_id, data in entries:
                        last_id = message_id
                        try:
                            data["severity"] = float(data["severity"])
                        except Exception:
                            pass

                        print(f"📥 Получено сообщение из Redis: {data}")

                        text = (
                            f"🚨 <b>Подозрительная активность</b>\n"
                            f"ID: <code>{data['id']}</code>\n"
                            f"Описание: {data['details']}\n"
                            f"Уровень риска: {data['severity']:.2f}"
                        )
                        self.bot._send_message(text)

                        self.email_sender.send_alert_email(
                            transaction_id=data['id'],
                            details=data['details'],
                            severity=data['severity']
                        )
                        
            except Exception as e:
                print(f"❌ Ошибка при чтении из Redis Streams: {e}")
                time.sleep(2)
                    
    def send_alert(self, id, details, severity):
        message = {
            "id": str(id),
            "details": details,
            "severity": str(severity)
        }
        self.redis.xadd("alerts_stream", message)
        print(f"📤 Отправлено в Redis Stream: {message}")
        
redis_notification = Redis()
