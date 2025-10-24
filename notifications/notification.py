import redis
from dotenv import load_dotenv
import os
import time
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from notifications.connect_tgbot import Bot
from notifications.email_sender import EmailSender

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
STREAM_KEY = "alerts_stream"

class RedisHandler:
    def __init__(self):
        print("Connecting to Redis...", flush=True)
        self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        print(f"Connected to Redis at {REDIS_URL}", flush=True)
        self.bot = Bot()
        self.email_sender = EmailSender()

        # Удаляем все старые сообщения из stream при старте
        if self.redis.exists(STREAM_KEY):
            self.redis.delete(STREAM_KEY)
            print(f"🗑️ Существующий stream '{STREAM_KEY}' очищен", flush=True)
        
    def listener(self):
        print('Listener started', flush=True)
        last_id = "0"

        while True:
            try:
                messages = self.redis.xread({STREAM_KEY: last_id}, block=0)
                for stream, entries in messages:
                    for message_id, data in entries:
                        last_id = message_id
                        print(f"📥 Получено сообщение: {data}", flush=True)

                        text = (
                            f"🚨 <b>Подозрительная активность</b>\n"
                            f"ID: <code>{data['id']}</code>\n"
                            f"Уровень риска: {float(data['severity']):.2f}\n"
                            f"Детали:\n{data['details']}"
                        )
                        self.bot._send_message(text)

                        self.email_sender.send_alert_email(
                            transaction_id=data['id'],
                            details=data['details'],
                            severity=float(data['severity'])
                        )
                        
            except Exception as e:
                print(f"❌ Ошибка при чтении из Redis Streams: {e}", flush=True)
                time.sleep(2)
                    
    def send_alert(self, id, details, severity):
        message = {
            "id": str(id),
            "details": details,
            "severity": str(severity)
        }
        self.redis.xadd(STREAM_KEY, message)
        print(f"📤 Отправлено в Redis Stream: {message}", flush=True)


# # Экземпляр для использования
# redis_handler = RedisHandler()

# # Пример отправки нового сообщения
# redis_handler.send_alert(id=10, details='Тестовое сообщение', severity=0.56)

# # Запуск listener-а
# redis_handler.listener()
