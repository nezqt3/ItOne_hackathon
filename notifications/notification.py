import redis
from dotenv import load_dotenv
import os
import time
import sys
import json
from datetime import datetime, timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from notifications.connect_tgbot import Bot
from notifications.email_sender import EmailSender

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
STREAM_KEY = "alerts_stream"


def parse_details(data: dict) -> dict:
    """Корректная десериализация поля details из Redis Stream"""
    details_raw = data.get('details', '{}')
    if isinstance(details_raw, str):
        try:
            details = json.loads(details_raw)
            if isinstance(details, str):
                details = json.loads(details)
        except json.JSONDecodeError:
            details = {}
    else:
        details = details_raw
    return details


def format_email_alert(data: dict) -> str:
    details = parse_details(data)
    transaction = details.get("transaction", {})
    risk_level = details.get("risk_level", "low")
    triggered_rules = details.get("triggered_rules", [])

    rows = ""
    for k, v in transaction.items():
        rows += f"<tr><td><b>{k}</b></td><td>{v}</td></tr>"

    rules_html = ", ".join(triggered_rules) if triggered_rules else "Нет"

    html_body = f"""
    <html>
    <body>
        <h2>🚨 Подозрительная транзакция</h2>
        <p><b>ID транзакции:</b> {data.get('id', 'UNKNOWN')}</p>
        <p><b>Уровень риска:</b> {risk_level} ({float(data.get('severity', 0)):.2f})</p>
        <p><b>Сработавшие правила:</b> {rules_html}</p>
        <table border="1" cellpadding="5" cellspacing="0">
            <thead>
                <tr><th>Поле</th><th>Значение</th></tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
    </body>
    </html>
    """
    return html_body


def format_telegram_alert(data: dict) -> str:
    details = parse_details(data)
    transaction = details.get("transaction", {})
    risk_level = details.get("risk_level", "low")
    triggered_rules = details.get("triggered_rules", [])

    severity = float(data.get('severity', 0))
    tx_id = data.get('id', 'UNKNOWN')

    if severity >= 0.8:
        risk_emoji = "🔥"
    elif severity >= 0.5:
        risk_emoji = "⚠️"
    else:
        risk_emoji = "ℹ️"

    rules_text = ", ".join(triggered_rules) if triggered_rules else "Нет"

    alert_text = (
        f"{risk_emoji} <b>Подозрительная транзакция</b>\n"
        f"<b>ID транзакции:</b> <code>{tx_id}</code>\n"
        f"<b>Уровень риска:</b> {risk_level} ({severity:.2f})\n"
        f"<b>Сработавшие правила:</b> {rules_text}\n"
        f"<b>Детали:</b>\n<pre>{json.dumps(transaction, indent=2)}</pre>\n"
    )
    return alert_text



class RedisHandler:
    def __init__(self):
        print("Connecting to Redis...", flush=True)
        self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        print(f"Connected to Redis at {REDIS_URL}", flush=True)
        self.bot = Bot()
        self.email_sender = EmailSender()

        # Очищаем stream при старте
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

                        # Отправка в Telegram
                        text = format_telegram_alert(data)
                        self.bot._send_message(text)

                        # Отправка на Email
                        html_content = format_email_alert(data)
                        self.email_sender.send_alert_email(
                            transaction_id=data['id'],
                            details=html_content,
                            severity=float(data['severity'])
                        )
            except Exception as e:
                print(f"❌ Ошибка при чтении из Redis Streams: {e}", flush=True)
                time.sleep(2)

    def send_alert(self, id, details, severity):
        """
        Отправка нового сообщения в Redis Stream
        details должны быть сериализованы в JSON с полями:
        transaction, risk_level, triggered_rules
        """
        message = {
            "id": str(id),
            "details": json.dumps(details),
            "severity": str(severity)
        }
        self.redis.xadd(STREAM_KEY, message)
        print(f"📤 Отправлено в Redis Stream: {message}", flush=True)
