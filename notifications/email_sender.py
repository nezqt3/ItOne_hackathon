import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os

load_dotenv()

class EmailSender(object):
    def __init__(self):
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", 587))
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.sender_password = os.getenv("SENDER_PASSWORD")
        self.admin_emails = os.getenv("ADMIN_EMAILS", "").split(",")
    def _send_email(self, subject: str, html_content: str):
        if not self.sender_email or not self.sender_password:
            print("❌ SMTP credentials not configured")
            return
        try:
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = self.sender_email
            message["To"] = ", ".join(self.admin_emails)
            html_part = MIMEText(html_content, "html")
            message.attach(html_part)
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(message)
            print(f"✅ Email отправлен: {subject}")
        except Exception as e:
            print(f"❌ Ошибка отправки email: {e}")

    def send_alert_email(self, transaction_id: str, details: str, severity: float):
        subject = f"🚨 Подозрительная транзакция #{transaction_id}"
        html_content = f"""
        <html>
          <body>
            <h2 style="color: #ff6b6b;">🚨 Обнаружена подозрительная транзакция</h2>
            <table border="1" cellpadding="8" style="border-collapse: collapse;">
              <tr><td><b>ID транзакции:</b></td><td>{transaction_id}</td></tr>
              <tr><td><b>Описание:</b></td><td>{details}</td></tr>
              <tr><td><b>Уровень риска:</b></td><td>{severity:.2f}</td></tr>
              <tr><td><b>Статус:</b></td><td>Требует проверки</td></tr>
            </table>
            <br>
            <p><small>Это автоматическое уведомление от системы мониторинга транзакций</small></p>
          </body>
        </html>
        """
        self._send_email(subject, html_content)
