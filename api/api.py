from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import csv
import tempfile
import os
import sys
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from notifications.notification import Redis

transactions = []
redis = Redis()

class SimpleAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path == '/transactions/count':
            self._send_json_response(200, {"count": len(transactions)})
        elif parsed_path.path == '/transactions/export-csv':
            self._export_to_csv()
        else:
            info = {
                "message": "Simple JSON to CSV API",
                "endpoints": {
                    "import_json": "POST /transactions/import-json",
                    "add_single": "POST /transactions/add",
                    "export_csv": "GET /transactions/export-csv",
                    "count": "GET /transactions/count"
                }
            }
            self._send_json_response(200, info)

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        try:
            data = json.loads(post_data.decode('utf-8'))
            if self.path == '/transactions/add':
                transactions.append(data)
                self._send_json_response(200, {"message": "Transaction added", "id": data['transaction_id']})
            elif self.path == '/transactions/import-json':
                # Обработка импорта JSON файла
                added_count = self._import_json_data(data)
                self._send_json_response(200, {"message": f"Imported {added_count} transactions"})
            elif self.path == '/notifications/create':
                self._send_notification(data)
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            self._send_json_response(400, {"error": str(e)})

    def _send_json_response(self, status_code, data):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def _import_json_data(self, json_data):
        added_count = 0
        if isinstance(json_data, list):
            for item in json_data:
                transactions.append(item)
                added_count += 1

        elif isinstance(json_data, dict):
            if 'transactions' in json_data and isinstance(json_data['transactions'], list):
                for item in json_data['transactions']:
                    transactions.append(item)
                    added_count += 1
            else:
                transactions.append(json_data)
                added_count += 1
        return added_count

    def _export_to_csv(self):
        if not transactions:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "No transactions available"}')
            return
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            headers = ['transaction_id', 'timestamp', 'sender_account', 'receiver_account',
                       'amount', 'transaction_type', 'merchant_category', 'location']
            writer.writerow(headers)
            for transaction in transactions:
                row = [
                    transaction.get('transaction_id', ''),
                    transaction.get('timestamp', ''),
                    transaction.get('sender_account', ''),
                    transaction.get('receiver_account', ''),
                    transaction.get('amount', ''),
                    transaction.get('transaction_type', ''),
                    transaction.get('merchant_category', ''),
                    transaction.get('location', '')
                ]
                writer.writerow(row)
            temp_path = f.name
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.send_header('Content-Disposition',
                         f'attachment; filename="transactions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"')
        self.end_headers()
        with open(temp_path, 'rb') as f:
            self.wfile.write(f.read())
        os.unlink(temp_path)
        
    def _send_notification(self, data):
        try:
            redis.send_alert(data['id'], data['details'], data['severity'])
            self._send_json_response(200, {"message": "Notification added", "data": data})
        except Exception as e:
            self._send_json_response(400, {"error": str(e)})

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8000), SimpleAPIHandler)
    threading.Thread(target=redis.listener, daemon=True).start()
    print("Server running on http://localhost:8000")
    server.serve_forever()
