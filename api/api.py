from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import csv
import tempfile
import os
import io
from queue import Queue
import re
import sys
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import threading
import uuid
import logging
from typing import Dict, List, Optional
import time
import signal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
# from methods.threerules import threshold_rule, pattern_rule, composite_rule
from notifications.notification import RedisHandler

class CorrelationFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = 'system'
        if not hasattr(record, 'component'):
            record.component = 'main'
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(component)s] [%(correlation_id)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('transaction_service.log', encoding='utf-8')
    ]
)
logger = logging.getLogger()
logger.addFilter(CorrelationFilter())

redis = RedisHandler()
WORKER_COUNT = 4
MAX_QUEUE_SIZE = 1000
transactions: Dict[str, Dict] = {}
processing_queue = Queue(maxsize=MAX_QUEUE_SIZE)
VALID_TRANSACTION_TYPES = {"withdrawal", "deposit", "transfer", "payment", "refund"}
VALID_MERCHANT_CATEGORIES = {"utilities", "online", "other", "entertainment", "travel", "retail", "food", "transport"}
VALID_DEVICES = {"mobile", "atm", "pos", "web", "terminal"}
VALID_FRAUD_TYPES = {"card_theft", "account_takeover", "merchant_fraud", "money_laundering", "phishing", ""}
VALID_PAYMENT_CHANNELS = {"card", "ACH", "wire_transfer", "UPI", "cash", "digital_wallet"}
ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{6,64}$")
IP_PATTERN = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
DEVICE_HASH_PATTERN = re.compile(r"^[A-F0-9]{8}$")

class TransactionProcessor:
    def __init__(self):
        self.running = True

    def process_transaction(self, tx_data: Dict):
        tx_id = tx_data['transaction_id']
        correlation_id = tx_data['correlation_id']
        try:
            logger.info(f"Starting transaction processing",
                        extra={'component': 'worker', 'correlation_id': correlation_id})
            transactions[tx_id]['status'] = 'processing'
            transactions[tx_id]['processed_at'] = datetime.now().isoformat()
            time.sleep(0.1)
            transactions[tx_id]['status'] = 'processed'
            transactions[tx_id]['completed_at'] = datetime.now().isoformat()
            logger.info(f"Transaction processed successfully",
                        extra={'component': 'worker', 'correlation_id': correlation_id})
        except Exception as e:
            logger.error(f"Transaction processing failed: {str(e)}",
                         extra={'component': 'worker', 'correlation_id': correlation_id})
            transactions[tx_id]['status'] = 'failed'
            transactions[tx_id]['error'] = str(e)

def worker_loop(processor: TransactionProcessor):
    while processor.running:
        try:
            tx_data = processing_queue.get(timeout=1)
            if tx_data is None:
                break
            processor.process_transaction(tx_data)
            processing_queue.task_done()
        except:
            continue

processor = TransactionProcessor()
workers = []
for i in range(WORKER_COUNT):
    t = threading.Thread(target=worker_loop, args=(processor,), daemon=True, name=f"Worker-{i + 1}")
    t.start()
    workers.append(t)

def validate_transaction(data: Dict) -> List[str]:
    errors = []
    required_fields = [
        'transaction_id', 'correlation_id', 'timestamp',
        'sender_account', 'receiver_account', 'amount',
        'transaction_type'
    ]
    for field in required_fields:
        if field not in data:
            errors.append(f"Missing required field: {field}")
    if errors:
        return errors
    tx_id = data['transaction_id']
    if not ID_PATTERN.match(tx_id):
        errors.append("transaction_id must be 6-64 alphanumeric characters")
    elif tx_id in transactions:
        errors.append(f"transaction_id '{tx_id}' already exists")
    correlation_id = data['correlation_id']
    if not ID_PATTERN.match(correlation_id):
        errors.append("correlation_id must be 6-64 alphanumeric characters")
    amount = data['amount']
    if not isinstance(amount, (int, float)) or amount <= 0:
        errors.append("amount must be a positive number")
    sender = data['sender_account']
    receiver = data['receiver_account']
    if not ID_PATTERN.match(sender):
        errors.append("sender_account has invalid format")
    if not ID_PATTERN.match(receiver):
        errors.append("receiver_account has invalid format")
    tx_type = data['transaction_type']
    if tx_type not in VALID_TRANSACTION_TYPES:
        errors.append(f"transaction_type must be one of {VALID_TRANSACTION_TYPES}")
    timestamp = data['timestamp']
    try:
        ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        if ts > datetime.now().astimezone(ts.tzinfo):
            errors.append("timestamp cannot be in the future")
    except Exception as e:
        errors.append(f"timestamp is invalid: {str(e)}")
    if 'merchant_category' in data and data['merchant_category'] not in VALID_MERCHANT_CATEGORIES:
        errors.append(f"merchant_category must be one of {VALID_MERCHANT_CATEGORIES}")
    if 'device_used' in data and data['device_used'] not in VALID_DEVICES:
        errors.append(f"device_used must be one of {VALID_DEVICES}")
    if 'is_fraud' in data and not isinstance(data['is_fraud'], bool):
        errors.append("is_fraud must be a boolean value")
    if 'fraud_type' in data and data['fraud_type'] not in VALID_FRAUD_TYPES:
        errors.append(f"fraud_type must be one of {VALID_FRAUD_TYPES}")
    if 'time_since_last_transaction' in data and not isinstance(data['time_since_last_transaction'], (int, float)):
        errors.append("time_since_last_transaction must be a number")
    if 'spending_deviation_score' in data and not isinstance(data['spending_deviation_score'], (int, float)):
        errors.append("spending_deviation_score must be a number")
    if 'velocity_score' in data and not isinstance(data['velocity_score'], (int, float)):
        errors.append("velocity_score must be a number")
    if 'geo_anomaly_score' in data and not isinstance(data['geo_anomaly_score'], (int, float)):
        errors.append("geo_anomaly_score must be a number")
    if 'payment_channel' in data and data['payment_channel'] not in VALID_PAYMENT_CHANNELS:
        errors.append(f"payment_channel must be one of {VALID_PAYMENT_CHANNELS}")
    if 'ip_address' in data and data['ip_address'] and not IP_PATTERN.match(data['ip_address']):
        errors.append("ip_address has invalid format")
    if 'device_hash' in data and data['device_hash'] and not DEVICE_HASH_PATTERN.match(data['device_hash']):
        errors.append("device_hash must be 8 hex characters")
    return errors

class FraudDetectionAPIHandler(BaseHTTPRequestHandler):
    def _set_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        self.send_response(200)
        self._set_cors_headers()
        self.end_headers()

    def _send_json_response(self, status_code: int, data: Dict, correlation_id: str = None):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self._set_cors_headers()
        self.end_headers()
        response_data = data.copy()
        if correlation_id:
            response_data['correlation_id'] = correlation_id
        self.wfile.write(json.dumps(response_data, ensure_ascii=False).encode('utf-8'))

    def _log_request(self, method: str, path: str, correlation_id: str = None):
        logger.info(f"{method} {path}",
                    extra={'component': 'api', 'correlation_id': correlation_id or 'unknown'})

    def do_GET(self):
        parsed_path = urlparse(self.path)
        correlation_id = str(uuid.uuid4())
        self._log_request('GET', self.path, correlation_id)
        try:
            if parsed_path.path == '/transactions/count':
                self._send_json_response(200, {
                    "count": len(transactions),
                    "queue_size": processing_queue.qsize(),
                    "processed_count": len([t for t in transactions.values() if t.get('status') == 'processed']),
                    "failed_count": len([t for t in transactions.values() if t.get('status') == 'failed'])
                }, correlation_id)
            elif parsed_path.path == '/transactions/export-csv':
                self._export_to_csv(correlation_id)
            elif parsed_path.path == '/transactions':
                self._get_transactions_list(parsed_path.query, correlation_id)
            elif parsed_path.path.startswith('/transactions/'):
                tx_id = parsed_path.path.split('/')[-1]
                self._get_transaction_details(tx_id, correlation_id)
            else:
                info = {
                    "message": "Financial Transactions Fraud Detection API",
                    "version": "1.0.0",
                    "supported_fields": {
                        "required": [
                            "transaction_id", "correlation_id", "timestamp",
                            "sender_account", "receiver_account", "amount", "transaction_type"
                        ],
                        "optional": [
                            "merchant_category", "location", "device_used", "is_fraud",
                            "fraud_type", "time_since_last_transaction", "spending_deviation_score",
                            "velocity_score", "geo_anomaly_score", "payment_channel",
                            "ip_address", "device_hash"
                        ]
                    },
                    "endpoints": {
                        "add_transaction": "POST /transactions",
                        "get_transaction": "GET /transactions/{id}",
                        "list_transactions": "GET /transactions",
                        "export_csv": "GET /transactions/export-csv",
                        "stats": "GET /transactions/count"
                    }
                }
                self._send_json_response(200, info, correlation_id)
        except Exception as e:
            logger.error(f"GET request failed: {str(e)}",
                         extra={'component': 'api', 'correlation_id': correlation_id})
            self._send_json_response(500, {"error": "Internal server error"}, correlation_id)

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        correlation_id = str(uuid.uuid4())
        self._log_request('POST', self.path, correlation_id)
        if content_length == 0:
            self._send_json_response(400, {"error": "Empty request body"}, correlation_id)
            return
        try:
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            if self.path == '/transactions':
                self._add_transaction(data, correlation_id)
            elif self.path == '/transactions/import-json':
                self._import_json_data(data, correlation_id)
            elif self.path == '/notifications/create':
                self._send_notification(data, correlation_id)
            elif self.path == '/threshold':
                self._check_threshold_rule(data)
            else:
                self._send_json_response(404, {"error": "Endpoint not found"}, correlation_id)
        except json.JSONDecodeError:
            self._send_json_response(400, {"error": "Invalid JSON format"}, correlation_id)
        except Exception as e:
            logger.error(f"POST request failed: {str(e)}",
                         extra={'component': 'api', 'correlation_id': correlation_id})
            self._send_json_response(500, {"error": "Internal server error"}, correlation_id)

    def _add_transaction(self, data: Dict, correlation_id: str):
        if 'correlation_id' not in data:
            data['correlation_id'] = correlation_id
        errors = validate_transaction(data)
        if errors:
            logger.warning(f"Validation failed: {errors}",
                           extra={'component': 'validation', 'correlation_id': correlation_id})
            self._send_json_response(400, {"error": "Validation failed", "details": errors}, correlation_id)
            return
        tx_id = data['transaction_id']
        transactions[tx_id] = {
            **data,
            'status': 'received',
            'received_at': datetime.now().isoformat(),
            'queue_position': processing_queue.qsize() + 1
        }
        try:
            processing_queue.put(data, timeout=5)
            transactions[tx_id]['status'] = 'queued'
            transactions[tx_id]['queued_at'] = datetime.now().isoformat()
            logger.info(f"Transaction queued successfully",
                        extra={'component': 'queue', 'correlation_id': correlation_id})
            self._send_json_response(202, {
                "message": "Transaction accepted for processing",
                "transaction_id": tx_id,
                "queue_position": processing_queue.qsize()
            }, correlation_id)
        except Exception as e:
            transactions[tx_id]['status'] = 'queue_failed'
            transactions[tx_id]['error'] = str(e)
            logger.error(f"Failed to queue transaction: {str(e)}",
                         extra={'component': 'queue', 'correlation_id': correlation_id})
            self._send_json_response(503, {
                "error": "Service temporarily unavailable",
                "details": "Queue is full"
            }, correlation_id)

    def _import_json_data(self, json_data: Dict, correlation_id: str):
        added_count = 0
        failed_count = 0
        errors = []
        if isinstance(json_data, list):
            transactions_list = json_data
        elif isinstance(json_data, dict) and 'transactions' in json_data:
            transactions_list = json_data['transactions']
        else:
            transactions_list = [json_data]
        for item in transactions_list:
            try:
                if 'correlation_id' not in item:
                    item['correlation_id'] = f"{correlation_id}-{added_count}"
                validation_errors = validate_transaction(item)
                if validation_errors:
                    failed_count += 1
                    errors.append({
                        'transaction': item.get('transaction_id', 'unknown'),
                        'errors': validation_errors
                    })
                    continue
                tx_id = item['transaction_id']
                transactions[tx_id] = {
                    **item,
                    'status': 'queued',
                    'received_at': datetime.now().isoformat(),
                    'queued_at': datetime.now().isoformat()
                }
                processing_queue.put(item, timeout=1)
                added_count += 1
            except Exception as e:
                failed_count += 1
                errors.append({
                    'transaction': item.get('transaction_id', 'unknown'),
                    'error': str(e)
                })
        result = {
            "message": f"Import completed: {added_count} added, {failed_count} failed",
            "added_count": added_count,
            "failed_count": failed_count
        }
        if errors:
            result["errors"] = errors[:10]
        self._send_json_response(207, result, correlation_id)

    def _export_to_csv(self, transactions: list[dict], correlation_id: str):
        if not transactions:
            self._send_json_response(404, {"error": "No transactions available"}, correlation_id)
            return

        try:
            output = io.StringIO()
            writer = csv.writer(output)

            headers = [
                'transaction_id', 'timestamp', 'sender_account', 'receiver_account',
                'amount', 'transaction_type', 'merchant_category', 'location',
                'device_used', 'is_fraud', 'fraud_type', 'time_since_last_transaction',
                'spending_deviation_score', 'velocity_score', 'geo_anomaly_score',
                'payment_channel', 'ip_address', 'device_hash', 'correlation_id',
                'status', 'received_at', 'processed_at'
            ]
            writer.writerow(headers)

            # Заполняем строки CSV
            for tx in transactions:
                writer.writerow([
                    tx.get('transaction_id', ''),
                    tx.get('timestamp', ''),
                    tx.get('sender_account', ''),
                    tx.get('receiver_account', ''),
                    tx.get('amount', ''),
                    tx.get('transaction_type', ''),
                    tx.get('merchant_category', ''),
                    tx.get('location', ''),
                    tx.get('device_used', ''),
                    tx.get('is_fraud', ''),
                    tx.get('fraud_type', ''),
                    tx.get('time_since_last_transaction', ''),
                    tx.get('spending_deviation_score', ''),
                    tx.get('velocity_score', ''),
                    tx.get('geo_anomaly_score', ''),
                    tx.get('payment_channel', ''),
                    tx.get('ip_address', ''),
                    tx.get('device_hash', ''),
                    tx.get('correlation_id', ''),
                    tx.get('status', ''),
                    tx.get('received_at', ''),
                    tx.get('processed_at', '')
                ])

            csv_data = output.getvalue().encode('utf-8')
            output.close()

            # Отправка CSV клиенту
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header(
                'Content-Disposition',
                f'attachment; filename="transactions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
            )
            self.send_header('Content-Length', str(len(csv_data)))
            self._set_cors_headers()
            self.end_headers()
            self.wfile.write(csv_data)

            logger.info(f"CSV export completed: {len(transactions)} transactions",
                        extra={'component': 'export', 'correlation_id': correlation_id})

        except Exception as e:
            logger.error(f"CSV export failed: {str(e)}",
                        extra={'component': 'export', 'correlation_id': correlation_id})
            self._send_json_response(500, {"error": "Export failed"}, correlation_id)

    def _get_transactions_list(self, query_string: str, correlation_id: str):
        try:
            query_params = parse_qs(query_string)
            page = int(query_params.get('page', [1])[0])
            limit = int(query_params.get('limit', [50])[0])
            status_filter = query_params.get('status', [None])[0]
            start_idx = (page - 1) * limit
            end_idx = start_idx + limit
            filtered_txs = list(transactions.values())
            if status_filter:
                filtered_txs = [tx for tx in filtered_txs if tx.get('status') == status_filter]
            filtered_txs.sort(key=lambda x: x.get('received_at', ''), reverse=True)
            paginated_txs = filtered_txs[start_idx:end_idx]
            result_txs = []
            for tx in paginated_txs:
                result_txs.append({
                    'transaction_id': tx.get('transaction_id'),
                    'correlation_id': tx.get('correlation_id'),
                    'timestamp': tx.get('timestamp'),
                    'sender_account': tx.get('sender_account'),
                    'receiver_account': tx.get('receiver_account'),
                    'amount': tx.get('amount'),
                    'transaction_type': tx.get('transaction_type'),
                    'merchant_category': tx.get('merchant_category'),
                    'location': tx.get('location'),
                    'device_used': tx.get('device_used'),
                    'is_fraud': tx.get('is_fraud'),
                    'status': tx.get('status'),
                    'received_at': tx.get('received_at')
                })
            self._send_json_response(200, {
                "transactions": result_txs,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": len(filtered_txs),
                    "pages": (len(filtered_txs) + limit - 1) // limit
                }
            }, correlation_id)
        except Exception as e:
            logger.error(f"Failed to get transactions list: {str(e)}",
                         extra={'component': 'api', 'correlation_id': correlation_id})
            self._send_json_response(500, {"error": "Failed to retrieve transactions"}, correlation_id)

    def _get_transaction_details(self, tx_id: str, correlation_id: str):
        if tx_id not in transactions:
            self._send_json_response(404, {"error": "Transaction not found"}, correlation_id)
            return
        tx_data = transactions[tx_id]
        self._send_json_response(200, {"transaction": tx_data}, correlation_id)

    def _send_notification(self, data: Dict, correlation_id: str):
        try:
            required_fields = ['id', 'details', 'severity']
            for field in required_fields:
                if field not in data:
                    self._send_json_response(400, {"error": f"Missing field: {field}"}, correlation_id)
                    return
            redis.send_alert(data['id'], data['details'], data['severity'])
            self._send_json_response(200, {"message": "Notification sent", "data": data}, correlation_id)
        except Exception as e:
            logger.error(f"Notification failed: {str(e)}",
                         extra={'component': 'notifications', 'correlation_id': correlation_id})
            self._send_json_response(400, {"error": str(e)}, correlation_id)
            
    # def _check_threshold_rule(self, data: Dict):
    #     print(data)
    #     try:
    #         required_fields = ['id', 'amount', 'operation', "number"]
    #         for field in required_fields:
    #             if field not in data:
    #                 self._send_json_response(400, {"error": f"Missing field: {field}"})
    #                 return
            
    #         amount = str(data['amount'])
    #         operation = data['operation']
    #         number = str(data['number'])
            
    #         bool = threshold_rule(amount, operation, number)
    #         self._send_json_response(200, {"message": "Threshold checking", "result": bool})
    #     except Exception as e:
    #         self._send_json_response(400, {"error": str(e)})

def shutdown(signum, frame):
    logger.info("Shutting down...", extra={'component': 'shutdown', 'correlation_id': 'system'})
    processor.running = False
    time.sleep(2)
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    server = HTTPServer(('0.0.0.0', 3000), FraudDetectionAPIHandler)
    listener_thread = threading.Thread(target=redis.listener, daemon=True, name="RedisListener")
    listener_thread.start()
    print("Fraud Detection API Server running on http://0.0.0.0:3000")
    print(f"Worker threads: {WORKER_COUNT}, Max queue size: {MAX_QUEUE_SIZE}")
    print("Supported transaction fields:")
    print("Required: transaction_id, timestamp, sender_account, receiver_account, amount, transaction_type")
    print("Optional: merchant_category, location, device_used, is_fraud, fraud_type, time_since_last_transaction, spending_deviation_score, velocity_score, geo_anomaly_score, payment_channel, ip_address, device_hash")
    logger.info("Server started successfully", extra={'component': 'server', 'correlation_id': 'system'})
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        shutdown(None, None)
