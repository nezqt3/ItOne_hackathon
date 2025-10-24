from django.utils import timezone
from django.db import models, transaction as db_transaction
from datetime import datetime, timedelta
from posts.models.models import Transactions, Rules
from posts.models.transaction_queue import TransactionQueue
from posts.utils.logging_utils import log_transaction_event
from posts.utils.metrics import (
    transactions_imported,
    transactions_failed,
    notifications_success,
    notifications_failed,
    alerts_total,
    alerts_failed,
    alert_delivery_time
)
import uuid
import operator
import requests
import json
import time

OPERATORS_MAP = {
    '>': operator.gt,
    '<': operator.lt,
    '>=': operator.ge,
    '<=': operator.le,
    '==': operator.eq,
}

# -----------------------
# Логгеры и сериализация
# -----------------------
def ensure_correlation_id(tx_obj=None, corr_id=None):
    if tx_obj and getattr(tx_obj, "correlation_id", None):
        return tx_obj.correlation_id
    return corr_id or str(uuid.uuid4())

def serialize_transaction(tx: dict) -> dict:
    serialized = {}
    for k, v in tx.items():
        serialized[k] = v.isoformat() if isinstance(v, datetime) else v
    return serialized

def log_safe(tx_id, corr_id, level, component, message, data=None):
    try:
        log_transaction_event(tx_id, corr_id, level, component, message, data=data)
    except Exception as e:
        print(f"[LOG_FAIL] ({component}/{level}) {tx_id}: {e} | {message}")

def log_queue_event(tx_id, queue_status, correlation_id=None, message=None):
    log_safe(tx_id, correlation_id or str(uuid.uuid4()), "INFO", "queue",
             message or f"→ Очередь: статус обновлён на {queue_status}")

# -----------------------
# Обновление статуса очереди
# -----------------------
def update_queue_status(transaction_id, status, correlation_id):
    try:
        queue_item, _ = TransactionQueue.objects.get_or_create(
            transaction_id=transaction_id,
            defaults={"status": status, "data": {}}
        )
        queue_item.status = status
        queue_item.save(update_fields=["status"])
        log_queue_event(transaction_id, status, correlation_id)
        return queue_item
    except Exception as e:
        log_queue_event(transaction_id, "failed", correlation_id, f"❌ Ошибка обновления очереди: {e}")
        return None

# -----------------------
# Отправка уведомлений
# -----------------------
def send_notification(transaction_id, tx, correlation_id, reason=None, queue_item=None):
    try:
        log_safe(transaction_id, correlation_id, "INFO", "notification",
                 f"📤 Отправка уведомления. Причина: {reason or 'обычная'}")

        tx_serialized = serialize_transaction(tx)
        payload = {
            "id": transaction_id,
            "details": json.dumps(tx_serialized),
            "severity": "0.9"
        }
        if reason:
            payload["reason"] = reason

        response = requests.post("http://api:3000/notifications/create", json=payload, timeout=5)
        response.raise_for_status()

        notifications_success.inc()
        alerts_total.labels(severity="critical").inc()

        log_safe(transaction_id, correlation_id, "INFO", "notification",
                 f"✅ Уведомление отправлено (status={response.status_code})")

        if queue_item:
            queue_item.status = "processed"
            queue_item.save(update_fields=["status"])
            log_queue_event(transaction_id, "processed", correlation_id, "Очередь: уведомление обработано")

    except Exception as e:
        notifications_failed.inc()
        alerts_failed.inc()
        log_safe(transaction_id, correlation_id, "ERROR", "notification", f"❌ Ошибка отправки уведомления: {e}")
        if queue_item:
            queue_item.status = "failed"
            queue_item.save(update_fields=["status"])
        raise

# -----------------------
# Применение правил
# -----------------------
def apply_rules(tx_obj):
    triggered_rules = []
    rules = Rules.objects.filter(is_active=True)
    log_safe(tx_obj.transaction_id, tx_obj.correlation_id, "INFO", "rules",
             f"🔍 Проверка правил. Активных правил: {rules.count()}")

    for rule in rules:
        try:
            log_safe(tx_obj.transaction_id, tx_obj.correlation_id, "DEBUG", "rules",
                     f"→ Проверяется правило '{rule.name}' ({rule.rule_type})")

            if rule.rule_type == "threshold":
                op_func = OPERATORS_MAP.get(rule.operator, operator.gt)
                if op_func(float(tx_obj.amount), float(rule.threshold_value)):
                    triggered_rules.append(rule)

            elif rule.rule_type == "pattern":
                window_start = tx_obj.timestamp - timedelta(minutes=rule.pattern_window_minutes or 0)
                recent_tx = Transactions.objects.filter(
                    sender_account=tx_obj.sender_account,
                    timestamp__gte=window_start
                )
                count = recent_tx.count()
                total = recent_tx.aggregate(total=models.Sum("amount"))["total"] or 0
                if count >= (rule.pattern_max_count or 0) or total > (rule.pattern_max_amount or 0):
                    triggered_rules.append(rule)

            elif rule.rule_type == "composite":
                match_all = True
                for cond in rule.composite_conditions or []:
                    if cond["type"] == "threshold":
                        op = OPERATORS_MAP.get(cond.get("operator", ">"), operator.gt)
                        if not op(tx_obj.amount, cond.get("value", 0)):
                            match_all = False
                    elif cond["type"] == "time_range":
                        hour = tx_obj.timestamp.hour
                        if not (cond.get("start", 0) <= hour <= cond.get("end", 23)):
                            match_all = False
                if match_all:
                    triggered_rules.append(rule)

        except Exception as e:
            log_safe(tx_obj.transaction_id, tx_obj.correlation_id, "ERROR", "rules",
                     f"Ошибка при проверке правила {rule.name}: {e}")

    log_safe(tx_obj.transaction_id, tx_obj.correlation_id, "INFO", "rules",
             f"Проверка завершена. Сработало {len(triggered_rules)}: {[r.name for r in triggered_rules]}")
    return triggered_rules

# -----------------------
# Импорт транзакций с метриками
# -----------------------
def import_transactions(data: list, source: str = "api_or_admin", task_id: str = None) -> dict:
    imported, failed = 0, 0
    total = len(data)

    queue_item = None
    if task_id:
        queue_item, _ = TransactionQueue.objects.get_or_create(
            transaction_id=task_id,
            defaults={"status": "queued", "data": {"progress": 0}}
        )

    log_safe(task_id, None, "INFO", "import", f"🚀 Начало импорта {total} транзакций (источник={source})")

    for i, tx in enumerate(data, start=1):
        start_time = time.time()
        transaction_id = tx.get("transaction_id") or str(uuid.uuid4())
        correlation_id = tx.get("correlation_id") or str(uuid.uuid4())
        tx_serialized = serialize_transaction(tx)

        try:
            timestamp = tx.get("timestamp")
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                timestamp = timezone.make_aware(timestamp)

            defaults = {
                "correlation_id": correlation_id,
                "timestamp": timestamp,
                "sender_account": tx.get("sender_account", "UNKNOWN_SENDER"),
                "receiver_account": tx.get("receiver_account", "UNKNOWN_RECEIVER"),
                "amount": tx.get("amount", 0.0),
                "transaction_type": tx.get("transaction_type", "UNKNOWN"),
                "location": tx.get("location"),
                "device_used": tx.get("device_used", "unspecified"),
                "is_fraud": False,
                "fraud_type": None,
                "status": tx.get("status", "NEW"),
                "api_source": source,
            }

            with db_transaction.atomic():
                obj, created = Transactions.objects.update_or_create(transaction_id=transaction_id, defaults=defaults)
                triggered = apply_rules(obj)

                if triggered:
                    obj.is_fraud = True
                    obj.fraud_type = ", ".join([r.name for r in triggered])
                    obj.save(update_fields=["is_fraud", "fraud_type"])
                    reason = f"⚠️ Сработали правила: {obj.fraud_type}"
                    send_notification(transaction_id, tx, correlation_id, reason=reason)

                obj.processing_time_ms = (time.time() - start_time) * 1000
                obj.save(update_fields=["processing_time_ms"])

                transactions_imported.inc()
                alert_delivery_time.observe(time.time() - start_time)
                imported += 1

        except Exception as e:
            failed += 1
            transactions_failed.inc()
            log_safe(transaction_id, correlation_id, "ERROR", "import",
                     f"❌ Ошибка обработки транзакции: {e}", data=tx_serialized)

        # обновляем прогресс
        if queue_item:
            progress = round((i / total) * 100, 2)
            queue_item.status = "processing"
            queue_item.data = {
                "progress": progress,
                "imported": imported,
                "failed": failed,
                "total": total
            }
            queue_item.save(update_fields=["status", "data"])

    # Завершаем импорт
    if queue_item:
        queue_item.status = "completed" if failed == 0 else "completed_with_errors"
        queue_item.data["progress"] = 100
        queue_item.save(update_fields=["status", "data"])

    log_safe(task_id, None, "INFO", "import",
             f"✅ Импорт завершён: {imported}/{total} успешно, {failed} ошибок")

    return {"imported": imported, "failed": failed, "status": "completed" if failed == 0 else "completed_with_errors"}
