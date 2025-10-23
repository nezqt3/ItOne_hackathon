from django.utils import timezone
from django.db import models
from django.db import transaction as db_transaction
from datetime import datetime, timedelta
from posts.models.models import Transactions, Metric, Rules
from posts.models.transaction_queue import TransactionQueue
from posts.utils.logging_utils import log_transaction_event
from posts.utils.metrics import alert_delivery_time
import uuid
import operator
import requests, json
import time

OPERATORS_MAP = {
    '>': operator.gt,
    '<': operator.lt,
    '>=': operator.ge,
    '<=': operator.le,
    '==': operator.eq,
}

def serialize_transaction(tx: dict) -> dict:
    """Преобразуем все datetime в строки ISO для JSON"""
    serialized = {}
    for k, v in tx.items():
        if isinstance(v, datetime):
            serialized[k] = v.isoformat()
        else:
            serialized[k] = v
    return serialized


def log_queue_event(tx_id, queue_status, correlation_id=None, message=None):
    try:
        log_transaction_event(
            transaction_id=tx_id,
            correlation_id=correlation_id or str(uuid.uuid4()),
            level="INFO",
            component="queue",
            message=message or f"Статус очереди: {queue_status}"
        )
    except Exception:
        pass


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
        log_queue_event(transaction_id, "failed", correlation_id,
                        message=f"Ошибка обновления статуса очереди: {e}")
        return None


def send_notification(transaction_id, tx, correlation_id, reason=None, queue_item=None):
    """Отправка уведомления и обновление очереди"""
    try:
        tx_serialized = serialize_transaction(tx)
        payload = {
            "id": transaction_id,
            "details": json.dumps(tx_serialized),
            "severity": "0.9"
        }
        if reason:
            payload["reason"] = reason

        response = requests.post(
            "http://api:3000/notifications/create",
            json=payload,
            timeout=5
        )
        response.raise_for_status()
        Metric.objects.get_or_create(name="notifications_success_total")[0].increment()
        Metric.objects.get_or_create(name="alerts_total_critical")[0].increment()
        log_transaction_event(transaction_id, correlation_id, "INFO", "notification",
                              f"Уведомление успешно отправлено: {reason or 'без причины'}")
    except Exception as e:
        Metric.objects.get_or_create(name="alerts_failed_total")[0].increment()
        log_transaction_event(transaction_id, correlation_id, "ERROR", "notification",
                              f"Ошибка отправки уведомления: {e}")
        raise


def apply_rules(tx_obj):
    """
    Проверяем транзакцию по активным правилам:
    threshold, pattern, composite.
    Для threshold учитывается оператор и значение из правила.
    """
    triggered_rules = []
    rules = Rules.objects.filter(is_active=True)
    
    for rule in rules:
        if rule.rule_type == "threshold":
            op_func = OPERATORS_MAP.get(rule.operator, operator.gt)
            if rule.threshold_value is not None and op_func(float(tx_obj.amount), float(rule.threshold_value)):
                triggered_rules.append(rule)
                
        elif rule.rule_type == "pattern":
            window_minutes = rule.pattern_window_minutes or 0
            window_start = tx_obj.timestamp - timedelta(minutes=window_minutes)
            recent_tx = Transactions.objects.filter(
                sender_account=tx_obj.sender_account,
                timestamp__gte=window_start
            )
            recent_tx_count = recent_tx.count()
            total_amount = recent_tx.aggregate(total=models.Sum("amount"))["total"] or 0
            if recent_tx_count >= (rule.pattern_max_count or 0) or total_amount > (rule.pattern_max_amount or 0):
                triggered_rules.append(rule)
                
        elif rule.rule_type == "composite":
            conditions = rule.composite_conditions or []
            match_all = True
            for cond in conditions:
                if cond["type"] == "threshold":
                    cond_op = OPERATORS_MAP.get(cond.get("operator", ">"), operator.gt)
                    if not cond_op(tx_obj.amount, cond.get("value", 0)):
                        match_all = False
                elif cond["type"] == "time_range":
                    hour = tx_obj.timestamp.hour
                    if not (cond.get("start", 0) <= hour <= cond.get("end", 23)):
                        match_all = False
            if match_all:
                triggered_rules.append(rule)
    
    return triggered_rules


def import_transactions(data: list, source: str = "api_or_admin") -> dict:
    imported_count = 0
    failed_count = 0

    for tx in data:
        start_time = time.time()
        transaction_id = tx.get("transaction_id") or str(uuid.uuid4())
        correlation_id = tx.get("correlation_id") or str(uuid.uuid4())
        tx_serialized = serialize_transaction(tx)

        log_transaction_event(transaction_id, correlation_id, "INFO", "import",
                              "Начало обработки транзакции", data=tx_serialized)

        # Обработка timestamp
        timestamp = tx.get("timestamp")
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                timestamp = timezone.now()
                log_transaction_event(transaction_id, correlation_id, "WARNING", "import",
                                      "Не удалось разобрать timestamp, используется текущее время")
        if timestamp.tzinfo is None:
            timestamp = timezone.make_aware(timestamp)

        defaults = {
            "correlation_id": correlation_id,
            "timestamp": timestamp,
            "sender_account": tx.get("sender_account", "UNKNOWN_SENDER"),
            "receiver_account": tx.get("receiver_account", "UNKNOWN_RECEIVER"),
            "amount": tx.get("amount", 0.0),
            "transaction_type": tx.get("transaction_type", "UNKNOWN"),
            "merchant_category": tx.get("merchant_category"),
            "location": tx.get("location"),
            "device_used": tx.get("device_used", "unspecified"),
            "is_fraud": False,
            "fraud_type": None,
            "time_since_last_transaction": tx.get("time_since_last_transaction"),
            "spending_deviation_score": tx.get("spending_deviation_score"),
            "velocity_score": tx.get("velocity_score"),
            "geo_anomaly_score": tx.get("geo_anomaly_score"),
            "payment_channel": tx.get("payment_channel", "digital"),
            "ip_address": tx.get("ip_address"),
            "device_hash": tx.get("device_hash"),
            "status": tx.get("status", "NEW"),
            "processed_by": tx.get("processed_by"),
            "api_source": source,
        }

        try:
            with db_transaction.atomic():
                obj, created = Transactions.objects.update_or_create(
                    transaction_id=transaction_id,
                    defaults=defaults
                )

                # Применяем правила
                triggered_rules = apply_rules(obj)
                if triggered_rules:
                    obj.is_fraud = True
                    obj.fraud_type = ", ".join([r.name for r in triggered_rules])
                    obj.save(update_fields=["is_fraud", "fraud_type"])
                    reason = f"Сработали правила: {obj.fraud_type}"
                    log_transaction_event(transaction_id, correlation_id, "WARN", "rules",
                                          reason)
                    # Отправляем уведомление о мошенничестве
                    send_notification(transaction_id, tx, correlation_id, reason=reason)

                if obj.status == "PROCESSED" and not obj.processed_at:
                    obj.processed_at = timezone.now()
                    if obj.received_at:
                        obj.processing_time_ms = (obj.processed_at - obj.received_at).total_seconds() * 1000
                    obj.save(update_fields=["processed_at", "processing_time_ms"])

                Metric.objects.get_or_create(name="transactions_imported_total")[0].increment()
                imported_count += 1

            log_transaction_event(transaction_id, correlation_id, "INFO", "ingest",
                                  f"Транзакция {'создана' if created else 'обновлена'}",
                                  data=tx_serialized)

            # Управление очередью
            queue_item = update_queue_status(transaction_id, "queued", correlation_id)
            update_queue_status(transaction_id, "processing", correlation_id)

            # Если транзакция не мошенническая — уведомление обычное
            if not triggered_rules:
                send_notification(transaction_id, tx, correlation_id, queue_item=queue_item)

            # Логирование времени обработки
            obj.processing_time_ms = (time.time() - start_time) * 1000
            obj.save(update_fields=["processing_time_ms"])
            alert_delivery_time.observe(time.time() - start_time)
            log_transaction_event(transaction_id, correlation_id, "INFO", "metrics",
                                  f"Время обработки транзакции: {obj.processing_time_ms:.2f} ms")

        except Exception as e:
            failed_count += 1
            Metric.objects.get_or_create(name="transactions_failed_total")[0].increment()
            log_transaction_event(transaction_id, correlation_id, "ERROR", "import",
                                  f"Ошибка импорта или уведомления: {e}", data=tx_serialized)

    return {"imported": imported_count, "failed": failed_count}
