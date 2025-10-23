import time
import logging
from django.utils import timezone
from posts.models.transaction_queue import TransactionQueue, TransactionQueueLog

logger = logging.getLogger(__name__)

class QueueManager:
    @staticmethod
    def enqueue(transaction_id, correlation_id, data):
        tx, created = TransactionQueue.objects.get_or_create(
            transaction_id=transaction_id,
            defaults={'correlation_id': correlation_id, 'data': data, 'status': 'queued'}
        )
        QueueManager.log_event(tx, "Queued", f"Transaction queued (new={created})")
        return tx

    @staticmethod
    def log_event(transaction, event, message, component="queue"):
        TransactionQueueLog.objects.create(
            transaction=transaction,
            event=event,
            message=message,
            component=component
        )
        logger.info(f"[{component}] {event}: {message}")

    @staticmethod
    def process_next():
        tx = TransactionQueue.objects.filter(status='queued').order_by('created_at').first()
        if not tx:
            return None

        tx.status = 'processing'
        tx.started_at = timezone.now()
        tx.save(update_fields=['status', 'started_at'])
        QueueManager.log_event(tx, "Processing started", "Transaction taken from queue")

        try:
            # Симуляция обработки
            time.sleep(1)
            tx.status = 'processed'
            tx.completed_at = timezone.now()
            tx.save(update_fields=['status', 'completed_at'])
            QueueManager.log_event(tx, "Processed successfully", "Transaction processed")
        except Exception as e:
            tx.status = 'failed'
            tx.error_message = str(e)
            tx.save(update_fields=['status', 'error_message'])
            QueueManager.log_event(tx, "Processing failed", str(e), component="worker")

        return tx
