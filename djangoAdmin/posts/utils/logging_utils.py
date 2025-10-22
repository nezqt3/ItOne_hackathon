from posts.models import TransactionLog, Transactions
from django.utils import timezone

def log_transaction_event(
    transaction_id: str,
    correlation_id: str,
    level: str,
    component: str,
    message: str,
    data: dict = None
):
    """Создает запись лога для транзакции"""
    try:
        tx = Transactions.objects.get(transaction_id=transaction_id)
    except Transactions.DoesNotExist:
        return  # транзакция еще не в БД — можно пропустить или логировать отдельно

    TransactionLog.objects.create(
        transaction=tx,
        correlation_id=correlation_id,
        level=level,
        component=component,
        message=message,
        structured_data=data or {},
        created_at=timezone.now()
    )