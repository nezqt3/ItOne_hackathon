from posts.models.models import TransactionLog, Transactions
from django.utils import timezone

def log_transaction_event(
    transaction_id: str = None,
    correlation_id: str = None,
    level: str = None,
    component: str = None,
    message: str = None,
    data: dict = None
):
    """Создает запись лога для транзакции или общего события"""
    if transaction_id:
        try:
            tx = Transactions.objects.get(transaction_id=transaction_id)
        except Transactions.DoesNotExist:
            tx = None
    else:
        tx = None

    TransactionLog.objects.create(
        transaction=tx,  # может быть None
        correlation_id=correlation_id or f"GEN-{timezone.now().strftime('%Y%m%d%H%M%S')}",
        level=level or "INFO",
        component=component or "general",
        message=message or "",
        structured_data=data or {},
        created_at=timezone.now()
    )