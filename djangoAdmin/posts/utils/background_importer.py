# posts/utils/background_importer.py
import json, threading, time
from posts.models.export_task import ImportExportTask
from posts.models.models import Transactions
from django.utils import timezone

def background_import_json(task_id, file_data):
    """Импорт JSON-файла с обновлением прогресса"""
    task = ImportExportTask.objects.get(id=task_id)
    task.status = "processing"
    task.save(update_fields=['status'])

    try:
        data = json.loads(file_data)
        transactions_data = data.get("transactions") if isinstance(data, dict) else data
        total = len(transactions_data)
        task.total_items = total
        task.save(update_fields=['total_items'])

        imported = 0
        for tx in transactions_data:
            Transactions.objects.create(
                transaction_id=tx.get("transaction_id"),
                timestamp=timezone.now(),
                sender_account=tx.get("sender_account"),
                receiver_account=tx.get("receiver_account"),
                amount=tx.get("amount"),
                transaction_type=tx.get("transaction_type", "UNKNOWN"),
                status="processed",
                correlation_id=tx.get("correlation_id") or f"import-{task.id}"
            )
            imported += 1
            if imported % 10 == 0:
                task.update_progress(imported)
                time.sleep(0.05)

        task.update_progress(total)
        task.status = "completed"
        task.message = f"✅ Импортировано {imported} транзакций"
        task.save()
    except Exception as e:
        task.status = "failed"
        task.message = f"Ошибка импорта: {e}"
        task.save(update_fields=['status', 'message'])
