import threading, time, json
from posts.models.export_task import ImportExportTask
from posts.utils.transaction_importer import import_transactions  # твой текущий модуль, где лежит функция
from posts.utils.logging_utils import log_transaction_event

def background_import(data, task_id: str):
    """Запускает import_transactions с обновлением прогресса"""
    task = ImportExportTask.objects.get(id=task_id)
    task.status = "processing"
    task.total_items = len(data)
    task.save(update_fields=["status", "total_items"])

    processed, failed = 0, 0

    for i, tx in enumerate(data, start=1):
        try:
            result = import_transactions([tx])  # твоя функция уже обрабатывает список
            processed += result.get("imported", 0)
            failed += result.get("failed", 0)
        except Exception as e:
            failed += 1
            log_transaction_event(None, None, "ERROR", "import_task", f"Ошибка транзакции в фоне: {e}")

        # обновляем каждые 10 итераций или по завершении
        if i % 10 == 0 or i == len(data):
            task.update_progress(i)
            time.sleep(0.05)

    task.status = "completed" if failed == 0 else "failed"
    task.message = f"Импорт завершён: {processed} успешно, {failed} с ошибками"
    task.progress = 100.0
    task.save(update_fields=["status", "message", "progress"])
