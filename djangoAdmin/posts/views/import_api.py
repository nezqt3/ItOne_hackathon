import json, uuid, threading
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from posts.models.transaction_queue import TransactionQueue
from posts.utils.transaction_importer import import_transactions

@csrf_exempt
def start_import(request):
    """Старт асинхронного импорта JSON"""
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    file = request.FILES.get("file")
    if not file:
        return JsonResponse({"error": "Файл не найден"}, status=400)

    try:
        data = json.load(file)
        if not isinstance(data, list):
            return JsonResponse({"error": "Ожидался список транзакций"}, status=400)

        task_id = str(uuid.uuid4())
        TransactionQueue.objects.create(
            transaction_id=task_id,
            status="queued",
            data={"progress": 0, "total": len(data)}
        )

        # Запуск импорта в отдельном потоке (чтобы не блокировать HTTP)
        threading.Thread(target=import_transactions, args=(data, "admin_json", task_id), daemon=True).start()

        return JsonResponse({"task_id": task_id})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def import_progress(request, task_id):
    """Возвращает текущий статус и прогресс импорта"""
    try:
        queue_item = TransactionQueue.objects.get(transaction_id=task_id)
        data = queue_item.data or {}
        progress = data.get("progress", 0)
        status = queue_item.status
        message = (
            f"Импорт завершён ({data.get('imported', 0)}/{data.get('total', 0)} успешно)"
            if status.startswith("completed")
            else None
        )

        return JsonResponse({
            "status": status,
            "progress": progress,
            "imported": data.get("imported", 0),
            "failed": data.get("failed", 0),
            "total": data.get("total", 0),
            "message": message,
            "error": data.get("error")
        })
    except TransactionQueue.DoesNotExist:
        return JsonResponse({"error": "Импорт не найден"}, status=404)