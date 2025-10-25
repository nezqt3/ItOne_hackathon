from django.contrib import admin
from django.urls import path, reverse
from django.utils import timezone
from django.shortcuts import render, redirect
from django.contrib import messages
from django.db import transaction as db_transaction
from datetime import datetime
from django.http import HttpResponse
import csv
import requests, json, time
from posts.utils.transaction_importer import import_transactions
from prometheus_client import Counter, Histogram
from posts.utils.queue_manager import QueueManager
from posts.utils.logging_utils import log_transaction_event

from posts.models.transaction_queue import TransactionQueue
from posts.models.models import (
    Metric,
    Transactions,
    TransactionsTypes,
    TransactionLog,
    Rules
)
from posts.utils.metrics import (
    transactions_imported,
    transactions_failed,
    notifications_success,
    notifications_failed,
    alerts_total,
    alerts_failed,
    alert_delivery_time
)

# --- Метрики ---

class TransactionsAdmin(admin.ModelAdmin):
    list_display = (
        'transaction_id', 'timestamp', 'sender_account',
        'receiver_account', 'amount', 'transaction_type', 'status'
    )
    list_filter = ('transaction_type', 'status', 'timestamp')
    search_fields = ('transaction_id', 'correlation_id', 'sender_account', 'receiver_account')
    readonly_fields = ('correlation_id',)
    list_filter_frauds = ("is_fraud", "status")
    
    change_list_template = "posts/templates/admin/posts/transactions/change_list_metrics.html"
    
    actions = ["view_logs_for_transaction"]
    
    def export_csv_view(self, request):
        """Экспорт всех транзакций в CSV и отдача пользователю"""
        # Создаём HttpResponse с нужным MIME-типом
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="transactions.csv"'

        writer = csv.writer(response)
        # Заголовки столбцов
        writer.writerow([
            'transaction_id', 'timestamp', 'sender_account', 'receiver_account',
            'amount', 'transaction_type', 'status', 'correlation_id', 'merchant_category', 'location'
        ])

        # Берём все транзакции из базы
        for tx in Transactions.objects.all().order_by('timestamp'):
            writer.writerow([
                tx.transaction_id,
                tx.timestamp.isoformat() if tx.timestamp else '',
                tx.sender_account,
                tx.receiver_account,
                tx.amount,
                tx.transaction_type,
                tx.status,
                tx.correlation_id,
                getattr(tx, 'merchant_category', ''),
                getattr(tx, 'location', '')
            ])

        return response

    def view_logs_for_transaction(self, request, queryset):
        """Позволяет в админке быстро посмотреть логи по correlation_id"""
        from posts.models.models import TransactionLog
        for tx in queryset:
            logs = TransactionLog.objects.filter(correlation_id=tx.correlation_id).order_by("created_at")
            if not logs.exists():
                self.message_user(request, f"⛔ Логи не найдены для correlation_id={tx.correlation_id}")
            else:
                for log in logs:
                    self.message_user(
                        request,
                        f"[{log.component.upper()}][{log.level}] {log.message}"
                    )
    view_logs_for_transaction.short_description = "Показать логи транзакции"

    def has_add_permission(self, request):
        return False
    
    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}

        # Метрики
        imported_metric, _ = Metric.objects.get_or_create(name='transactions_imported_total')
        failed_metric, _ = Metric.objects.get_or_create(name='transactions_failed_total')
        notif_success_metric, _ = Metric.objects.get_or_create(name='notifications_success_total')
        notif_failed_metric, _ = Metric.objects.get_or_create(name='notifications_failed_total')
        alerts_total_metric, _ = Metric.objects.get_or_create(name='alerts_total_critical')
        alerts_failed_metric, _ = Metric.objects.get_or_create(name='alerts_failed_total')

        # Очередь
        queued = TransactionQueue.objects.filter(status='queued').count()
        processing = TransactionQueue.objects.filter(status='processing').count()
        processed = TransactionQueue.objects.filter(status='processed').count()
        failed_queue = TransactionQueue.objects.filter(status='failed').count()

        # Контекст для шаблона
        extra_context['import_url'] = reverse("admin:import_json")
        extra_context['export_url'] = reverse("admin:transactions_export_csv")
        extra_context['metrics'] = {
            'transactions': {
                'imported': imported_metric.value,
                'failed': failed_metric.value
            },
            'notifications': {
                'success': notif_success_metric.value,
                'failed': notif_failed_metric.value
            },
            'alerts': {
                'total': alerts_total_metric.value,
                'failed': alerts_failed_metric.value
            },
            'queue': {
                'queued': queued,
                'processing': processing,
                'processed': processed,
                'failed': failed_queue
            }
        }

        return super().changelist_view(request, extra_context=extra_context)

    # -----------------------------
    #   КАСТОМНЫЕ URL (импорт)
    # -----------------------------
    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path('export-csv/', self.admin_site.admin_view(self.export_csv_view), name='transactions_export_csv'),
            path('import-json/', self.admin_site.admin_view(self.import_json), name='import_json'),
        ]
        return custom + urls

    # -----------------------------
    #   СОХРАНЕНИЕ МОДЕЛИ
    # -----------------------------
    def save_model(self, request, obj, form, change):
        is_create = not change
        if not obj.timestamp:
            obj.timestamp = timezone.now()

        super().save_model(request, obj, form, change)

        # Лог
        log_transaction_event(
            transaction_id=obj.transaction_id,
            correlation_id=obj.correlation_id,
            level="INFO",
            component="ingest",
            message="Transaction saved via admin",
            data={"source": "admin_ui", "new": is_create}
        )

        if is_create:
            Metric.objects.get_or_create(name='transactions_imported_total')[0].increment()

        # Уведомление
        start_time = time.time()
        try:
            response = requests.post(
                "http://api:3000/notifications/create",
                json={
                    "id": obj.transaction_id,
                    "details": json.dumps({
                        "transaction_id": obj.transaction_id,
                        "correlation_id": obj.correlation_id,
                        "timestamp": obj.timestamp.isoformat(),
                        "sender_account": obj.sender_account,
                        "receiver_account": obj.receiver_account,
                        "amount": obj.amount,
                        "transaction_type": obj.transaction_type,
                        "merchant_category": obj.merchant_category,
                        "location": obj.location,
                    }),
                    "severity": "0.9"
                },
                timeout=5
            )
            response.raise_for_status()
            Metric.objects.get_or_create(name='notifications_success_total')[0].increment()
            Metric.objects.get_or_create(name='alerts_total_critical')[0].increment()
        except Exception as e:
            Metric.objects.get_or_create(name='alerts_failed_total')[0].increment()
            log_transaction_event(
                transaction_id=obj.transaction_id,
                correlation_id=obj.correlation_id,
                level="ERROR",
                component="notification",
                message=f"Notification failed: {e}"
            )
        finally:
            alert_delivery_time.observe(time.time() - start_time)

    # -----------------------------
    #   ИМПОРТ JSON
    # -----------------------------
    def import_json(self, request):
        if request.method == "POST":
            json_file = request.FILES.get("json_file")
            if not json_file:
                Metric.objects.get_or_create(name='transactions_failed_total')[0].increment()
                messages.error(request, "❌ Не выбран JSON-файл для импорта")
                return redirect("..")

            try:
                data = json.load(json_file)
                transactions_data = data.get("transactions") if isinstance(data, dict) else data

                # --- используем helper ---
                result = import_transactions(transactions_data, source="admin")

                # --- уведомления админке ---
                if result["failed"] == 0:
                    messages.success(request, f"✅ Импортировано {result['imported']} транзакций")
                else:
                    messages.warning(request, f"⚠️ Импорт завершён с ошибками: "
                                              f"{result['imported']} успешно, {result['failed']} с ошибками")

            except json.JSONDecodeError:
                Metric.objects.get_or_create(name='transactions_failed_total')[0].increment()
                messages.error(request, "Ошибка: неверный формат JSON-файла")
            except Exception as e:
                Metric.objects.get_or_create(name='transactions_failed_total')[0].increment()
                messages.error(request, f"Ошибка импорта: {e}")

            return redirect("..")

        # --- если GET ---
        context = dict(
            self.admin_site.each_context(request),
            title="Импортировать транзакции через API"
        )
        return render(request, "import_json.html", context)
