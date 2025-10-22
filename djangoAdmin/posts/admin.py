from django.contrib import admin
from django.urls import path
from django.utils import timezone
from django.shortcuts import render, redirect
from django.contrib import messages
import json
from prometheus_client import Counter, Histogram
from .utils.logging_utils import log_transaction_event
from django.db import transaction as db_transaction
import requests
from datetime import datetime
from .models import Transactions, TransactionsTypes, TransactionLog, Rules
import time

transactions_imported = Counter('transactions_imported_total', 'Total number of imported transactions')
transactions_failed = Counter('transactions_failed_total', 'Number of failed imports')
transactions_types_created = Counter('transactions_types_created_total', 'Number of transaction types created')
transactions_types_updated = Counter('transactions_types_updated_total', 'Number of transaction types updated')
notifications_success = Counter('notifications_success_total', 'Успешные уведомления')
notifications_failed = Counter('notifications_failed_total', 'Неудачные уведомления')

alerts_total = Counter(
    'alerts_total', 'Общее количество сгенерированных алертов', ['severity']
)
alerts_failed = Counter(
    'alerts_failed_total', 'Количество алерто   в, которые не удалось обработать'
)

alert_delivery_time = Histogram(
    'alert_delivery_seconds', 'Время доставки уведомлений по алертам'
)

@admin.register(Transactions)
class TransactionsAdmin(admin.ModelAdmin):
    list_display = ('transaction_id','timestamp','sender_account','receiver_account','amount','transaction_type','status')
    list_filter = ('transaction_type','status','timestamp')
    search_fields = ('transaction_id','correlation_id','sender_account','receiver_account')

    change_list_template = "admin/posts/transactions/change_list_metrics.html"

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}

        extra_context['import_url'] = "import-json/"
        extra_context['metrics'] = {
            'transactions': {
                'imported': int(transactions_imported._value.get()),
                'failed': int(transactions_failed._value.get())
            },
            'types': {
                'created': int(transactions_types_created._value.get()),
                'updated': int(transactions_types_updated._value.get())
            },
            'notifications': {
                'success': int(notifications_success._value.get()),
                'failed': int(notifications_failed._value.get())
            },
            'alerts': {
                'total': int(sum([alerts_total.labels(severity=s)._value.get() for s in ['CRITICAL']])),
                'failed': int(alerts_failed._value.get())
            }
        }

        return super().changelist_view(request, extra_context=extra_context)

    def get_urls(self):
        urls = super().get_urls()
        my_urls = [
            path('import-json/', self.admin_site.admin_view(self.import_json), name='import_json'),
        ]
        return my_urls + urls

    def save_model(self, request, obj, form, change):
        is_create = not change

        if not obj.timestamp:
            obj.timestamp = timezone.now()

        super().save_model(request, obj, form, change)

        # Логирование события
        log_transaction_event(
            transaction_id=obj.transaction_id,
            correlation_id=obj.correlation_id,
            level="INFO",
            component="ingest",
            message="Transaction saved via admin",
            data={"source": "admin_ui", "new": is_create}
        )

        # Увеличиваем счетчик метрик
        if is_create:
            transactions_imported.inc()
        else:
            # Можно отдельный счетчик для обновлений, если нужно
            pass

        # Отправка уведомления
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
            notifications_success.inc()
            alerts_total.labels(severity='CRITICAL').inc()
        except Exception as e:
            alerts_failed.inc()
            log_transaction_event(
                transaction_id=obj.transaction_id,
                correlation_id=obj.correlation_id,
                level="ERROR",
                component="notification",
                message=f"Failed to send notification/alert: {e}",
            )
        finally:
            alert_delivery_time.observe(time.time() - start_time)

    def import_json(self, request):
        """Загрузка JSON → API → сохранение в БД"""
        if request.method == "POST":
            json_file = request.FILES.get("json_file")
            if not json_file:
                messages.error(request, "⚠️ Файл не выбран")
                transactions_failed.inc()
                return redirect("..")

            try:
                data = json.load(json_file)

                api_url = "http://api:3000/transactions/import-json"
                response = requests.post(api_url, json=data)

                if response.status_code != 200:
                    messages.error(request, f"❌ Ошибка API: {response.text}")
                    transactions_failed.inc()
                    return redirect("..")

                result = response.json()
                imported_data = None

                if isinstance(result, dict) and "transactions" in result:
                    imported_data = result["transactions"]
                elif isinstance(data, dict) and "transactions" in data:
                    imported_data = data["transactions"]
                elif isinstance(data, list):
                    imported_data = data
                else:
                    imported_data = []

                imported_count = 0

                with db_transaction.atomic():
                    for tx in imported_data:
                        
                        timestamp = tx.get("timestamp")
                        if timestamp:
                            if isinstance(timestamp, str):
                                timestamp = datetime.fromisoformat(timestamp)
                            timestamp = timezone.make_aware(timestamp)
                        
                        obj, created = Transactions.objects.update_or_create(
                            transaction_id=tx.get("transaction_id"),
                            defaults={
                                "correlation_id": tx.get("correlation_id"),
                                "timestamp": timestamp,
                                "sender_account": tx.get("sender_account"),
                                "receiver_account": tx.get("receiver_account"),
                                "amount": tx.get("amount"),
                                "transaction_type": tx.get("transaction_type"),
                                "merchant_category": tx.get("merchant_category"),
                                "location": tx.get("location"),
                            },
                        )

                        log_transaction_event(
                            transaction_id=obj.transaction_id,
                            correlation_id=obj.correlation_id,
                            level="INFO",
                            component="ingest",
                            message="Transaction imported successfully",
                            data={"source": "admin_import", "new": created}
                        )

                        imported_count += 1
                        
                        start_time = time.time()
                        try:
                            response = requests.post("http://api:3000/notifications/create", 
                                                     json={
                                "id": tx.get('transaction_id'), 
                                "details": json.dumps(tx),
                                "severity": "0.9"
                                       })
                            response.raise_for_status()
                            notifications_success.inc()
                            alerts_total.labels(severity='CRITICAL').inc()
                        except:
                            alerts_failed.inc()
                            log_transaction_event(
                                transaction_id=obj.transaction_id,
                                correlation_id=obj.correlation_id,
                                level="ERROR",
                                component="notification",
                                message="Failed to send notification/alert",
                            )
                        finally:
                            alert_delivery_time.observe(time.time() - start_time)
                            
                transactions_imported.inc(imported_count)

                messages.success(request, f"✅ Импортировано {imported_count} транзакций через API")

            except json.JSONDecodeError:
                transactions_failed.inc()
                messages.error(request, "Ошибка: неверный формат JSON")
            except requests.exceptions.RequestException as e:
                transactions_failed.inc()
                messages.error(request, f"Ошибка подключения к API: {e}")
            except Exception as e:
                transactions_failed.inc()
                messages.error(request, f"Ошибка импорта: {e}")

            return redirect("..")

        context = dict(
            self.admin_site.each_context(request),
            title="Импортировать транзакции через API",
        )
        return render(request, "import_json.html", context)

@admin.register(TransactionsTypes)
class TransactionsTypesAdmin(admin.ModelAdmin):
    list_display = ('transaction_id', 'transaction_type')
    
    change_from_template = "admin/transactions/change_list_graph.html"

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['metrics'] = {
            'transactions': {
                'imported': int(transactions_imported._value.get()),
                'failed': int(transactions_failed._value.get())
            },
            'types': {
                'created': int(transactions_types_created._value.get()),
                'updated': int(transactions_types_updated._value.get())
            },
            'notifications': {
                'success': int(notifications_success._value.get()),
                'failed': int(notifications_failed._value.get())
            },
            'alerts': {
                'total': int(sum([alerts_total.labels(severity=s)._value.get() for s in ['CRITICAL']])),
                'failed': int(alerts_failed._value.get())
            }
        }
        return super().changelist_view(request, extra_context=extra_context)

@admin.register(TransactionLog)
class TransactionLogAdmin(admin.ModelAdmin):
    list_display = ('transaction', 'component', 'level', 'message', 'created_at')
    list_filter = ('level', 'component', 'created_at')
    search_fields = ('transaction__transaction_id', 'message', 'correlation_id')
    readonly_fields = ('transaction', 'correlation_id', 'level', 'component', 'message', 'structured_data', 'created_at')
    
@admin.register(Rules)
class RulesAdmin(admin.ModelAdmin):
    list_display = ('name','is_active','created_at','updated_at')
    list_filter = ('is_active',)
    search_fields = ('name','description')
    actions = ['enable_rules','disable_rules']

    def enable_rules(self, request, queryset):
        queryset.update(is_active=True)
        self.message_user(request, f"{queryset.count()} правил включено")
    enable_rules.short_description = "Enable selected rules"

    def disable_rules(self, request, queryset):
        queryset.update(is_active=False)
        self.message_user(request, f"{queryset.count()} правил отключено")
    disable_rules.short_description = "Disable selected rules"
    