from django.contrib import admin
from django.urls import path
from django.shortcuts import render, redirect
from django.contrib import messages
import json
from django.db import transaction as db_transaction
from django.urls import reverse
import requests
from .models import Transactions, TransactionsTypes
from django.utils.html import format_html

@admin.register(Transactions)
class TransactionsAdmin(admin.ModelAdmin):
    list_display = ('transaction_id', 'timestamp', 'sender_account', 'receiver_account', 'amount', 'transaction_type')

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['import_url'] = reverse('admin:import_json')
        return super().changelist_view(request, extra_context=extra_context)

    def get_urls(self):
        urls = super().get_urls()
        my_urls = [
            path('import-json/', self.admin_site.admin_view(self.import_json), name='import_json'),
        ]
        return my_urls + urls

    def import_json(self, request):
        """Загрузка JSON → API → сохранение в БД"""
        if request.method == "POST":
            json_file = request.FILES.get("json_file")
            if not json_file:
                messages.error(request, "⚠️ Файл не выбран")
                return redirect("..")

            try:
                data = json.load(json_file)

                api_url = "http://api:3000/transactions/import-json"
                response = requests.post(api_url, json=data)

                if response.status_code != 200:
                    messages.error(request, f"❌ Ошибка API: {response.text}")
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
                        Transactions.objects.update_or_create(
                            transaction_id=tx.get("transaction_id"),
                            defaults={
                                "correlation_id": tx.get("correlation_id"),
                                "timestamp": tx.get("timestamp"),
                                "sender_account": tx.get("sender_account"),
                                "receiver_account": tx.get("receiver_account"),
                                "amount": tx.get("amount"),
                                "transaction_type": tx.get("transaction_type"),
                                "merchant_category": tx.get("merchant_category"),
                                "location": tx.get("location"),
                            },
                        )
                        imported_count += 1

                messages.success(request, f"✅ Импортировано {imported_count} транзакций через API")

            except json.JSONDecodeError:
                messages.error(request, "Ошибка: неверный формат JSON")
            except requests.exceptions.RequestException as e:
                messages.error(request, f"Ошибка подключения к API: {e}")
            except Exception as e:
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
