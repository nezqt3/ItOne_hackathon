from django.contrib import admin
from django.urls import path, reverse
from django.shortcuts import render, redirect
from django.contrib import messages
import json
from prometheus_client import Counter, Histogram
from posts.utils.logging_utils import log_transaction_event
from django.db import transaction as db_transaction
from posts.models import Metric
import requests
from posts.models import TransactionsTypes
import time

class TransactionsTypesAdmin(admin.ModelAdmin):
    list_display = ('transaction_id', 'transaction_type')
    list_filter = ('transaction_type',)
    search_fields = ('transaction_id', 'transaction_type')
    
    change_list_template = "posts/templates/admin/posts/transactions/change_list_metrics_types.html"

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        
        created_metric, _ = Metric.objects.get_or_create(name='transactions_types_created_total')
        updated_metric, _ = Metric.objects.get_or_create(name='transactions_types_updated_total')
        
        extra_context['metrics'] = {
            'types': {
                'created': created_metric.value,
                'updated': updated_metric.value
            }
        }
        return super().changelist_view(request, extra_context=extra_context)

    def save_model(self, request, obj, form, change):
        is_create = not change

        start_time = time.time()

        super().save_model(request, obj, form, change)
        
        elapsed_time = time.time() - start_time  # Время выполнения
        elapsed_ms = round(elapsed_time * 1000, 2)

        log_transaction_event(
            transaction_id=obj.transaction_id,
            level="INFO",
            component="ingest_type",
            message="TransactionType saved via admin",
            data={"source": "admin_ui", "new": is_create, "elapsed_time_ms": elapsed_ms}
        )

        # Метрики
        if is_create:
            Metric.objects.get_or_create(name='transactions_types_created_total')[0].increment()
        else:
            Metric.objects.get_or_create(name='transactions_types_updated_total')[0].increment()
