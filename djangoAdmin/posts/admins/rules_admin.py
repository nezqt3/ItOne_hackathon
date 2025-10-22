from django.contrib import admin
from django.urls import path
from django.utils import timezone
from django.shortcuts import render, redirect
from django.contrib import messages
import json
from prometheus_client import Counter, Histogram
from posts.utils.logging_utils import log_transaction_event
from django.db import transaction as db_transaction
import requests
from datetime import datetime
from posts.models import Transactions, TransactionsTypes, TransactionLog, Rules
import time
    
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
    