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

class TransactionLogAdmin(admin.ModelAdmin):
    list_display = ('transaction', 'component', 'level', 'message', 'created_at')
    list_filter = ('level', 'component', 'created_at')
    search_fields = ('transaction__transaction_id', 'message', 'correlation_id')
    readonly_fields = ('transaction', 'correlation_id', 'level', 'component', 'message', 'structured_data', 'created_at')