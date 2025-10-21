from django.contrib import admin
from .models import Transactions, TransactionsTypes

admin.site.register(Transactions)
admin.site.register(TransactionsTypes)