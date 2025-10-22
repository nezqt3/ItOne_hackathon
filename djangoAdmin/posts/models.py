from django.db import models

class Transactions(models.Model):
    transaction_id = models.CharField(primary_key=True, unique=True, max_length=50)
    correlation_id = models.CharField(max_length=50)
    timestamp = models.DateTimeField()
    sender_account = models.CharField(max_length=255)
    receiver_account = models.CharField(max_length=255)
    amount = models.IntegerField()
    transaction_type = models.CharField(max_length=30)
    merchant_category = models.CharField(max_length=30)
    location = models.CharField(max_length=30)

class TransactionsTypes(models.Model):
    transaction_id = models.CharField(primary_key=True, unique=True, max_length=50)
    transaction_type = models.CharField(max_length=30)