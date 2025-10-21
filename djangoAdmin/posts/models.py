from django.db import models

class Transactions(models.Model):
    transaction_id = models.IntegerField(primary_key=True)
    correlation_id = models.IntegerField()
    timestamp = models.DateTimeField()
    sender_account = models.CharField(max_length=255)
    receiver_account = models.CharField(max_length=255)
    amount = models.IntegerField(max_length=30)
    transaction_type = models.CharField(max_length=30)
    merchant_category = models.CharField(max_length=30)
    location = models.CharField(max_length=30)

class TransactionsTypes(models.Model):
    transaction_id = models.IntegerField(primary_key=True)
    transaction_type = models.CharField(max_length=30)