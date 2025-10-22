from django.db import models
from django.utils import timezone
from simple_history.models import HistoricalRecords

class Rules(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    history = HistoricalRecords()  # для истории изменений

    def __str__(self):
        return self.name

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
    
    STATUS_CHOICES = [
        ('NEW', 'New'),
        ('PROCESSED', 'Processed'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='NEW')

    def __str__(self):
        return f"{self.transaction_id} ({self.amount})"
    
class TransactionsTypes(models.Model):
    transaction_id = models.CharField(primary_key=True, unique=True, max_length=50)
    transaction_type = models.CharField(max_length=30)
    
    def __str__(self):
        return f"{self.transaction_id} → {self.transaction_type}"
    
class TransactionLog(models.Model):
    LEVEL_CHOICES = [
        ('INFO', 'Info'),
        ('WARN', 'Warning'),
        ('ERROR', 'Error'),
    ]

    COMPONENT_CHOICES = [
        ('ingest', 'Ingest'),
        ('queue', 'Queue'),
        ('rules', 'Rules Engine'),
        ('notify', 'Notification'),
    ]

    id = models.AutoField(primary_key=True)
    transaction = models.ForeignKey(Transactions, on_delete=models.CASCADE, related_name='logs')
    correlation_id = models.CharField(max_length=50)
    level = models.CharField(max_length=10, choices=LEVEL_CHOICES, default='INFO')
    component = models.CharField(max_length=20, choices=COMPONENT_CHOICES)
    message = models.TextField()
    structured_data = models.JSONField(default=dict)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"[{self.level}] {self.component} — {self.transaction.transaction_id}"
    
class Metric(models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(default=0)
    
    def increment(self, amount=1):
        self.value += amount
        self.save()