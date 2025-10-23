from django.db import models
from django.utils import timezone

class TransactionQueue(models.Model):
    STATUS_CHOICES = [
        ('queued', 'Queued'),
        ('processing', 'Processing'),
        ('processed', 'Processed'),
        ('failed', 'Failed'),
    ]

    transaction_id = models.CharField(max_length=64, unique=True)
    correlation_id = models.CharField(max_length=64)
    data = models.JSONField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='queued')
    created_at = models.DateTimeField(default=timezone.now)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"{self.transaction_id} ({self.status})"


class TransactionQueueLog(models.Model):
    transaction = models.ForeignKey(TransactionQueue, on_delete=models.CASCADE, related_name='logs')
    event = models.CharField(max_length=100)
    message = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    component = models.CharField(max_length=50, default='queue')

    def __str__(self):
        return f"[{self.created_at}] {self.event}"
