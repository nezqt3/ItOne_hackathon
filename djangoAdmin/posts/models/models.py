from django.db import models
from django.utils import timezone
from simple_history.models import HistoricalRecords
from django.contrib.auth import get_user_model
import json

RULE_TYPES = [
    ('threshold', 'Threshold'),
    ('pattern', 'Pattern'),
    ('composite', 'Composite'),
]

RULE_OPERATORS = [
    ('>', 'Greater than (>)'),
    ('<', 'Less than (<)'),
    ('>=', 'Greater or equal (>=)'),
    ('<=', 'Less or equal (<=)'),
    ('==', 'Equal (==)'),
]

User = get_user_model()

class Rules(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    rule_type = models.CharField(max_length=20, choices=RULE_TYPES)
    
    # --- Threshold rule ---
    threshold_value = models.FloatField(blank=True, null=True)
    operator = models.CharField(max_length=2, choices=RULE_OPERATORS, blank=True, null=True)

    # --- Pattern rule ---
    pattern_window_minutes = models.IntegerField(blank=True, null=True)
    pattern_max_count = models.IntegerField(blank=True, null=True)
    pattern_max_amount = models.FloatField(blank=True, null=True)

    # --- Composite rule ---
    composite_conditions = models.JSONField(blank=True, null=True, help_text="Список условий для composite")

    created_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="rules_created"
    )
    updated_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="rules_updated"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    history = HistoricalRecords()

    def save(self, *args, **kwargs):
        super_should_save = True

        # Сохраняем только если нужно (чтобы избежать циклов)
        if self.is_active:
            Rules.objects.filter(rule_type=self.rule_type).exclude(pk=self.pk).update(is_active=False)

        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name} ({self.rule_type})"

class Transactions(models.Model):
    # 🔹 Основные поля
    transaction_id = models.CharField(primary_key=True, unique=True, max_length=50)
    correlation_id = models.CharField(max_length=50, blank=True, null=True, db_index=True, help_text="ID корреляции для трассировки между системами")
    timestamp = models.DateTimeField(default=timezone.now)
    sender_account = models.CharField(max_length=255)
    receiver_account = models.CharField(max_length=255)
    amount = models.DecimalField(max_digits=12, decimal_places=2)
    transaction_type = models.CharField(max_length=30)
    merchant_category = models.CharField(max_length=50, blank=True, null=True)
    location = models.CharField(max_length=100, blank=True, null=True)
    device_used = models.CharField(max_length=50, blank=True, null=True)

    # 🔹 Антифрод / аналитика
    is_fraud = models.BooleanField(default=False)
    fraud_type = models.CharField(max_length=100, blank=True, null=True)
    time_since_last_transaction = models.FloatField(blank=True, null=True)
    spending_deviation_score = models.FloatField(blank=True, null=True)
    velocity_score = models.FloatField(blank=True, null=True)
    geo_anomaly_score = models.FloatField(blank=True, null=True)

    # 🔹 Технические метаданные
    payment_channel = models.CharField(max_length=50, blank=True, null=True)
    ip_address = models.GenericIPAddressField(blank=True, null=True)
    device_hash = models.CharField(max_length=128, blank=True, null=True)

    # 🔹 Статус обработки
    STATUS_CHOICES = [
        ('NEW', 'New'),
        ('QUEUED', 'Queued'),
        ('PROCESSING', 'Processing'),
        ('PROCESSED', 'Processed'),
        ('FAILED', 'Failed'),
    ]
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='NEW')

    # 🔹 Тайминги / метрики
    received_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)
    processing_time_ms = models.FloatField(blank=True, null=True)

    # 🔹 Служебные
    processed_by = models.CharField(max_length=100, blank=True, null=True)
    api_source = models.CharField(max_length=100, blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ['-timestamp']
        verbose_name = "Transaction"
        verbose_name_plural = "Transactions"

    def __str__(self):
        return f"{self.transaction_id} — {self.transaction_type} ({self.amount})"

    # 🔹 Удобный метод для статуса
    def mark_as_processed(self, processed_by="system"):
        self.status = "PROCESSED"
        self.processed_by = processed_by
        self.processed_at = timezone.now()
        self.processing_time_ms = (
            (self.processed_at - self.received_at).total_seconds() * 1000
            if self.received_at else None
        )
        self.save(update_fields=[
            "status", "processed_by", "processed_at", "processing_time_ms", "updated_at"
        ])
    
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