from django.db import models
from django.utils import timezone
import uuid

class ExportTask(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    task_type = models.CharField(max_length=50, default="export_csv")
    status = models.CharField(max_length=20, default="queued")  # queued, processing, completed, failed
    total_items = models.IntegerField(default=0)
    processed_items = models.IntegerField(default=0)
    progress = models.FloatField(default=0.0)
    file_path = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def update_progress(self, processed):
        self.processed_items = processed
        if self.total_items:
            self.progress = round((processed / self.total_items) * 100, 2)
        self.save(update_fields=['processed_items', 'progress', 'updated_at'])

class ImportExportTask(models.Model):
    TASK_TYPES = [
        ('import_json', 'Import JSON'),
        ('export_csv', 'Export CSV'),
    ]
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    task_type = models.CharField(max_length=50, choices=TASK_TYPES)
    status = models.CharField(max_length=20, default="queued")  # queued, processing, completed, failed
    total_items = models.IntegerField(default=0)
    processed_items = models.IntegerField(default=0)
    progress = models.FloatField(default=0.0)
    file_path = models.CharField(max_length=255, blank=True, null=True)
    message = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def update_progress(self, processed):
        self.processed_items = processed
        if self.total_items:
            self.progress = round((processed / self.total_items) * 100, 2)
        self.save(update_fields=['processed_items', 'progress', 'updated_at'])