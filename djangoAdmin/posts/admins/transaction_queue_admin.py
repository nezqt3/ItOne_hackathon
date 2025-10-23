from django.contrib import admin
from posts.models.transaction_queue import TransactionQueue, TransactionQueueLog

@admin.register(TransactionQueue)
class TransactionQueueAdmin(admin.ModelAdmin):
    list_display = ('transaction_id', 'status', 'created_at', 'started_at', 'completed_at')
    list_filter = ('status',)
    search_fields = ('transaction_id', 'correlation_id')
    readonly_fields = ('created_at', 'started_at', 'completed_at')
    ordering = ('-created_at',)

    def view_on_site(self, obj):
        return None


@admin.register(TransactionQueueLog)
class TransactionQueueLogAdmin(admin.ModelAdmin):
    list_display = ('transaction', 'event', 'component', 'created_at')
    list_filter = ('component',)
    search_fields = ('transaction__transaction_id', 'event', 'message')
    ordering = ('-created_at',)
