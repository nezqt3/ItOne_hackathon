from django.core.management.base import BaseCommand
from posts.utils.queue_manager import QueueManager
import time

class Command(BaseCommand):
    help = "Processes queued transactions continuously."

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Queue processor started..."))
        while True:
            tx = QueueManager.process_next()
            if tx:
                self.stdout.write(f"Processed transaction: {tx.transaction_id} ({tx.status})")
            else:
                time.sleep(2)
