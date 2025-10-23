from prometheus_client import Counter, Histogram

transactions_imported = Counter('transactions_imported_total', 'Total number of imported transactions')
transactions_failed = Counter('transactions_failed_total', 'Number of failed imports')
notifications_success = Counter('notifications_success_total', 'Successful notifications')
notifications_failed = Counter('notifications_failed_total', 'Failed notifications')
alerts_total = Counter('alerts_total', 'Total alerts generated', ['severity'])
alerts_failed = Counter('alerts_failed_total', 'Failed alerts')
alert_delivery_time = Histogram('alert_delivery_seconds', 'Alert delivery time in seconds')