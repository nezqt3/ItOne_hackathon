from prometheus_client import Counter, Histogram, Gauge

# -----------------------------
# Транзакции
# -----------------------------
transactions_imported = Counter(
    'transactions_imported_total',
    'Общее количество импортированных транзакций'
)
transactions_failed = Counter(
    'transactions_failed_total',
    'Количество транзакций, которые не удалось импортировать'
)

# -----------------------------
# Уведомления
# -----------------------------
notifications_success = Counter(
    'notifications_success_total',
    'Количество успешно доставленных уведомлений'
)
notifications_failed = Counter(
    'notifications_failed_total',
    'Количество уведомлений с ошибкой'
)

# -----------------------------
# Алерты
# -----------------------------
alerts_total = Counter(
    'alerts_total',
    'Общее количество сработавших алертов',
    ['severity']  # можно фильтровать по уровню критичности: info, warning, critical
)
alerts_failed = Counter(
    'alerts_failed_total',
    'Количество алертов, которые не удалось доставить'
)

# -----------------------------
# Время доставки алертов
# -----------------------------
alert_delivery_time = Histogram(
    'alert_delivery_seconds',
    'Время доставки алерта в секундах',
    buckets=[0.1, 0.3, 1, 3, 5, 10, 30, 60]  # настраиваем кастомные интервалы
)

# -----------------------------
# Gauge для доли успешных уведомлений
# -----------------------------
alerts_success_ratio = Gauge(
    'alerts_success_ratio',
    'Процент успешно доставленных уведомлений'
)

def update_success_ratio():
    """Пересчитывает долю успешных уведомлений"""
    try:
        total = notifications_success._value.get() + notifications_failed._value.get()
        success = notifications_success._value.get()
        ratio = (success / total * 100) if total > 0 else 0
        alerts_success_ratio.set(ratio)
    except Exception:
        # если что-то пошло не так — игнорируем, чтобы не падал сервис
        pass
