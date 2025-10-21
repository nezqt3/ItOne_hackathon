from django.apps import AppConfig

class TransactionsConfig(AppConfig):
    default_auto_field = "django.db.models.Transactions"
    name = "transactions"

class TransactionsTypesConfig(AppConfig):
    default_auto_field = "django.db.models.TransactionsTypes"
    name = "transactions_types"