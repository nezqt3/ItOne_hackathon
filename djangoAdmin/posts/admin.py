from django.contrib import admin
from posts.admins.transaction_admin import TransactionsAdmin    
from posts.admins.transaction_types_admin import TransactionsTypesAdmin
from posts.admins.logs import TransactionLogAdmin
from posts.admins.rules_admin import RulesAdmin

from posts.models.models import Transactions, TransactionsTypes, TransactionLog, Rules

admin.site.register(Transactions, TransactionsAdmin)
# admin.site.register(TransactionsTypes, TransactionsTypesAdmin)
admin.site.register(TransactionLog, TransactionLogAdmin)
admin.site.register(Rules, RulesAdmin)