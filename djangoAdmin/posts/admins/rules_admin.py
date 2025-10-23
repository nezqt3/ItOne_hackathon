from django.contrib import admin
from django.utils import timezone
from posts.models.models import Rules
from posts.utils.logging_utils import log_transaction_event
from django.contrib.auth import get_user_model

User = get_user_model()

class RulesAdmin(admin.ModelAdmin):
    list_display = ('name', 'rule_type', 'operator', 'is_active', 'created_by', 'updated_by', 'created_at', 'updated_at')
    list_filter = ('is_active', 'rule_type', 'operator')
    search_fields = ('name', 'description')
    actions = ['enable_rules', 'disable_rules']

    fieldsets = (
        (None, {
            'fields': ('name', 'description', 'is_active', 'rule_type')
        }),
        ('Threshold settings', {
            'fields': ('threshold_value', "operator"),
        }),
        ('Pattern settings', {
            'fields': ('pattern_window_minutes', 'pattern_max_count', 'pattern_max_amount'),
        }),
        ('Composite settings', {
            'fields': ('composite_conditions',),
        }),
        ('Audit info', {
            'fields': ('created_by', 'updated_by', 'created_at', 'updated_at'),
        }),
    )

    readonly_fields = ('created_by', 'updated_by', 'created_at', 'updated_at')

    def save_model(self, request, obj, form, change):
        """
        Сохраняем автора/редактора, управляем активностью и логируем действия.
        """
        user = request.user
        is_create = not change

        # Автор/редактор
        if not obj.created_by:
            obj.created_by = user
        obj.updated_by = user

        # Если включаем новое правило — все старые такого типа становятся неактивными
        if obj.is_active:
            Rules.objects.filter(rule_type=obj.rule_type).exclude(id=obj.id).update(is_active=False)

        # Сохраняем объект
        super().save_model(request, obj, form, change)

        # Корреляционный ID для трассировки
        correlation_id = f"RULE-{obj.id}-{timezone.now().strftime('%Y%m%d%H%M%S')}"

        # Используем transaction_id=rule.id, чтобы точно сохранялось
        log_transaction_event(
            transaction_id=f"RULE-{obj.id}",  # всегда уникальный
            correlation_id=correlation_id,
            level="INFO",
            component="rules_admin",
            message=f"{'Создано' if is_create else 'Изменено'} правило пользователем {user.username}",
            data={
                "rule_id": obj.id,
                "rule_name": obj.name,
                "user": user.username,
                "rule_type": obj.rule_type,
                "active": obj.is_active,
                "operator": obj.operator,
                "threshold": obj.threshold_value,
                "pattern_window_minutes": obj.pattern_window_minutes,
                "pattern_max_count": obj.pattern_max_count,
                "pattern_max_amount": obj.pattern_max_amount,
                "composite_conditions": obj.composite_conditions,
            }
        )

    def enable_rules(self, request, queryset):
        updated = 0
        for rule in queryset:
            rule.is_active = True
            rule.updated_by = request.user
            rule.save()
            # Деактивируем старые правила того же типа
            Rules.objects.filter(rule_type=rule.rule_type).exclude(id=rule.id).update(is_active=False)
            updated += 1

            log_transaction_event(
                transaction_id=f"RULE-{rule.id}",  # обязательно уникальный
                correlation_id=f"RULE-ENABLE-{rule.id}-{timezone.now().strftime('%Y%m%d%H%M%S')}",
                level="INFO",
                component="rules_admin",
                message=f"Активировано правило {rule.name} ({rule.rule_type}) пользователем {request.user.username}",
                data={"rule_id": rule.id, "user": request.user.username}
            )

        self.message_user(request, f"{updated} правил включено")
    enable_rules.short_description = "Enable selected rules"

    def disable_rules(self, request, queryset):
        updated = 0
        for rule in queryset:
            rule.is_active = False
            rule.updated_by = request.user
            rule.save()
            updated += 1

            log_transaction_event(
                transaction_id=f"RULE-{rule.id}",  # обязательно уникальный
                correlation_id=f"RULE-ENABLE-{rule.id}-{timezone.now().strftime('%Y%m%d%H%M%S')}",
                level="INFO",
                component="rules_admin",
                message=f"Активировано правило {rule.name} ({rule.rule_type}) пользователем {request.user.username}",
                data={"rule_id": rule.id, "user": request.user.username}
            )

        self.message_user(request, f"{updated} правил отключено")
    disable_rules.short_description = "Disable selected rules"
