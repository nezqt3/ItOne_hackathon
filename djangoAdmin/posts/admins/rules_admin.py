from django.contrib import admin
from django import forms
from django.utils.safestring import mark_safe
from posts.models.models import Rules

RULE_TYPES = [
    ('threshold', 'Threshold'),
    ('pattern', 'Pattern'),
    ('composite', 'Composite'),
]

class RulesForm(forms.ModelForm):
    class Meta:
        model = Rules
        fields = ['name', 'description', 'is_active', 'rule_type', 'config']

    # Простой виджет для JSON с подсветкой
    config = forms.JSONField(
        required=False,
        widget=forms.Textarea(attrs={'style': 'width: 90%; height: 150px; font-family: monospace;'}),
        help_text=mark_safe(
            "Настройка правила в формате JSON. <br>"
            "<b>Threshold:</b> {\"threshold\": 1000} <br>"
            "<b>Pattern:</b> {\"window_minutes\": 30, \"max_count\": 3, \"max_amount\": 1000} <br>"
            "<b>Composite:</b> {\"conditions\": [{\"type\": \"threshold\", \"value\": 50000}, {\"type\": \"time_range\", \"start\": 0, \"end\": 6}]}"
        )
    )

class RulesAdmin(admin.ModelAdmin):
    list_display = ('name', 'rule_type', 'is_active', 'created_by', 'updated_by', 'created_at', 'updated_at')
    list_filter = ('is_active', 'rule_type')
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
        """Автоматически сохраняем автора и последнего редактора"""
        if not obj.created_by:
            obj.created_by = request.user
        obj.updated_by = request.user
        super().save_model(request, obj, form, change)

