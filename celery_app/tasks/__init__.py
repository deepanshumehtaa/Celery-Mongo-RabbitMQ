from celery_app.base_task import generate_lock_key
from celery_app.tasks.analytics import generate_analytics_report
from celery_app.tasks.order import process_user_order
from celery_app.tasks.archive import archive_audit_logs
from celery_app.tasks.payment import process_payment_settlement
from celery_app.tasks.failing import always_failing_task

# Backwards compatibility aliases
process_high_priority_task = generate_analytics_report
process_default_task = process_user_order
process_low_priority_task = archive_audit_logs
process_failing_task = process_payment_settlement

__all__ = [
    "generate_lock_key",
    "generate_analytics_report",
    "process_user_order",
    "archive_audit_logs",
    "process_payment_settlement",
    "always_failing_task",
    "process_high_priority_task",
    "process_default_task",
    "process_low_priority_task",
    "process_failing_task",
]
