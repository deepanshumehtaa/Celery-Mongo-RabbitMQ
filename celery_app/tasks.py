import hashlib
import logging
import time
import uuid
from datetime import datetime, timezone

from celery_app.celery import app
from celery_app.base_task import (
    MongoLoggedTask,
    extract_trace_id,
    generate_lock_key,
    log_task_state,
    write_task_response
)
from utils.logger import get_trace_logger

logger = get_trace_logger(__name__)

# ----------------- Domain Task Definitions -----------------

@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.generate_analytics_report")
def generate_analytics_report(self, data: dict = None):
    """
    High Priority Task: Generates a real-time executive analytics report.
    Calculates revenue totals, conversion rates, and risk scores.
    """
    if data is None or not isinstance(data, dict):
        data = {
            "metrics": [100.0, 250.5, 400.0, 75.25],
            "report_type": "Executive Summary"
        }
        
    trace_id = extract_trace_id((data,), {})
    t_logger = get_trace_logger(__name__, trace_id=trace_id)
    t_logger.info("Executing analytics report generation for data: %s", data)
    
    time.sleep(1.5)  # Simulate processing heavy analytics computation
    metrics = data.get("metrics", [100.0, 250.5, 400.0, 75.25])
    total_value = sum(metrics) if isinstance(metrics, list) else 0.0
    avg_value = total_value / len(metrics) if metrics and isinstance(metrics, list) else 0.0
    
    report_summary = {
        "report_type": data.get("report_type", "Executive Summary"),
        "total_processed_records": len(metrics) if isinstance(metrics, list) else 1,
        "total_value": round(total_value, 2),
        "average_value": round(avg_value, 2),
        "status": "COMPLETED",
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
    t_logger.info("Analytics report generated successfully: %s", report_summary)
    return report_summary


@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.process_user_order")
def process_user_order(self, data: dict = None):
    """
    Default Priority Task: Processes user e-commerce order transactions.
    Calculates subtotal, tax rate, discount calculations, and updates order status.
    """
    if data is None or not isinstance(data, dict):
        data = {
            "amount": 150.0,
            "tax_rate": 0.08,
            "discount": 10.0,
            "customer_id": "CUST-1001"
        }

    trace_id = extract_trace_id((data,), {})
    t_logger = get_trace_logger(__name__, trace_id=trace_id)
    t_logger.info("Processing user order transaction: %s", data)
    
    amount = data.get("amount", 150.0)
    tax_rate = data.get("tax_rate", 0.08)
    discount = data.get("discount", 10.0)
    
    subtotal = max(0.0, amount - discount)
    tax_amount = subtotal * tax_rate
    final_total = round(subtotal + tax_amount, 2)
    
    order_result = {
        "order_id": data.get("order_id", f"ORD-{uuid.uuid4().hex[:6].upper()}"),
        "customer_id": data.get("customer_id", "CUST-1001"),
        "subtotal": subtotal,
        "tax_amount": round(tax_amount, 2),
        "final_total": final_total,
        "status": "ORDER_PROCESSED",
        "processed_at": datetime.now(timezone.utc).isoformat()
    }
    t_logger.info("User order processed successfully: %s", order_result)
    return order_result


@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.archive_audit_logs")
def archive_audit_logs(self, data: dict = None):
    """
    Low Priority Task: Archives system audit logs and compresses historical data.
    """
    if data is None or not isinstance(data, dict):
        data = {
            "log_entries": 500,
            "archive_type": "audit_logs"
        }

    trace_id = extract_trace_id((data,), {})
    t_logger = get_trace_logger(__name__, trace_id=trace_id)
    t_logger.info("Starting low-priority audit log archival with payload: %s", data)
    
    log_entries = data.get("log_entries", 500)
    compressed_size_kb = round(log_entries * 0.42, 2)
    archive_hash = hashlib.sha256(f"archive_{log_entries}_{datetime.now(timezone.utc)}".encode("utf-8")).hexdigest()[:12]
    
    archival_result = {
        "archive_id": f"ARC-{archive_hash.upper()}",
        "entries_archived": log_entries,
        "estimated_size_kb": compressed_size_kb,
        "compression_ratio": "68%",
        "status": "ARCHIVED",
        "archived_at": datetime.now(timezone.utc).isoformat()
    }
    t_logger.info("Audit log archival completed: %s", archival_result)
    return archival_result


@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.process_payment_settlement")
def process_payment_settlement(self, fail_until_retry: int = 3, **kwargs):
    """
    Failing Task: Simulates payment gateway settlement with transient API timeouts.
    Retries up to max_retries using exponential backoff before achieving final settlement.
    """
    if fail_until_retry is None:
        fail_until_retry = 3
    if kwargs is None or not isinstance(kwargs, dict):
        kwargs = {}

    trace_id = extract_trace_id((fail_until_retry,), kwargs)
    t_logger = get_trace_logger(__name__, trace_id=trace_id)
    current_retry = self.request.retries
    
    t_logger.info("Attempting payment gateway settlement (Attempt %d/%d)", current_retry + 1, fail_until_retry + 1)
    
    if current_retry < fail_until_retry:
        raise ValueError(f"Third-party payment gateway timeout on attempt {current_retry + 1}")
        
    settlement_result = {
        "settlement_id": f"STL-{uuid.uuid4().hex[:8].upper()}",
        "status": "SETTLED",
        "attempt_count": current_retry + 1,
        "settled_at": datetime.now(timezone.utc).isoformat()
    }
    t_logger.info("Payment settlement succeeded after %d retries: %s", current_retry, settlement_result)
    return settlement_result


# Backwards compatibility aliases
process_high_priority_task = generate_analytics_report
process_default_task = process_user_order
process_low_priority_task = archive_audit_logs
process_failing_task = process_payment_settlement
