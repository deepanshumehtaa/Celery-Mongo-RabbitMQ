import hashlib
import time
import uuid
from datetime import datetime, timezone

def always_failing_logic(retry: int, t_logger):
    """
    Business logic for always_failing_task.
    Intentionally raises RuntimeError.
    """
    t_logger.error("Executing always-failing task: attempt %d/4", retry + 1)
    time.sleep(4)
    raise RuntimeError(f"Permanent task failure simulated on attempt {retry + 1}")

def generate_analytics_report_biz(data: dict, t_logger) -> dict:
    """
    Business logic for generate_analytics_report.
    """
    t_logger.info("Executing analytics report generation for data: %s", data)
    time.sleep(4)
    metrics = data.get("metrics", [100.0, 250.5, 400.0, 75.25])
    total_value = sum(metrics) if isinstance(metrics, list) else 0.0
    1/0
    avg_value = total_value / len(metrics) if metrics and isinstance(metrics, list) else 0.0
    
    return {
        "report_type": data.get("report_type", "Executive Summary"),
        "total_processed_records": len(metrics) if isinstance(metrics, list) else 1,
        "total_value": round(total_value, 2),
        "average_value": round(avg_value, 2),
        "processing_time_seconds": 4.0,
        "status": "COMPLETED",
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

def process_user_order_biz(data: dict, t_logger) -> dict:
    """
    Business logic for process_user_order.
    """
    t_logger.info("Processing user order transaction: %s", data)
    time.sleep(4)
    amount = data.get("amount", 150.0)
    tax_rate = data.get("tax_rate", 0.08)
    discount = data.get("discount", 10.0)
    
    subtotal = max(0.0, amount - discount)
    tax_amount = subtotal * tax_rate
    final_total = round(subtotal + tax_amount, 2)
    
    return {
        "order_id": data.get("order_id", f"ORD-{uuid.uuid4().hex[:6].upper()}"),
        "customer_id": data.get("customer_id", "CUST-1001"),
        "subtotal": subtotal,
        "tax_amount": round(tax_amount, 2),
        "final_total": final_total,
        "processing_time_seconds": 4.0,
        "status": "ORDER_PROCESSED",
        "processed_at": datetime.now(timezone.utc).isoformat()
    }

def archive_audit_logs_biz(data: dict, t_logger) -> dict:
    """
    Business logic for archive_audit_logs.
    """
    t_logger.info("Starting low-priority audit log archival with payload: %s", data)
    time.sleep(4)
    log_entries = data.get("log_entries", 500)
    compressed_size_kb = round(log_entries * 0.42, 2)
    archive_hash = hashlib.sha256(f"archive_{log_entries}_{datetime.now(timezone.utc)}".encode("utf-8")).hexdigest()[:12]
    
    return {
        "archive_id": f"ARC-{archive_hash.upper()}",
        "entries_archived": log_entries,
        "estimated_size_kb": compressed_size_kb,
        "compression_ratio": "68%",
        "processing_time_seconds": 4.0,
        "status": "ARCHIVED",
        "archived_at": datetime.now(timezone.utc).isoformat()
    }

def process_payment_settlement_biz(fail_until_retry: int, current_retry: int, t_logger) -> dict:
    """
    Business logic for process_payment_settlement.
    """
    t_logger.info("Attempting payment gateway settlement (Attempt %d/%d)", current_retry + 1, fail_until_retry + 1)
    time.sleep(4)
    if current_retry < fail_until_retry:
        raise ValueError(f"Third-party payment gateway timeout on attempt {current_retry + 1}")
        
    return {
        "settlement_id": f"STL-{uuid.uuid4().hex[:8].upper()}",
        "status": "SETTLED",
        "attempt_count": current_retry + 1,
        "processing_time_seconds": 4.0,
        "settled_at": datetime.now(timezone.utc).isoformat()
    }
