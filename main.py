import logging
import time
from datetime import datetime, timezone
from constants import settings
from app_configs.rabbitmq_config import RabbitMQConfig
from database import MongoDBManager
from utils.logger import get_trace_logger
from celery_app.tasks import (
    process_high_priority_task,
    process_default_task,
    process_low_priority_task,
    process_failing_task
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = get_trace_logger(__name__, trace_id="main-producer")

def verify_results():
    """
    Polls MongoDB to print the status of executed tasks and verify system correctness.
    """
    logger.info("\n--- Querying MongoDB for Execution Logs ---")
    logs_col = MongoDBManager.get_logs_collection()
    responses_col = MongoDBManager.get_responses_collection()

    # Wait a bit to ensure async task processing has finished
    logger.info("Polling database in 5 seconds...")
    time.sleep(5)

    # 1. Print Task Logs
    logger.info("\n=== MongoDB 'task_logs' Collection Records ===")
    logs = list(logs_col.find())
    for log_item in logs:
        logger.info("Task ID: %s | Task: %s | Trace ID: %s | Status: %s | Retries: %s | Created At: %s",
                    log_item.get("task_id"), log_item.get("task_name"), log_item.get("trace_id"),
                    log_item.get("status"), log_item.get("retry_count"), log_item.get("created_at"))

    # 2. Print Task Responses
    logger.info("\n=== MongoDB 'task_responses' Collection Records ===")
    responses = list(responses_col.find())
    for resp in responses:
        logger.info("Task ID: %s | Task: %s | Trace ID: %s | Status: %s | Created At: %s | Response: %s",
                    resp.get("task_id"), resp.get("task_name"), resp.get("trace_id"),
                    resp.get("status"), resp.get("created_at"), resp.get("response"))

def main():
    logger.info("Connecting to MongoDB...")
    MongoDBManager.initialize_indexes()

    # 1. Queue tasks with different priorities
    logger.info("\n--- Dispatching Priority Tasks ---")
    
    logger.info("Dispatching task to '%s' queue...", RabbitMQConfig.HIGH_PRIORITY_QUEUE)
    high_task = process_high_priority_task.apply_async(
        args=({"trace_id": "tr-high-101", "metric_id": 101, "action": "render_report"},), 
        queue=RabbitMQConfig.HIGH_PRIORITY_QUEUE
    )
    logger.info("High priority Task sent. ID: %s", high_task.id, trace_id="tr-high-101")

    logger.info("Dispatching task to '%s' queue...", RabbitMQConfig.DEFAULT_QUEUE)
    default_task_obj = process_default_task.apply_async(
        args=({"trace_id": "tr-default-202", "metric_id": 202, "action": "update_cache"},), 
        queue=RabbitMQConfig.DEFAULT_QUEUE
    )
    logger.info("Default task sent. ID: %s", default_task_obj.id, trace_id="tr-default-202")

    logger.info("Dispatching task to '%s' queue...", RabbitMQConfig.LOW_PRIORITY_QUEUE)
    low_task = process_low_priority_task.apply_async(
        args=({"trace_id": "tr-low-303", "a": 10, "b": 20},), 
        queue=RabbitMQConfig.LOW_PRIORITY_QUEUE
    )
    logger.info("Low priority Task sent. ID: %s", low_task.id, trace_id="tr-low-303")

    # 2. Queue duplicate tasks to test Distributed Locking
    logger.info("\n--- Dispatching Duplicate Tasks (Testing Distributed Lock) ---")
    payload = {"trace_id": "tr-dup-9999", "transaction_id": 9999, "amount": 250.0}
    
    logger.info("Dispatching Task A with payload: %s", payload, trace_id="tr-dup-9999")
    dup_a = process_default_task.apply_async(args=(payload,), queue=RabbitMQConfig.DEFAULT_QUEUE)
    logger.info("Task A sent. ID: %s", dup_a.id, trace_id="tr-dup-9999")

    logger.info("Dispatching identical Task B (Duplicate) with same payload: %s", payload, trace_id="tr-dup-9999")
    dup_b = process_default_task.apply_async(args=(payload,), queue=RabbitMQConfig.DEFAULT_QUEUE)
    logger.info("Task B sent. ID: %s", dup_b.id, trace_id="tr-dup-9999")

    # 3. Queue failing task to test Retries & Exponential Backoff
    logger.info("\n--- Dispatching Failing Task (Testing Retries & Exponential Backoff) ---")
    failing_task_obj = process_failing_task.apply_async(args=(3,), kwargs={"trace_id": "tr-fail-777"}, queue=RabbitMQConfig.DEFAULT_QUEUE)
    logger.info("Failing task sent. ID: %s", failing_task_obj.id, trace_id="tr-fail-777")

    logger.info("\nAll tasks dispatched successfully! Waiting for workers to execute...")
    
    # Poll database results periodically to view live updates
    for i in range(3):
        time.sleep(4)
        verify_results()

    # Close MongoDB connections
    MongoDBManager.close_client()
    logger.info("\nDemonstration finished. You can view the MongoDB database collections for full states.")

if __name__ == "__main__":
    main()
