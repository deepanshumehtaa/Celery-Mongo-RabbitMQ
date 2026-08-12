import logging
import time
from datetime import datetime, timezone
from constants import settings
from app_configs.rabbitmq_config import RabbitMQConfig
from database import MongoDBManager
from celery_app.tasks import (
    process_high_priority_task,
    process_default_task,
    addition_task,
    process_failing_task
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    logger.info("\n1. Tasks Execution Logs (Collection: %s):", logs_col.name)
    logs = list(logs_col.find())
    for log in logs:
        # Format dates for clean printing
        created = log.get("created_at")
        created_str = created.strftime("%H:%M:%S") if created else "N/A"
        
        logger.info(
            "Task ID: %s | Name: %s | Status: %s | Retries: %s | Created: %s | Error: %s",
            log.get("task_id"),
            log.get("task_name"),
            log.get("status"),
            log.get("retry_count"),
            created_str,
            log.get("error_message", "None")
        )

    # 2. Print Task Responses
    logger.info("\n2. Task Responses (Collection: %s):", responses_col.name)
    responses = list(responses_col.find())
    for resp in responses:
        created = resp.get("created_at")
        created_str = created.strftime("%H:%M:%S") if created else "N/A"
        logger.info(
            "Task ID: %s | Name: %s | Created: %s | Response: %s",
            resp.get("task_id"),
            resp.get("task_name"),
            created_str,
            resp.get("response")
        )

def main():
    logger.info("Starting Celery-Mongo-RabbitMQ Demonstration & Verification Client...")
    
    # Force initialize MongoDB indexes before sending tasks (so the collections exist and indexes are active)
    logger.info("Initializing MongoDB collections and indexes...")
    MongoDBManager.initialize_indexes()

    # Clear previous runs' logs and responses to start fresh
    logger.info("Cleaning database collections for a fresh run...")
    MongoDBManager.get_logs_collection().delete_many({})
    MongoDBManager.get_responses_collection().delete_many({})
    MongoDBManager.get_locks_collection().delete_many({})

    # 1. Queue tasks with different priorities
    logger.info("\n--- Dispatching Priority Tasks ---")
    
    logger.info("Dispatching task to '%s' queue...", RabbitMQConfig.HIGH_PRIORITY_QUEUE)
    high_task = process_high_priority_task.apply_async(
        args=({"metric_id": 101, "action": "render_report"},), 
        queue=RabbitMQConfig.HIGH_PRIORITY_QUEUE
    )
    logger.info("High priority Task sent. ID: %s", high_task.id)

    logger.info("Dispatching task to '%s' queue...", RabbitMQConfig.DEFAULT_QUEUE)
    default_task_obj = process_default_task.apply_async(
        args=({"metric_id": 202, "action": "update_cache"},), 
        queue=RabbitMQConfig.DEFAULT_QUEUE
    )
    logger.info("Default task sent. ID: %s", default_task_obj.id)

    logger.info("Dispatching task to '%s' queue...", RabbitMQConfig.LOW_PRIORITY_QUEUE)
    low_task = addition_task.apply_async(
        args=({"metric_id": 303, "action": "archive_logs"},), 
        queue=RabbitMQConfig.LOW_PRIORITY_QUEUE
    )
    logger.info("Low priority Task sent. ID: %s", low_task.id)

    # 2. Queue duplicate tasks to test Distributed Locking
    logger.info("\n--- Dispatching Duplicate Tasks (Testing Distributed Lock) ---")
    payload = {"transaction_id": 9999, "amount": 250.0}
    
    logger.info("Dispatching Task A with payload: %s", payload)
    dup_a = process_default_task.apply_async(args=(payload,), queue=RabbitMQConfig.DEFAULT_QUEUE)
    logger.info("Task A sent. ID: %s", dup_a.id)

    logger.info("Dispatching identical Task B (Duplicate) with same payload: %s", payload)
    dup_b = process_default_task.apply_async(args=(payload,), queue=RabbitMQConfig.DEFAULT_QUEUE)
    logger.info("Task B sent. ID: %s", dup_b.id)

    # 3. Queue failing task to test Retries & Exponential Backoff
    logger.info("\n--- Dispatching Failing Task (Testing Retries & Exponential Backoff) ---")
    # Will fail 3 times (attempts 1, 2, 3) and succeed on the 4th (retry_count == 3)
    failing_task_obj = process_failing_task.apply_async(args=(3,), queue=RabbitMQConfig.DEFAULT_QUEUE)
    logger.info("Failing task sent. ID: %s", failing_task_obj.id)

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
