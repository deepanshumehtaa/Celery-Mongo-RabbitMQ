import argparse
import json
import logging
from constants import settings
from app_configs.rabbitmq_config import RabbitMQConfig
from celery_app.tasks import (
    process_high_priority_task,
    process_default_task,
    process_low_priority_task,
    process_failing_task,
    always_failing_task
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    CLI utility to trigger background tasks with specific routing and payloads.
    """
    parser = argparse.ArgumentParser(description="Trigger background Celery tasks with specific priorities.")
    parser.add_argument(
        "--task",
        choices=["high", "default", "low", "fail", "always_fail"],
        required=True,
        help="The type of task to trigger. Options: high, default, low, fail, always_fail."
    )
    parser.add_argument(
        "--data",
        type=str,
        default='{"message": "Hello background worker!"}',
        help='JSON string payload for the task (used for high, default, and low tasks). E.g. \'{"id": 123, "name": "data"}\''
    )
    parser.add_argument(
        "--fail-until",
        type=int,
        default=3,
        help="For the 'fail' task: number of times to fail before succeeding on retry (default: 3)."
    )
    
    args = parser.parse_args()

    # Parse --data argument as JSON
    try:
        data_payload = json.loads(args.data)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse --data as JSON. Check your syntax. Error: %s", e)
        return

    logger.info("Dispatching task request to broker...")

    if args.task == "high":
        # Routes specifically to the high priority queue
        task = process_high_priority_task.apply_async(args=(data_payload,), queue=RabbitMQConfig.HIGH_PRIORITY_QUEUE)
        logger.info("🚀 Dispatched High Priority Task successfully! Task ID: %s", task.id)
        logger.info("Check MongoDB logs collection for task_id: %s", task.id)
        
    elif args.task == "default":
        # Routes to default queue
        task = process_default_task.apply_async(args=(data_payload,), queue=RabbitMQConfig.DEFAULT_QUEUE)
        logger.info("✉️ Dispatched Default Task successfully! Task ID: %s", task.id)
        logger.info("Check MongoDB logs collection for task_id: %s", task.id)
        
    elif args.task == "low":
        # Routes to low priority queue
        task = process_low_priority_task.apply_async(args=(data_payload,), queue=RabbitMQConfig.LOW_PRIORITY_QUEUE)
        logger.info("🐢 Dispatched Low Priority Task successfully! Task ID: %s", task.id)
        logger.info("Check MongoDB logs collection for task_id: %s", task.id)
        
    elif args.task == "fail":
        # Dispatches the failing task
        task = process_failing_task.apply_async(args=(args.fail_until,), queue=RabbitMQConfig.DEFAULT_QUEUE)
        logger.info("⚠️ Dispatched Failing (Retry) Task successfully! Task ID: %s", task.id)
        logger.info("Task will fail %d times and then succeed on retry attempt %d.", args.fail_until, args.fail_until + 1)
        logger.info("Check MongoDB logs collection for task_id: %s", task.id)

    elif args.task == "always_fail":
        # Dispatches the always failing task
        task = always_failing_task.apply_async(args=(data_payload,), queue=RabbitMQConfig.DEFAULT_QUEUE)
        logger.info("💥 Dispatched Always Failing Task successfully! Task ID: %s", task.id)
        logger.info("Task will exhaust max retries and fail permanently in MongoDB.")

if __name__ == "__main__":
    main()
