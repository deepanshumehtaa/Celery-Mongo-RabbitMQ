import argparse
import asyncio
import time
import uuid
from datetime import datetime, timezone

from app_configs.rabbitmq_config import RabbitMQConfig
from constants import settings
from database import MongoDBManager
from utils.logger import get_trace_logger
from celery_app.tasks import (
    generate_analytics_report,
    process_user_order,
    archive_audit_logs,
    process_payment_settlement
)

logger = get_trace_logger(__name__, trace_id="load-test-runner")

async def enqueue_single_task(task_type: str, task_index: int, semaphore: asyncio.Semaphore):
    """
    Enqueues a single task to RabbitMQ asynchronously using asyncio.to_thread 
    to prevent blocking the event loop.
    """
    async with semaphore:
        trace_id = f"tr-load-{task_index:04d}-{uuid.uuid4().hex[:6]}"
        
        if task_type == "high":
            payload = {
                "trace_id": trace_id,
                "report_type": f"Load Test High Priority #{task_index}",
                "metrics": [100.0 + task_index, 200.0, 300.0]
            }
            result = await asyncio.to_thread(
                generate_analytics_report.apply_async,
                args=(payload,),
                queue=RabbitMQConfig.HIGH_PRIORITY_QUEUE
            )
            return ("high", result.id)

        elif task_type == "default":
            payload = {
                "trace_id": trace_id,
                "order_id": f"ORD-LOAD-{task_index:04d}",
                "amount": 50.0 + (task_index % 100),
                "discount": 5.0
            }
            result = await asyncio.to_thread(
                process_user_order.apply_async,
                args=(payload,),
                queue=RabbitMQConfig.DEFAULT_QUEUE
            )
            return ("default", result.id)

        elif task_type == "low":
            payload = {
                "trace_id": trace_id,
                "log_entries": 100 + (task_index * 10),
                "archive_type": "load_test_logs"
            }
            result = await asyncio.to_thread(
                archive_audit_logs.apply_async,
                args=(payload,),
                queue=RabbitMQConfig.LOW_PRIORITY_QUEUE
            )
            return ("low", result.id)

        else:  # "fail"
            result = await asyncio.to_thread(
                process_payment_settlement.apply_async,
                args=(2,),  # Fails 2 times then recovers
                kwargs={"trace_id": trace_id},
                queue=RabbitMQConfig.DEFAULT_QUEUE
            )
            return ("fail", result.id)

async def run_load_test(total_requests: int, concurrency: int, poll_db: bool):
    """
    Asynchronously enqueues a large batch of task requests to RabbitMQ queues.
    """
    logger.info("Starting AsyncIO Load Test...")
    logger.info("Total Requests: %d | Max Concurrency: %d", total_requests, concurrency)
    
    # Initialize MongoDB connection/indexes
    await asyncio.to_thread(MongoDBManager.initialize_indexes)
    
    semaphore = asyncio.Semaphore(concurrency)
    task_types = ["high", "default", "low", "fail"]
    
    start_time = time.time()
    
    async_tasks = []
    for idx in range(1, total_requests + 1):
        # Round-robin task distribution across queue types
        task_type = task_types[(idx - 1) % len(task_types)]
        async_tasks.append(enqueue_single_task(task_type, idx, semaphore))

    # Run all enqueue operations concurrently
    results = await asyncio.gather(*async_tasks)
    
    elapsed = time.time() - start_time
    rps = total_requests / elapsed if elapsed > 0 else total_requests

    counts = {"high": 0, "default": 0, "low": 0, "fail": 0}
    for t_type, _ in results:
        counts[t_type] += 1

    print("\n" + "=" * 65)
    print("                ASYNCIO LOAD TEST RESULTS SUMMARY                ")
    print("=" * 65)
    print(f"  🚀 Total Requests Enqueued : {total_requests}")
    print(f"  ⏱️  Total Time Elapsed     : {elapsed:.3f} seconds")
    print(f"  ⚡ Enqueue Throughput (RPS) : {rps:.2f} requests/sec")
    print("-" * 65)
    print("  Queue Distribution:")
    print(f"     • High Priority Queue   ('{RabbitMQConfig.HIGH_PRIORITY_QUEUE}') : {counts['high']}")
    print(f"     • Default Queue         ('{RabbitMQConfig.DEFAULT_QUEUE}')       : {counts['default']}")
    print(f"     • Low Priority Queue    ('{RabbitMQConfig.LOW_PRIORITY_QUEUE}')   : {counts['low']}")
    print(f"     • Failing/Retry Tasks  ('{RabbitMQConfig.DEFAULT_QUEUE}')       : {counts['fail']}")
    print("=" * 65 + "\n")

    if poll_db:
        print("Polling MongoDB in 5 seconds to observe worker execution count...")
        await asyncio.sleep(5)
        
        logs_col = MongoDBManager.get_logs_collection()
        responses_col = MongoDBManager.get_responses_collection()
        
        total_logs = await asyncio.to_thread(logs_col.count_documents, {})
        total_resp = await asyncio.to_thread(responses_col.count_documents, {})
        
        print(f"📊 Live MongoDB Stats -> Total Task Logs: {total_logs} | Total Task Responses: {total_resp}")

def main():
    parser = argparse.ArgumentParser(description="AsyncIO Load Test Generator for Celery + RabbitMQ + MongoDB.")
    parser.add_argument(
        "-n", "--total-requests",
        type=int,
        default=100,
        help="Total number of tasks to enqueue (default: 100)."
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=20,
        help="Max concurrent asyncio enqueue workers (default: 20)."
    )
    parser.add_argument(
        "--poll-db",
        action="store_true",
        help="Poll MongoDB after load test to report processed count."
    )
    
    args = parser.parse_args()
    asyncio.run(run_load_test(args.total_requests, args.concurrency, args.poll_db))

if __name__ == "__main__":
    main()
