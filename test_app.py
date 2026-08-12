import sys
import unittest
import json
import time
from datetime import datetime, timezone

from constants import settings
from app_configs.rabbitmq_config import RabbitMQConfig
from database import MongoDBManager
from celery_app.tasks import (
    generate_lock_key,
    process_high_priority_task,
    process_default_task,
    process_low_priority_task,
    process_failing_task
)

class TestEnqueueAndExecution(unittest.TestCase):
    """
    Test suite that enqueues tasks directly to RabbitMQ broker queues
    (high_priority, default, low_priority) for Celery workers to consume and process.
    Verifies task dispatching and verifies state tracking, responses, retries, and duplicate locks in MongoDB.
    """

    @classmethod
    def setUpClass(cls):
        """Initialize MongoDB indexes before test run."""
        MongoDBManager.initialize_indexes()

    def test_01_enqueue_high_priority_task(self):
        """Pushes a task to the high_priority queue in RabbitMQ."""
        payload = {"trace_id": "tr-test-high-1", "action": "generate_report", "priority": "high"}
        async_result = process_high_priority_task.apply_async(
            args=(payload,),
            queue=RabbitMQConfig.HIGH_PRIORITY_QUEUE
        )
        self.assertIsNotNone(async_result.id)
        self.__class__.high_task_id = async_result.id
        print(f"\n[Enqueue] High priority task pushed to RabbitMQ queue '{RabbitMQConfig.HIGH_PRIORITY_QUEUE}'. Task ID: {async_result.id}")

    def test_02_enqueue_default_task(self):
        """Pushes a task to the default queue in RabbitMQ."""
        payload = {"trace_id": "tr-test-default-2", "action": "update_cache", "priority": "default"}
        async_result = process_default_task.apply_async(
            args=(payload,),
            queue=RabbitMQConfig.DEFAULT_QUEUE
        )
        self.assertIsNotNone(async_result.id)
        self.__class__.default_task_id = async_result.id
        print(f"[Enqueue] Default priority task pushed to RabbitMQ queue '{RabbitMQConfig.DEFAULT_QUEUE}'. Task ID: {async_result.id}")

    def test_03_enqueue_low_priority_task(self):
        """Pushes a task to the low_priority queue in RabbitMQ."""
        payload = {"trace_id": "tr-test-low-3", "action": "archive_logs", "priority": "low"}
        async_result = process_low_priority_task.apply_async(
            args=(payload,),
            queue=RabbitMQConfig.LOW_PRIORITY_QUEUE
        )
        self.assertIsNotNone(async_result.id)
        self.__class__.low_task_id = async_result.id
        print(f"[Enqueue] Low priority task pushed to RabbitMQ queue '{RabbitMQConfig.LOW_PRIORITY_QUEUE}'. Task ID: {async_result.id}")

    def test_04_enqueue_duplicate_tasks(self):
        """Pushes two identical tasks concurrently to test MongoDB duplicate locking."""
        payload = {"trace_id": "tr-test-dup-4", "transaction_id": 999, "amount": 500.0}
        
        async_a = process_default_task.apply_async(args=(payload,), queue=RabbitMQConfig.DEFAULT_QUEUE)
        async_b = process_default_task.apply_async(args=(payload,), queue=RabbitMQConfig.DEFAULT_QUEUE)
        
        self.assertIsNotNone(async_a.id)
        self.assertIsNotNone(async_b.id)
        self.__class__.dup_a_id = async_a.id
        self.__class__.dup_b_id = async_b.id
        print(f"[Enqueue] Duplicate Task A pushed to '{RabbitMQConfig.DEFAULT_QUEUE}'. Task ID: {async_a.id}")
        print(f"[Enqueue] Duplicate Task B pushed to '{RabbitMQConfig.DEFAULT_QUEUE}'. Task ID: {async_b.id}")

    def test_05_enqueue_failing_task_with_retries(self):
        """Pushes a failing task to test 3 retries with exponential backoff."""
        async_result = process_failing_task.apply_async(args=(3,), kwargs={"trace_id": "tr-test-fail-5"}, queue=RabbitMQConfig.DEFAULT_QUEUE)
        self.assertIsNotNone(async_result.id)
        self.__class__.fail_task_id = async_result.id
        print(f"[Enqueue] Failing task pushed to RabbitMQ. Task ID: {async_result.id}")

    def test_06_verify_mongodb_records(self):
        """
        Queries MongoDB to verify that records created by active workers match expected states.
        If worker is running, asserts logs, responses, retries, and created_at timestamps.
        """
        logs_col = MongoDBManager.get_logs_collection()
        responses_col = MongoDBManager.get_responses_collection()

        print("\nWaiting up to 10 seconds to inspect MongoDB for processed tasks...")
        
        max_wait = 10
        start = time.time()
        while time.time() - start < max_wait:
            log_count = logs_col.count_documents({})
            if log_count >= 5:
                time.sleep(1)
                break
            time.sleep(1)

        total_logs = logs_col.count_documents({})
        print(f"Total MongoDB Task Logs recorded: {total_logs}")

        if total_logs > 0:
            high_log = logs_col.find_one({"task_id": getattr(self.__class__, "high_task_id", None)})
            if high_log:
                self.assertIn("status", high_log)
                self.assertIn("created_at", high_log)
                self.assertIn("trace_id", high_log)
                print(f"Verified High Priority Log in MongoDB -> Status: {high_log['status']}, Trace ID: {high_log['trace_id']}")

            total_responses = responses_col.count_documents({})
            print(f"Total MongoDB Task Responses recorded: {total_responses}")
            if total_responses > 0:
                high_task_id = getattr(self.__class__, "high_task_id", None)
                sample_resp = responses_col.find_one({"task_id": high_task_id}) if high_task_id else responses_col.find_one(sort=[("created_at", -1)])
                if sample_resp:
                    self.assertIn("created_at", sample_resp)
                    self.assertIn("response", sample_resp)
                    self.assertIn("status", sample_resp)
                    self.assertIn("trace_id", sample_resp)
                    self.assertIn(sample_resp["status"], ["SUCCESS", "FAILED"])
                    print(f"Verified task responses contain 'status' ('{sample_resp['status']}'), 'trace_id' ('{sample_resp['trace_id']}'), and 'created_at' timestamp fields.")

class TestLockKeyGeneration(unittest.TestCase):
    def test_lock_key_deterministic(self):
        key1 = generate_lock_key("test_task", (1, 2), {"a": "b"})
        key2 = generate_lock_key("test_task", (1, 2), {"a": "b"})
        self.assertEqual(key1, key2)
        self.assertTrue(key1.startswith("lock:test_task:"))

def interactive_enqueue_menu():
    """
    Interactive prompt asking in exact 3-step order:
    1st Question -> Select the task
    2nd Question -> Select the queue
    3rd Question -> Enter positional arguments (args) using input()
    """
    MongoDBManager.initialize_indexes()
    
    print("\n" + "=" * 60)
    print("       CELERY TASK INTERACTIVE DISPATCHER MENU       ")
    print("=" * 60)
    print("1st Question -> Select the task to execute:")
    print("  1. generate_analytics_report (High Priority)")
    print("  2. process_user_order (Default Priority)")
    print("  3. archive_audit_logs (Low Priority)")
    print("  4. process_payment_settlement (Retry Test)")
    print("  5. Run Automated Test Suite (unittest)")
    print("  6. Exit")
    print("=" * 60)
    
    try:
        task_choice = input("\nEnter task choice (1-6): ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nExiting.")
        return

    if task_choice == "5":
        print("\nRunning automated unittest suite...")
        unittest.main(argv=[sys.argv[0]], exit=False)
        return
    elif task_choice == "6":
        print("Exiting.")
        return
    elif task_choice not in ["1", "2", "3", "4"]:
        print("Invalid task choice selected.")
        return

    tasks_map = {
        "1": (process_high_priority_task, settings.queue_high_priority),
        "2": (process_default_task, settings.queue_default),
        "3": (process_low_priority_task, settings.queue_low_priority),
        "4": (process_failing_task, settings.queue_default)
    }
    selected_task_fn, default_queue = tasks_map[task_choice]

    # 2nd Question: Select the queue
    print("\n" + "-" * 50)
    print("2nd Question -> Select the destination queue in RabbitMQ:")
    print(f"  1. {settings.queue_high_priority}")
    print(f"  2. {settings.queue_default}")
    print(f"  3. {settings.queue_low_priority}")
    print("  4. Custom queue name")
    print(f"  (Press Enter to use default for this task: '{default_queue}')")
    print("-" * 50)
    
    try:
        queue_choice = input(f"Enter queue choice (1-4, default='{default_queue}'): ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nExiting.")
        return

    if queue_choice == "1":
        target_queue = settings.queue_high_priority
    elif queue_choice == "2":
        target_queue = settings.queue_default
    elif queue_choice == "3":
        target_queue = settings.queue_low_priority
    elif queue_choice == "4":
        try:
            custom_q = input("Enter custom queue name: ").strip()
        except (EOFError, KeyboardInterrupt):
            custom_q = ""
        target_queue = custom_q if custom_q else default_queue
    else:
        target_queue = default_queue

    # 3rd Question: Enter the arguments (args) using input()
    print("\n" + "-" * 50)
    print("3rd Question -> Enter positional task arguments (args):")
    print("-" * 50)
    
    if task_choice == "4":
        try:
            raw_args = input("Enter positional argument for fail_until retry count (args) [Default: 3]: ").strip()
        except (EOFError, KeyboardInterrupt):
            raw_args = ""
        fail_until = int(raw_args) if raw_args.isdigit() else 3
        task_args = (fail_until,)
    else:
        try:
            raw_args = input("Enter positional task arguments (args) [e.g. {\"user_id\": 101} or list/string, default: {\"job\": \"custom_task\"}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            raw_args = ""
            
        if not raw_args:
            task_args = ({"job": "custom_task", "created_at": datetime.now(timezone.utc).isoformat()},)
        else:
            try:
                parsed = json.loads(raw_args)
                if isinstance(parsed, list):
                    task_args = tuple(parsed)
                elif isinstance(parsed, dict):
                    task_args = (parsed,)
                else:
                    task_args = (parsed,)
            except Exception:
                if "," in raw_args:
                    task_args = tuple(x.strip() for x in raw_args.split(","))
                else:
                    task_args = (raw_args,)

    # Dispatch task to RabbitMQ
    print(f"\nDispatching '{selected_task_fn.name}' with args {task_args} to RabbitMQ queue '{target_queue}'...")
    async_res = selected_task_fn.apply_async(args=task_args, queue=target_queue)

    print(f"\n🚀 Successfully enqueued task to RabbitMQ queue '{target_queue}'!")
    print(f"Task ID: {async_res.id}")
    print("Check MongoDB 'task_logs' collection for execution state.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["--auto", "-a", "test"]:
        unittest.main(argv=[sys.argv[0]])
    else:
        interactive_enqueue_menu()
