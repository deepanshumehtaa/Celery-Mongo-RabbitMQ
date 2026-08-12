import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from celery import Task
from celery.exceptions import Retry

from celery_app.celery import app
from database import MongoDBManager
from celery_app.lock import MongoLock, LockAcquisitionError

logger = logging.getLogger(__name__)

def generate_lock_key(task_name: str, args: tuple, kwargs: dict) -> str:
    """
    Generates a deterministic unique lock key based on the task name and its arguments.
    Ensures arguments are serialized in a stable order.
    """
    try:
        stable_kwargs = json.dumps(kwargs, sort_keys=True, default=str)
        stable_args = json.dumps(args, default=str)
    except Exception as e:
        logger.error("Failed to serialize task arguments: %s", e)
        stable_args = str(args)
        stable_kwargs = str(kwargs)
        
    raw_key = f"{task_name}:{stable_args}:{stable_kwargs}"
    hasher = hashlib.sha256(raw_key.encode("utf-8"))
    return f"lock:{task_name}:{hasher.hexdigest()}"

def log_task_state(task_id: str, task_name: str, lock_key: str, args: tuple, kwargs: dict, status: str, error_message: str = None, retry_count: int = 0):
    """
    Logs task state updates directly to MongoDB.
    """
    try:
        logs_col = MongoDBManager.get_logs_collection()
        now = datetime.now(timezone.utc)
        
        update_data = {
            "task_name": task_name,
            "lock_key": lock_key,
            "args": list(args),
            "kwargs": kwargs,
            "status": status,
            "created_at": now,
            "retry_count": retry_count
        }
        
        if error_message:
            update_data["error_message"] = error_message
            
        logs_col.update_one(
            {"task_id": task_id},
            {"$set": update_data},
            upsert=True
        )
    except Exception as e:
        logger.error("Failed to log task state to MongoDB: %s", e)

def write_task_response(task_id: str, task_name: str, result: any):
    """
    Saves the final response/result of a successful task to a separate MongoDB collection.
    """
    try:
        responses_col = MongoDBManager.get_responses_collection()
        now = datetime.now(timezone.utc)
        
        responses_col.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "task_name": task_name,
                    "response": result,
                    "created_at": now
                }
            },
            upsert=True
        )
    except Exception as e:
        logger.error("Failed to write task response to MongoDB: %s", e)


class MongoLoggedTask(Task):
    """
    Custom Celery Task subclass implementing DRY & SOLID design:
    - Encapsulates execution tracking, storing state to MongoDB.
    - Integrates a distributed MongoLock.
    - Captures task outcomes (writing success outputs to responses).
    - Enforces 3 retries and exponential backoff on exceptions.
    """
    abstract = True

    def __call__(self, *args, **kwargs):
        task_id = self.request.id
        task_name = self.name
        lock_key = generate_lock_key(task_name, args, kwargs)
        
        # 1. Idempotency Check: check if the exact task_id has already completed successfully
        responses_col = MongoDBManager.get_responses_collection()
        existing_response = responses_col.find_one({"task_id": task_id})
        if existing_response:
            logger.info("Task %s [%s] already completed successfully. Skipping execution.", task_name, task_id)
            return existing_response.get("response")

        # 2. Duplicate Check: check if a task with the exact same inputs (lock_key) has already succeeded
        logs_col = MongoDBManager.get_logs_collection()
        existing_run = logs_col.find_one({"lock_key": lock_key, "status": "SUCCESS"})
        if existing_run:
            logger.info("Task %s with duplicate arguments has already executed successfully. Skipping.", task_name)
            resp = responses_col.find_one({"task_id": existing_run["task_id"]})
            return resp.get("response") if resp else None

        # 3. Log STARTED state in database
        log_task_state(
            task_id=task_id,
            task_name=task_name,
            lock_key=lock_key,
            args=args,
            kwargs=kwargs,
            status="STARTED",
            retry_count=self.request.retries
        )

        # 4. Enforce Locking
        lock = MongoLock(lock_key=lock_key, task_id=task_id)
        acquired = False
        try:
            lock.acquire()
            acquired = True
            
            # Execute actual task body
            result = super().__call__(*args, **kwargs)
            
            # Log successful completion
            log_task_state(
                task_id=task_id,
                task_name=task_name,
                lock_key=lock_key,
                args=args,
                kwargs=kwargs,
                status="SUCCESS",
                retry_count=self.request.retries
            )
            # Write response to separate collection
            write_task_response(task_id, task_name, result)
            return result

        except LockAcquisitionError as e:
            # Skip duplicate task execution since lock is held
            log_task_state(
                task_id=task_id,
                task_name=task_name,
                lock_key=lock_key,
                args=args,
                kwargs=kwargs,
                status="SKIPPED",
                error_message=str(e),
                retry_count=self.request.retries
            )
            return None

        except Retry:
            # Re-raise Celery's internal Retry exception without editing state.
            # We already updated state to 'RETRYING' before raising self.retry() below.
            raise

        except Exception as exc:
            current_retry = self.request.retries
            max_retries = 3
            if current_retry < max_retries:
                # Exponential backoff: 2s, 4s, 8s
                countdown = 2 ** (current_retry + 1)
                
                log_task_state(
                    task_id=task_id,
                    task_name=task_name,
                    lock_key=lock_key,
                    args=args,
                    kwargs=kwargs,
                    status="RETRYING",
                    error_message=f"{type(exc).__name__}: {str(exc)}",
                    retry_count=current_retry + 1
                )
                
                logger.warning(
                    "Task %s [%s] failed. Retrying (attempt %d/%d) in %ds. Error: %s",
                    task_name, task_id, current_retry + 1, max_retries, countdown, exc
                )
                raise self.retry(exc=exc, countdown=countdown, max_retries=max_retries)
            else:
                # Max retries exceeded
                log_task_state(
                    task_id=task_id,
                    task_name=task_name,
                    lock_key=lock_key,
                    args=args,
                    kwargs=kwargs,
                    status="FAILED",
                    error_message=f"{type(exc).__name__}: {str(exc)}",
                    retry_count=current_retry
                )
                logger.error("Task %s [%s] failed permanently after %d retries. Error: %s", task_name, task_id, current_retry, exc)
                raise exc
        finally:
            if acquired:
                lock.release()

# ----------------- Task Definitions -----------------

@app.task(base=MongoLoggedTask, bind=True)
def process_high_priority_task(self, data: dict):
    """
    Sample high priority task. Simulate processing heavy operations.
    """
    logger.info("Starting high priority task execution with data: %s", data)
    time.sleep(2)  # Simulate processing delay
    return {
        "status": "completed",
        "priority": "high",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "input_data": data
    }


@app.task(base=MongoLoggedTask, bind=True)
def process_default_task(self, data: dict):
    """
    Sample default task.
    """
    logger.info("Starting default task execution with data: %s", data)
    return {
        "status": "completed",
        "priority": "default",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "input_data": data
    }


@app.task(base=MongoLoggedTask, bind=True)
def addition_task(self, data: dict):
    """
    Sample low priority task.
    """
    res = sum(data.values())
    logger.info("Starting low priority task execution with data: %s", data)
    return {
        "status": "completed",
        "priority": "low",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "input_data": data,
        "res": res,
    }


@app.task(base=MongoLoggedTask, bind=True)
def process_failing_task(self, fail_until_retry: int = 3):
    """
    Fails repeatedly to demonstrate the exponential backoff retry mechanism.
    If self.request.retries is less than fail_until_retry, it raises a ValueError.
    """
    current_retry = self.request.retries
    logger.info("Executing failing task: attempt %d (will fail until attempt %d)",
                current_retry + 1, fail_until_retry + 1)
    
    if current_retry < fail_until_retry:
        raise ValueError(f"Simulated transient error on attempt {current_retry + 1}")
        
    return {
        "status": "recovered",
        "message": f"Successfully recovered on attempt {current_retry + 1}",
        "recovered_at": datetime.now(timezone.utc).isoformat()
    }
