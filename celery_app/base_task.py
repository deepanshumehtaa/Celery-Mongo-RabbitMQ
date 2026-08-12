import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from celery import Task
from celery.exceptions import Retry

from database import MongoDBManager
from celery_app.lock import MongoLock, LockAcquisitionError
from utils.logger import get_trace_logger

logger = get_trace_logger(__name__)

def extract_trace_id(args: tuple, kwargs: dict) -> str:
    """
    Extracts trace_id from kwargs or dictionary arguments.
    Generates a unique trace_id if not supplied in incoming payload.
    """
    if "trace_id" in kwargs and kwargs["trace_id"]:
        return str(kwargs["trace_id"])
    for arg in args:
        if isinstance(arg, dict) and "trace_id" in arg and arg["trace_id"]:
            return str(arg["trace_id"])
    return f"tr-{uuid.uuid4().hex[:8]}"

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

def log_task_state(task_id: str, task_name: str, lock_key: str, trace_id: str, args: tuple, kwargs: dict, status: str, error_message: str = None, retry_count: int = 0):
    """
    Logs task state updates directly to MongoDB including trace_id.
    """
    try:
        logs_col = MongoDBManager.get_logs_collection()
        now = datetime.now(timezone.utc)
        
        update_data = {
            "task_name": task_name,
            "lock_key": lock_key,
            "trace_id": trace_id,
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
        logger.error("Failed to log task state to MongoDB: %s", e, trace_id=trace_id)

def write_task_response(task_id: str, task_name: str, trace_id: str, result: any, status: str = "SUCCESS"):
    """
    Saves the final response/result of a task (with status SUCCESS or FAILED and trace_id) to MongoDB.
    """
    try:
        responses_col = MongoDBManager.get_responses_collection()
        now = datetime.now(timezone.utc)
        
        responses_col.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "task_name": task_name,
                    "trace_id": trace_id,
                    "status": status,
                    "response": result,
                    "created_at": now
                }
            },
            upsert=True
        )
    except Exception as e:
        logger.error("Failed to write task response to MongoDB: %s", e, trace_id=trace_id)


class MongoLoggedTask(Task):
    """
    Custom Celery Task subclass implementing DRY & SOLID design:
    - Encapsulates execution tracking, storing state and trace_id to MongoDB.
    - Integrates a distributed MongoLock.
    - Captures task outcomes (writing success/failure outputs with trace_id to responses).
    - Enforces 3 retries and exponential backoff on exceptions.
    """
    abstract = True

    def __call__(self, *args, **kwargs):
        task_id = self.request.id
        task_name = self.name
        trace_id = extract_trace_id(args, kwargs)
        task_logger = get_trace_logger(__name__, trace_id=trace_id)
        
        lock_key = generate_lock_key(task_name, args, kwargs)
        
        # 1. Idempotency Check: check if the exact task_id has already completed successfully
        responses_col = MongoDBManager.get_responses_collection()
        existing_response = responses_col.find_one({"task_id": task_id})
        if existing_response:
            task_logger.info("Task %s [%s] already completed successfully. Skipping execution.", task_name, task_id)
            return existing_response.get("response")

        # 2. Duplicate Check: check if a task with the exact same inputs (lock_key) has already succeeded
        logs_col = MongoDBManager.get_logs_collection()
        existing_run = logs_col.find_one({"lock_key": lock_key, "status": "SUCCESS"})
        if existing_run:
            task_logger.info("Task %s with duplicate arguments has already executed successfully. Skipping.", task_name)
            resp = responses_col.find_one({"task_id": existing_run["task_id"]})
            return resp.get("response") if resp else None

        # 3. Log STARTED state in database
        log_task_state(
            task_id=task_id,
            task_name=task_name,
            lock_key=lock_key,
            trace_id=trace_id,
            args=args,
            kwargs=kwargs,
            status="STARTED",
            retry_count=self.request.retries
        )

        # 4. Enforce Locking
        lock = MongoLock(lock_key=lock_key, task_id=task_id, trace_id=trace_id)
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
                trace_id=trace_id,
                args=args,
                kwargs=kwargs,
                status="SUCCESS",
                retry_count=self.request.retries
            )
            # Write response with status="SUCCESS" to separate collection
            write_task_response(task_id, task_name, trace_id, result, status="SUCCESS")
            return result

        except LockAcquisitionError as e:
            # Skip duplicate task execution since lock is held
            log_task_state(
                task_id=task_id,
                task_name=task_name,
                lock_key=lock_key,
                trace_id=trace_id,
                args=args,
                kwargs=kwargs,
                status="SKIPPED",
                error_message=str(e),
                retry_count=self.request.retries
            )
            return None

        except Retry:
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
                    trace_id=trace_id,
                    args=args,
                    kwargs=kwargs,
                    status="RETRYING",
                    error_message=f"{type(exc).__name__}: {str(exc)}",
                    retry_count=current_retry + 1
                )
                
                task_logger.warning(
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
                    trace_id=trace_id,
                    args=args,
                    kwargs=kwargs,
                    status="FAILED",
                    error_message=f"{type(exc).__name__}: {str(exc)}",
                    retry_count=current_retry
                )
                # Write failed response with status="FAILED" to task_responses collection
                write_task_response(task_id, task_name, trace_id, f"{type(exc).__name__}: {str(exc)}", status="FAILED")
                task_logger.error("Task %s [%s] failed permanently after %d retries. Error: %s", task_name, task_id, current_retry, exc)
                raise exc
        finally:
            if acquired:
                lock.release()
