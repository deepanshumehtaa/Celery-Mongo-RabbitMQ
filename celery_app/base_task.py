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
from schema import OutputResponse, EnumStatus, TaskRequestResponseDoc

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

def save_task_request_response(
    task_id: str,
    task_name: str,
    trace_id: str,
    lock_key: str,
    args: tuple,
    kwargs: dict,
    status: EnumStatus,
    response: dict = None,
    error_message: str = None,
    retry_count: int = 0
):
    """
    Saves and updates task request details using the TaskRequestResponseDoc model.
    """
    try:
        req_resp_col = MongoDBManager.get_task_request_responses_collection()
        now_str = datetime.now(timezone.utc).isoformat()

        # Format input payload cleanly under 'input'
        if len(args) == 1 and not kwargs:
            input_payload = args[0]
        elif not args and kwargs:
            input_payload = kwargs
        else:
            input_payload = {"args": list(args), "kwargs": kwargs}

        # Check if record already exists to preserve created_at
        existing = req_resp_col.find_one({"task_id": task_id})
        created_at_val = existing.get("created_at") if existing else now_str

        # Construct nested response model if provided
        response_obj = None
        if response is not None:
            if isinstance(response, dict) and ("task_response" in response or "error_message" in response):
                response_obj = OutputResponse(
                    task_response=response.get("task_response"),
                    error_message=response.get("error_message")
                )
            else:
                response_obj = OutputResponse(
                    task_response=None,
                    error_message=str(response)
                )

        doc = TaskRequestResponseDoc(
            task_id=task_id,
            task_name=task_name,
            trace_id=trace_id,
            lock_key=lock_key,
            input=input_payload,
            status=status,
            retry_count=retry_count,
            created_at=created_at_val,
            updated_at=now_str,
            response=response_obj
        )

        doc_dict = doc.model_dump(exclude_none=True)
        if error_message:
            doc_dict["error_message"] = error_message

        req_resp_col.update_one(
            {"task_id": task_id},
            {"$set": doc_dict},
            upsert=True
        )
    except Exception as e:
        logger.error("Failed to save task request/response to MongoDB: %s", e, trace_id=trace_id)

# Backward compatibility aliases
log_task_state = save_task_request_response
write_task_response = save_task_request_response


class MongoLoggedTask(Task):
    """
    Custom Celery Task subclass implementing DRY & SOLID design:
    - Stores incoming input parameters (under 'input'), execution state, and response output
      in a single MongoDB collection ('task_request_responses').
    - Integrates a distributed MongoLock.
    - Captures task outcomes (writing success/failure outputs with trace_id to response field).
    - Enforces retries and exponential backoff on exceptions.
    """
    abstract = True

    @property
    def trace_id(self) -> str:
        """
        Exposes trace_id stored in the task request context.
        """
        return getattr(self.request, "trace_id", None) or "N/A"

    @property
    def logger(self):
        """
        Provides a trace-adapter logging instance dynamically bound to the current task.
        """
        return get_trace_logger(self.name, trace_id=self.trace_id)

    def __call__(self, *args, **kwargs):
        task_id = self.request.id
        task_name = self.name
        
        # Extract trace_id and persist it inside kwargs so it stays identical across retries
        trace_id = extract_trace_id(args, kwargs)
        if isinstance(kwargs, dict):
            if "trace_id" not in kwargs or not kwargs["trace_id"]:
                kwargs["trace_id"] = trace_id
        if self.request.kwargs is not None and isinstance(self.request.kwargs, dict):
            self.request.kwargs["trace_id"] = trace_id
            
        self.request.trace_id = trace_id
        
        task_logger = get_trace_logger(__name__, trace_id=trace_id)
        lock_key = generate_lock_key(task_name, args, kwargs)
        req_resp_col = MongoDBManager.get_task_request_responses_collection()
        
        # 1. Idempotency Check: check if the exact task_id has already completed successfully
        existing_response = req_resp_col.find_one({"task_id": task_id, "status": EnumStatus.SUCCESS})
        if existing_response and "response" in existing_response:
            task_logger.info("Task %s [%s] already completed successfully. Skipping execution.", task_name, task_id)
            return existing_response.get("response")

        # 2. Duplicate Check: check if a task with the exact same inputs (lock_key) has already succeeded
        existing_run = req_resp_col.find_one({"lock_key": lock_key, "status": EnumStatus.SUCCESS})
        if existing_run:
            task_logger.info("Task %s with duplicate arguments has already executed successfully. Skipping.", task_name)
            cached_result = existing_run.get("response")

            # Save request entry as SUCCESS with cached response for the current task_id
            save_task_request_response(
                task_id=task_id,
                task_name=task_name,
                trace_id=trace_id,
                lock_key=lock_key,
                args=args,
                kwargs=kwargs,
                status=EnumStatus.SUCCESS,
                response=cached_result,
                retry_count=self.request.retries
            )
            return cached_result

        # 3. Log STARTED state in database (saving input parameters immediately on request start)
        save_task_request_response(
            task_id=task_id,
            task_name=task_name,
            trace_id=trace_id,
            lock_key=lock_key,
            args=args,
            kwargs=kwargs,
            status=EnumStatus.IN_PROGRESS,
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

            # Determine completion status from result dictionary
            db_status = EnumStatus.SUCCESS
            error_message = None
            if isinstance(result, dict):
                if result.get("error_message") is not None:
                    db_status = EnumStatus.FAILED
                    error_message = result.get("error_message")
                elif result.get("status") == EnumStatus.FAILED:
                    db_status = EnumStatus.FAILED
                    error_message = result.get("error_message")

            # Update document with final status and result response payload
            save_task_request_response(
                task_id=task_id,
                task_name=task_name,
                trace_id=trace_id,
                lock_key=lock_key,
                args=args,
                kwargs=kwargs,
                status=db_status,
                response=result,
                error_message=error_message,
                retry_count=self.request.retries
            )
            return result

        except LockAcquisitionError as e:
            # Task execution failed due to active lock conflict
            lock_err_payload = OutputResponse(
                error_message=f"LockAcquisitionError: {str(e)}"
            ).model_dump()

            save_task_request_response(
                task_id=task_id,
                task_name=task_name,
                trace_id=trace_id,
                lock_key=lock_key,
                args=args,
                kwargs=kwargs,
                status=EnumStatus.FAILED,
                response=lock_err_payload,
                error_message=str(e),
                retry_count=self.request.retries
            )
            return None

        except Retry:
            raise

        except Exception as exc:
            current_retry = self.request.retries
            configured_max_retries = getattr(self, "max_retries", 3)
            if kwargs.get("no_retry", False):
                configured_max_retries = 0
            elif "max_retries" in kwargs:
                configured_max_retries = kwargs["max_retries"]

            if current_retry < configured_max_retries:
                # Exponential backoff: 2s, 4s, 8s
                countdown = 2 ** (current_retry + 1)
                
                save_task_request_response(
                    task_id=task_id,
                    task_name=task_name,
                    trace_id=trace_id,
                    lock_key=lock_key,
                    args=args,
                    kwargs=kwargs,
                    status=EnumStatus.RETRYING,
                    error_message=f"{type(exc).__name__}: {str(exc)}",
                    retry_count=current_retry + 1
                )
                
                task_logger.warning(
                    "Task %s [%s] failed. Retrying (attempt %d/%d) in %ds. Error: %s",
                    task_name, task_id, current_retry + 1, configured_max_retries, countdown, exc
                )
                raise self.retry(exc=exc, countdown=countdown, max_retries=configured_max_retries, kwargs=kwargs)
            else:
                # Max retries exceeded (or max_retries == 0)
                error_msg = f"{type(exc).__name__}: {str(exc)}"
                err_payload = OutputResponse(
                    error_message=error_msg
                ).model_dump()

                save_task_request_response(
                    task_id=task_id,
                    task_name=task_name,
                    trace_id=trace_id,
                    lock_key=lock_key,
                    args=args,
                    kwargs=kwargs,
                    status=EnumStatus.FAILED,
                    response=err_payload,
                    error_message=error_msg,
                    retry_count=current_retry
                )
                task_logger.error("Task %s [%s] failed permanently (retry %d/%d). Error: %s", task_name, task_id, current_retry, configured_max_retries, exc)
                raise exc
        finally:
            if acquired:
                lock.release()
