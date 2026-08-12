import logging
from datetime import datetime, timedelta, timezone
from pymongo.errors import DuplicateKeyError
from constants import settings
from database import MongoDBManager
from utils.logger import get_trace_logger

logger = get_trace_logger(__name__)


class LockAcquisitionError(Exception):
    """Raised when a lock cannot be acquired because it is already held by another worker."""
    pass


class MongoLock:
    """
    Distributed lock implementation using MongoDB.
    Prevents concurrent execution of duplicate tasks by enforcing unique lock keys.
    """
    def __init__(self, lock_key: str, task_id: str, trace_id: str = None, timeout_seconds: int = None):
        self.lock_key = lock_key
        self.task_id = task_id
        self.trace_id = trace_id or "N/A"
        self.timeout_seconds = timeout_seconds or settings.mongo_lock_timeout_seconds
        self.collection = MongoDBManager.get_locks_collection()
        self.logger = get_trace_logger(__name__, trace_id=self.trace_id)

    def acquire(self) -> bool:
        """
        Attempts to acquire the lock by inserting a document.
        If a duplicate key error is encountered, checks if the lock has expired,
        attempting to atomic-update (re-acquire) it if expired.
        
        Returns:
            True if acquired.
        Raises:
            LockAcquisitionError if the lock is held by another active task.
        """
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=self.timeout_seconds)

        lock_doc = {
            "lock_key": self.lock_key,
            "task_id": self.task_id,
            "acquired_at": now,
            "expires_at": expires_at
        }

        try:
            self.collection.insert_one(lock_doc)
            logger.info("Successfully acquired lock for key: %s (task_id: %s)", self.lock_key, self.task_id)
            return True
        except DuplicateKeyError:
            # The lock document already exists.
            existing = self.collection.find_one({"lock_key": self.lock_key})
            if existing:
                existing_expires = existing.get("expires_at")
                
                # Make sure existing_expires is timezone-aware
                if existing_expires and existing_expires.tzinfo is None:
                    existing_expires = existing_expires.replace(tzinfo=timezone.utc)

                if existing_expires and now > existing_expires:
                    logger.warning("Found expired lock for key: %s. Attempting to re-acquire.", self.lock_key)
                    # Atomically update the expired lock check to prevent race conditions
                    result = self.collection.find_one_and_update(
                        {
                            "lock_key": self.lock_key,
                            "expires_at": existing.get("expires_at")  # Optimistic check
                        },
                        {
                            "$set": {
                                "task_id": self.task_id,
                                "acquired_at": now,
                                "expires_at": expires_at
                            }
                        }
                    )
                    if result:
                        logger.info("Successfully re-acquired expired lock for key: %s (task_id: %s)", self.lock_key, self.task_id)
                        return True
            
            logger.warning("Lock conflict for key: %s (task_id: %s). Lock is held by another task.", self.lock_key, self.task_id)
            raise LockAcquisitionError(f"Lock already held for key: {self.lock_key}")

    def release(self):
        """
        Releases the lock.
        Only deletes the lock document if the task_id matches, preventing a slower task
        from releasing a lock that has expired and been acquired by another task.
        """
        try:
            result = self.collection.delete_one({"lock_key": self.lock_key, "task_id": self.task_id})
            if result.deleted_count > 0:
                logger.info("Successfully released lock for key: %s (task_id: %s)", self.lock_key, self.task_id)
            else:
                logger.debug(
                    "Lock key: %s (task_id: %s) was not deleted (possibly already expired or owned by someone else)",
                    self.lock_key,
                    self.task_id)
        except Exception as e:
            logger.error("Error releasing lock for key: %s: %s", self.lock_key, e)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
