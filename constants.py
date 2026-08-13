import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load environment variables from .env using python-dotenv
load_dotenv()


class Settings(BaseSettings):
    """
    Centralized configuration settings class loaded from environment variables.
    Defines key constants, RabbitMQ parameters, and DB settings used across the application.
    """
    model_config = SettingsConfigDict(
        # Load environment variables from local .env file
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    # RabbitMQ / Celery Broker settings
    celery_broker_url: str = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
    broker_connection_retry_on_startup: bool = os.getenv("BROKER_CONNECTION_RETRY_ON_STARTUP", "true").lower() == "true"
    broker_pool_limit: int = int(os.getenv("BROKER_POOL_LIMIT", "10"))
    broker_heartbeat: int = int(os.getenv("BROKER_HEARTBEAT", "60"))

    # RabbitMQ Priority Queue Names
    queue_high_priority: str = os.getenv("QUEUE_HIGH_PRIORITY", "high_priority")
    queue_default: str = os.getenv("QUEUE_DEFAULT", "default")
    queue_low_priority: str = os.getenv("QUEUE_LOW_PRIORITY", "low_priority")

    # MongoDB configuration settings
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    mongo_db_name: str = os.getenv("MONGO_DB_NAME", "celery_tasks")
    mongo_task_request_responses_collection: str = os.getenv("MONGO_TASK_REQUEST_RESPONSES_COLLECTION", "task_request_responses")
    mongo_locks_collection: str = os.getenv("MONGO_LOCKS_COLLECTION", "task_locks")
    mongo_lock_timeout_seconds: int = int(os.getenv("MONGO_LOCK_TIMEOUT_SECONDS", "300"))

# Global configuration instance
settings = Settings()
