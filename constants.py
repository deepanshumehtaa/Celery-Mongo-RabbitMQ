from pydantic_settings import BaseSettings, SettingsConfigDict


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
    celery_broker_url: str = "amqp://guest:guest@localhost:5672//"
    broker_connection_retry_on_startup: bool = True
    broker_pool_limit: int = 10
    broker_heartbeat: int = 60

    # RabbitMQ Priority Queue Names
    queue_high_priority: str = "high_priority"
    queue_default: str = "default"
    queue_low_priority: str = "low_priority"

    # MongoDB configuration settings
    mongo_uri: str = "mongodb://localhost:27017/"
    mongo_db_name: str = "celery_tasks"
    mongo_logs_collection: str = "task_logs"
    mongo_responses_collection: str = "task_responses"
    mongo_locks_collection: str = "task_locks"
    mongo_lock_timeout_seconds: int = 300

# Global configuration instance
settings = Settings()
