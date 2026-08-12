"""
uv run celery -A celery_app.celery worker -Q high_priority,default,low_priority --loglevel=info
"""

import logging
from celery import Celery
from celery.signals import worker_ready, worker_shutdown
from app_configs.rabbitmq_config import RabbitMQConfig
from database import MongoDBManager

logger = logging.getLogger(__name__)

# Initialize Celery
app = Celery(
    "celery_app",
    include=["celery_app.tasks"]
)

# Apply RabbitMQ broker settings & Kombu priority task queues from app_configs/rabbitmq_config.py
RabbitMQConfig.apply_to_celery(app)

# Celery performance and reliability configurations
app.conf.update(
    # Late acknowledgment: worker acknowledges the message AFTER task completion.
    # This prevents task loss if the worker crashes during execution.
    task_acks_late=True,
    
    # Prefetch multiplier: set to 1 so worker processes pull only one task at a time.
    # Essential for priority queues to distribute tasks dynamically and fairly.
    worker_prefetch_multiplier=1,
    
    # Requeue tasks on worker loss (e.g. OOM or hard crash)
    task_reject_on_worker_lost=True,
    
    # Serialization format
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)


@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    """
    Hook executed when the worker process finishes startup and is ready to process tasks.
    We use this to initialize our database connections and indexes.
    """
    logger.info("Celery worker ready. Initializing MongoDB database and indexes...")
    try:
        MongoDBManager.initialize_indexes()
        logger.info("MongoDB initialized successfully on worker startup.")
    except Exception as e:
        logger.critical("Failed to initialize MongoDB during worker startup: %s", e)


@worker_shutdown.connect
def on_worker_shutdown(sender, **kwargs):
    """
    Hook executed when the worker process is shutting down.
    Ensures MongoDB connection pool is closed gracefully.
    """
    logger.info("Celery worker shutting down. Cleaning up connections...")
    MongoDBManager.close_client()
