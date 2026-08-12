from kombu import Queue
from constants import settings

class RabbitMQConfig:
    """
    RabbitMQ & Celery Queue Configuration Manager.
    Defines Kombu priority queues, routing settings, broker parameters, and binds them to Celery.
    """
    # Broker connection parameters
    BROKER_URL = settings.celery_broker_url
    BROKER_CONNECTION_RETRY_ON_STARTUP = settings.broker_connection_retry_on_startup
    BROKER_POOL_LIMIT = settings.broker_pool_limit
    BROKER_HEARTBEAT = settings.broker_heartbeat

    # Priority Queue Names
    HIGH_PRIORITY_QUEUE = settings.queue_high_priority
    DEFAULT_QUEUE = settings.queue_default
    LOW_PRIORITY_QUEUE = settings.queue_low_priority

    # Celery Kombu Task Queues
    TASK_QUEUES = (
        Queue(HIGH_PRIORITY_QUEUE, routing_key=HIGH_PRIORITY_QUEUE),
        Queue(DEFAULT_QUEUE, routing_key=DEFAULT_QUEUE),
        Queue(LOW_PRIORITY_QUEUE, routing_key=LOW_PRIORITY_QUEUE),
    )

    # Queue Defaults
    TASK_DEFAULT_QUEUE = DEFAULT_QUEUE
    TASK_DEFAULT_EXCHANGE = DEFAULT_QUEUE
    TASK_DEFAULT_ROUTING_KEY = DEFAULT_QUEUE

    @classmethod
    def apply_to_celery(cls, app):
        """
        Applies RabbitMQ configuration and queue definitions directly to a Celery app instance.
        """
        app.conf.task_queues = cls.TASK_QUEUES
        app.conf.task_default_queue = cls.TASK_DEFAULT_QUEUE
        app.conf.task_default_exchange = cls.TASK_DEFAULT_EXCHANGE
        app.conf.task_default_routing_key = cls.TASK_DEFAULT_ROUTING_KEY
        
        app.conf.update(
            broker_url=cls.BROKER_URL,
            broker_connection_retry_on_startup=cls.BROKER_CONNECTION_RETRY_ON_STARTUP,
            broker_pool_limit=cls.BROKER_POOL_LIMIT,
            broker_heartbeat=cls.BROKER_HEARTBEAT,
        )
