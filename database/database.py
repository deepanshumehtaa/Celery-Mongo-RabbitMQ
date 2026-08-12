import logging
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from constants import settings

logger = logging.getLogger(__name__)

class MongoDBManager:
    """
    Singleton connection manager for MongoDB.
    Ensures a single connection pool is managed and reused across tasks.
    """
    _client: MongoClient = None

    @classmethod
    def get_client(cls) -> MongoClient:
        if cls._client is None:
            logger.info("Initializing MongoDB Client with URI: %s", settings.mongo_uri)
            cls._client = MongoClient(
                settings.mongo_uri,
                # Enforce safe connection pool behaviors suitable for celery workers
                maxPoolSize=50,
                minPoolSize=10,
                serverSelectionTimeoutMS=5000
            )
        return cls._client

    @classmethod
    def get_db(cls) -> Database:
        client = cls.get_client()
        return client[settings.mongo_db_name]

    @classmethod
    def get_collection(cls, collection_name: str) -> Collection:
        db = cls.get_db()
        return db[collection_name]

    @classmethod
    def get_logs_collection(cls) -> Collection:
        return cls.get_collection(settings.mongo_logs_collection)

    @classmethod
    def get_responses_collection(cls) -> Collection:
        return cls.get_collection(settings.mongo_responses_collection)

    @classmethod
    def get_locks_collection(cls) -> Collection:
        return cls.get_collection(settings.mongo_locks_collection)

    @classmethod
    def close_client(cls):
        if cls._client is not None:
            logger.info("Closing MongoDB Client connection.")
            cls._client.close()
            cls._client = None

    @classmethod
    def initialize_indexes(cls):
        """
        Initializes indexes required by the application:
        - Unique index on lock_key in the locks collection
        - TTL index on expires_at in the locks collection
        - Indexes on task_id for fast log and response lookups
        """
        try:
            # Setup Lock indexes
            locks_col = cls.get_locks_collection()
            locks_col.create_index("lock_key", unique=True)
            locks_col.create_index("expires_at", expireAfterSeconds=0)
            
            # Setup Log indexes
            logs_col = cls.get_logs_collection()
            logs_col.create_index("task_id", unique=True)
            logs_col.create_index("status")
            
            # Setup Response indexes
            responses_col = cls.get_responses_collection()
            responses_col.create_index("task_id", unique=True)
            
            logger.info("MongoDB indexes initialized successfully.")
        except Exception as e:
            logger.error("Failed to initialize MongoDB indexes: %s", e)
            raise e
