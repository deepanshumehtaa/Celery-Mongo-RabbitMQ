import logging
from database import MongoDBManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_all_collections():
    """
    Deletes all documents across the 3 MongoDB collections:
    1. Task Logs (task_logs)
    2. Task Responses (task_responses)
    3. Distributed Locks (task_locks)
    """
    logger.info("Connecting to MongoDB...")
    
    try:
        logs_col = MongoDBManager.get_logs_collection()
        responses_col = MongoDBManager.get_responses_collection()
        locks_col = MongoDBManager.get_locks_collection()

        # Delete records
        logs_res = logs_col.delete_many({})
        responses_res = responses_col.delete_many({})
        locks_res = locks_col.delete_many({})

        logger.info("--- Cleanup Summary ---")
        logger.info("🗑️  Deleted %d records from '%s' collection.", logs_res.deleted_count, logs_col.name)
        logger.info("🗑️  Deleted %d records from '%s' collection.", responses_res.deleted_count, responses_col.name)
        logger.info("🗑️  Deleted %d records from '%s' collection.", locks_res.deleted_count, locks_col.name)
        logger.info("✅ All 3 MongoDB collections cleared successfully!")
        
    except Exception as e:
        logger.error("Error clearing database collections: %s", e)
    finally:
        MongoDBManager.close_client()

if __name__ == "__main__":
    clear_all_collections()
