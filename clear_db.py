import logging
from database import MongoDBManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_all_collections():
    """
    Deletes all documents across MongoDB collections:
    1. Task Request Responses (task_request_responses)
    2. Distributed Locks (task_locks)
    """
    logger.info("Connecting to MongoDB...")
    
    try:
        req_resp_col = MongoDBManager.get_task_request_responses_collection()
        locks_col = MongoDBManager.get_locks_collection()

        # Delete records
        req_resp_res = req_resp_col.delete_many({})
        locks_res = locks_col.delete_many({})

        logger.info("--- Cleanup Summary ---")
        logger.info("🗑️  Deleted %d records from '%s' collection.", req_resp_res.deleted_count, req_resp_col.name)
        logger.info("🗑️  Deleted %d records from '%s' collection.", locks_res.deleted_count, locks_col.name)
        logger.info("✅ All MongoDB collections cleared successfully!")
        
    except Exception as e:
        logger.error("Error clearing database collections: %s", e)
    finally:
        MongoDBManager.close_client()

if __name__ == "__main__":
    clear_all_collections()
