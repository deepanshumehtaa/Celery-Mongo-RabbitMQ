from celery_app.celery import app
from celery_app.base_task import MongoLoggedTask
from schema import OutputResponse
from src.biz import archive_audit_logs_biz

@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.archive_audit_logs")
def archive_audit_logs(self, data: dict = None, **kwargs):
    """
    Low Priority Task: Wraps system audit logs archiving business logic.
    """
    if data is None or not isinstance(data, dict):
        data = {
            "log_entries": 500,
            "archive_type": "audit_logs"
        }

    archival_result = archive_audit_logs_biz(data, self.logger)
    
    response_obj = OutputResponse(
        task_response=archival_result
    )
    return response_obj.model_dump()
