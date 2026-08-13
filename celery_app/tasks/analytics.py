from celery_app.celery import app
from celery_app.base_task import MongoLoggedTask
from schema import OutputResponse
from src.biz import generate_analytics_report_biz

@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.generate_analytics_report")
def generate_analytics_report(self, data: dict = None, **kwargs):
    """
    High Priority Task: Wraps analytics report generation business logic.
    """
    if data is None or not isinstance(data, dict):
        data = {
            "metrics": [100.0, 250.5, 400.0, 75.25],
            "report_type": "Executive Summary"
        }
        
    report_summary = generate_analytics_report_biz(data, self.logger)
    
    response_obj = OutputResponse(
        task_response=report_summary
    )
    return response_obj.model_dump()
