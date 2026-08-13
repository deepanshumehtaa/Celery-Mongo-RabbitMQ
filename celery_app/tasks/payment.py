from celery_app.celery import app
from celery_app.base_task import MongoLoggedTask
from schema import OutputResponse
from src.biz import process_payment_settlement_biz

@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.process_payment_settlement")
def process_payment_settlement(self, fail_until_retry: int = 3, **kwargs):
    """
    Failing Task: Wraps payment gateway settlement logic with transient timeout simulations.
    """
    settlement_result = process_payment_settlement_biz(
        fail_until_retry=fail_until_retry,
        current_retry=self.request.retries,
        t_logger=self.logger
    )
    
    response_obj = OutputResponse(
        task_response=settlement_result
    )
    return response_obj.model_dump()
