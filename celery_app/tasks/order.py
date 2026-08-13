from celery_app.celery import app
from celery_app.base_task import MongoLoggedTask
from schema import OutputResponse
from src.biz import process_user_order_biz

@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.process_user_order")
def process_user_order(self, data: dict = None, **kwargs):
    """
    Default Priority Task: Wraps user e-commerce order transaction processing business logic.
    """
    if data is None or not isinstance(data, dict):
        data = {
            "amount": 150.0,
            "tax_rate": 0.08,
            "discount": 10.0,
            "customer_id": "CUST-1001"
        }

    order_result = process_user_order_biz(data, self.logger)
    
    response_obj = OutputResponse(
        task_response=order_result
    )
    return response_obj.model_dump()
