from celery_app.celery import app
from celery_app.base_task import MongoLoggedTask, save_task_request_response, generate_lock_key
from schema import OutputResponse, EnumStatus
from src.biz import always_failing_logic

@app.task(base=MongoLoggedTask, bind=True, name="celery_app.tasks.always_failing_task", max_retries=2)
def always_failing_task(self, data: dict = None, **kwargs):
    """
    Always Failing Task: Intentionally raises an exception on retry attempts.
    """
    if data is None or not isinstance(data, dict):
        data = {"reason": "Simulated permanent system failure"}
        
    current_retry = self.request.retries
    max_retries = kwargs.get("max_retries", getattr(self, "max_retries", 2))
    if kwargs.get("no_retry", False):
        max_retries = 0

    err_msg = None
    try:
        always_failing_logic(current_retry, self.logger)
    except Exception as e:
        if current_retry < max_retries:
            countdown = 2 ** (current_retry + 1)
            self.logger.warning("Task %s failed on attempt %d/%d. Retrying in %ds. Error: %s", self.name, current_retry + 1, max_retries + 1, countdown, e)
            save_task_request_response(
                task_id=self.request.id,
                task_name=self.name,
                trace_id=self.trace_id,
                lock_key=generate_lock_key(self.name, (data,), kwargs),
                args=(data,),
                kwargs=kwargs,
                status=EnumStatus.RETRYING,
                error_message=str(e),
                retry_count=current_retry + 1
            )
            raise self.retry(exc=e, countdown=countdown, max_retries=max_retries, kwargs=kwargs)
        else:
            err_msg = str(e)

    return OutputResponse(
        error_message=err_msg
    ).model_dump()
