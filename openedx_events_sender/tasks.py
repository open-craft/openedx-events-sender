"""Asynchronous Celery tasks."""

from typing import Dict

import requests
from celery import Celery
from django.conf import settings


def get_celery_app():
    """Get Celery app to reuse configuration and queues."""
    if getattr(settings, 'TESTING', False):
        # We can ignore this in the testing environment.
        return Celery(task_always_eager=True)

    from lms import CELERY_APP  # noqa pylint: disable=import-error, import-outside-toplevel # pragma: no cover

    return CELERY_APP  # pragma: no cover


app = get_celery_app()


@app.task
def send_data_task(data: Dict[str, str], url: str, headers: Dict[str, str]):
    """Send the application/json POST request with `data` and `headers` to the `url`."""
    return requests.post(url, json=data, headers=headers, timeout=getattr(settings, "EVENT_SENDER_TIMEOUT", 30))
