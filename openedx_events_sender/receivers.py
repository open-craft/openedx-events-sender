"""
Handlers for `openedx-events` signals.

Idea: when we add support for more signals, we should consider subscribing to all signals from `openedx-events`, and
handling only the ones defined in settings. This way, we could change `send_enrollment_data` to a generic `send_data`.
"""

import logging
from typing import Any, Dict, Optional

from django.conf import settings
from openedx_events.learning.data import CourseEnrollmentData

from openedx_events_sender.tasks import send_data_task
from openedx_events_sender.utils import prepare_data

log = logging.getLogger(__name__)


def send_enrollment_data(enrollment: CourseEnrollmentData, **_kwargs):
    """
    Send enrollment data after receiving the `COURSE_ENROLLMENT_CHANGED` signal.
    """
    if not (url := getattr(settings, "EVENT_SENDER_ENROLLMENT_URL", None)):
        return
    headers = getattr(settings, "EVENT_SENDER_ENROLLMENT_HEADERS", {})
    field_mapping = getattr(settings, "EVENT_SENDER_ENROLLMENT_FIELD_MAPPING", None)

    _send_data(enrollment, url, headers, field_mapping)


def _send_data(data: Any, url: str, headers: Dict[str, str], field_mapping: Optional[Dict[str, str]]):
    """Unified function for sending data."""
    try:
        data = prepare_data(data, field_mapping)
        send_data_task.delay(data, url, headers)
    except Exception as e:  # noqa pylint: disable=broad-except
        # This plugin should never break an enrollment action.
        log.exception(e)
