"""
Tests for the signal receivers. It also implicitly tests utils and tasks.

If you want to test more receivers, add new entries to `pytestmark`.
"""

import datetime
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union
from unittest.mock import MagicMock, patch

import pytest
from opaque_keys.edx.keys import CourseKey
from openedx_events.learning.data import CourseData, CourseEnrollmentData, UserData, UserPersonalData
from openedx_events.learning.signals import COURSE_ENROLLMENT_CHANGED
from openedx_events.tooling import OpenEdxPublicSignal

from openedx_events_sender.receivers import send_enrollment_data


@dataclass
class EventTestData:
    """
    A structure shared by all tests in this module.
    """

    event: str
    event_data: Dict[str, Any]
    parsed_data: Dict[str, Optional[Union[int, str]]]
    field_mapping: Dict[str, str]
    filtered_data: Dict[str, Optional[Union[int, str]]]
    signal: OpenEdxPublicSignal
    receiver: Callable


TEST_URL = "https://localhost"
pytestmark = pytest.mark.parametrize(
    "data",
    [
        EventTestData(
            event="ENROLLMENT",
            event_data={
                "enrollment": CourseEnrollmentData(
                    user=UserData(
                        pii=UserPersonalData(
                            username="test",
                            email="test@example.com",
                            name="Test Example",
                        ),
                        id=42,
                        is_active=True,
                    ),
                    course=CourseData(
                        course_key=CourseKey.from_string("course-v1:edX+DemoX+Demo_Course"),
                        display_name="Demonstration Course",
                        start=datetime.datetime(2022, 9, 30, 0, 0, 0),
                    ),
                    mode="audit",
                    is_active=True,
                    creation_date=datetime.datetime(2022, 9, 30, 12, 34, 56),
                ),
            },
            parsed_data={
                "user_id": 42,
                "user_is_active": True,
                "user_pii_username": "test",
                "user_pii_email": "test@example.com",
                "user_pii_name": "Test Example",
                "course_course_key": "course-v1:edX+DemoX+Demo_Course",
                "course_display_name": "Demonstration Course",
                "course_start": "2022-09-30 00:00:00",
                "course_end": None,
                "mode": "audit",
                "is_active": True,
                "creation_date": "2022-09-30 12:34:56",
                "created_by": None,
            },
            field_mapping={
                "user_pii_email": "email",
                "course_course_key": "course_id",
                "is_active": "is_active",
            },
            filtered_data={
                "email": "test@example.com",
                "course_id": "course-v1:edX+DemoX+Demo_Course",
                "is_active": True,
            },
            signal=COURSE_ENROLLMENT_CHANGED,
            receiver=send_enrollment_data,
        )
    ],
)


def _send_enrollment_event(data):
    """Connect receiver to the signal and send event from the signal with test-specific event data."""
    signal = data.signal
    signal.connect(data.receiver)
    signal.send_event(**data.event_data)


def _set_url(settings, data, url: str = TEST_URL):
    """Set event handler URL in Django settings."""
    setattr(settings, f"EVENT_SENDER_{data.event}_URL", url)


def _set_headers(settings, data, headers: Dict[str, str]):
    """Set event headers in Django settings."""
    setattr(settings, f"EVENT_SENDER_{data.event}_HEADERS", headers)


def _set_field_mapping(settings, data, field_mapping: Dict[str, str]):
    """Set event field mapping in Django settings."""
    setattr(settings, f"EVENT_SENDER_{data.event}_FIELD_MAPPING", field_mapping)


@patch("openedx_events_sender.receivers._send_data")
def test_send_data_without_url(send_data_mock: MagicMock, data: EventTestData):
    """Sending data is optional - we only send a request if a target URL is defined."""
    _send_enrollment_event(data)
    send_data_mock.assert_not_called()


@patch("openedx_events_sender.tasks.requests")
def test_send_data(request_mock: MagicMock, settings, data: EventTestData):
    """Receiver should send a request when a URL is defined."""

    _set_url(settings, data)
    _send_enrollment_event(data)

    request_mock.post.assert_called_once_with(TEST_URL, json=data.parsed_data, headers={}, timeout=30)


@patch("openedx_events_sender.tasks.requests")
def test_send_headers(request_mock: MagicMock, settings, data: EventTestData):
    """Defined headers should be included in the request."""
    headers = {"Authorization": "JWT key"}
    _set_url(settings, data)
    _set_headers(settings, data, headers)
    _send_enrollment_event(data)

    request_mock.post.assert_called_once_with(TEST_URL, json=data.parsed_data, headers=headers, timeout=30)


@patch("openedx_events_sender.tasks.requests")
def test_send_without_data(request_mock: MagicMock, settings, data: EventTestData):
    """Defining an empty field mapping should result in sending a request with empty data."""
    _set_url(settings, data)
    _set_field_mapping(settings, data, {})
    _send_enrollment_event(data)

    request_mock.post.assert_called_once_with(TEST_URL, json={}, headers={}, timeout=30)


@patch("openedx_events_sender.tasks.requests")
def test_send_filtered_data(request_mock: MagicMock, settings, data: EventTestData):
    """Defining a custom field mapping should result in sending only the defined fields."""
    _set_url(settings, data)
    _set_field_mapping(settings, data, data.field_mapping)
    _send_enrollment_event(data)

    request_mock.post.assert_called_once_with(TEST_URL, json=data.filtered_data, headers={}, timeout=30)


@patch("openedx_events_sender.receivers.prepare_data", side_effect=Exception("Test exception."))
def test_handle_exception(_prepare_data_mock: MagicMock, settings, caplog, data: EventTestData):
    """The signal receiver should never raise an exception. It should log it, though."""
    _set_url(settings, data, "invalid_url")
    _send_enrollment_event(data)

    assert "Test exception" in caplog.text


def test_raise_exception_in_celery_when_sending_data(settings, caplog, data: EventTestData):
    """Celery task can raise an exception, as it's handled by a different process."""
    _set_url(settings, data, "invalid_url")
    _send_enrollment_event(data)

    assert 'celery.app.trace' in caplog.text
