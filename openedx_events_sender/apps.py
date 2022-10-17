"""
openedx_events_sender Django application initialization.
"""

from django.apps import AppConfig


class OpenedxEventsSenderConfig(AppConfig):
    """
    Configuration for the openedx_events_sender Django application.
    """

    name = "openedx_events_sender"

    plugin_app = {
        "signals_config": {
            "lms.djangoapp": {
                "relative_path": "receivers",
                "receivers": [
                    {
                        "receiver_func_name": "send_enrollment_data",
                        "signal_path": "openedx_events.learning.signals.COURSE_ENROLLMENT_CHANGED",
                    },
                ],
            }
        },
    }
