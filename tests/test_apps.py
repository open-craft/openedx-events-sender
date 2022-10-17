"""Test Django application initialization and its configuration."""

from typing import Iterable

from django.apps import apps


def test_app():
    """
    Check that the app is detected by Django.
    Verify the Open edX plugin config structure.
    """
    app = apps.get_app_config("openedx_events_sender")

    assert app.name == "openedx_events_sender"
    assert isinstance(app.plugin_app["signals_config"]["lms.djangoapp"]["receivers"], Iterable)
