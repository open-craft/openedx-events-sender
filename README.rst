openedx-events-sender
#####################

|pypi-badge| |ci-badge| |codecov-badge| |doc-badge| |pyversions-badge|
|license-badge| |status-badge|

Purpose
*******

Send Open edX events to selected URLs.

Getting Started
***************

Developing
==========

One Time Setup
--------------
.. code-block::

  # Clone the repository
  git clone git@github.com:open-craft/openedx-events-sender.git
  cd openedx-events-sender

  # Set up a virtualenv using virtualenvwrapper with the same name as the repo and activate it
  mkvirtualenv -p python3.8 openedx-events-sender


Every time you develop something in this repo
---------------------------------------------
.. code-block::

  # Activate the virtualenv
  workon openedx-events-sender

  # Grab the latest code
  git checkout main
  git pull

  # Install/update the dev requirements
  make requirements

  # Run the tests and quality checks (to verify the status before you make any changes)
  make validate

  # Make a new branch for your changes
  git checkout -b <your_github_username>/<short_description>

  # Using your favorite editor, edit the code to make your change.
  vim ...

  # Run your new tests
  pytest ./path/to/new/tests

  # Run all the tests and quality checks
  make validate

  # Commit all your changes
  git commit ...
  git push

  # Open a PR and ask for review.

Deploying
=========

Native Installation
-------------------

To deploy this to an Open edX instance, include it in the ``EDXAPP_PRIVATE_REQUIREMENTS`` or ``EDXAPP_EXTRA_REQUIREMENTS`` variables.

Include the require configuration settings in ``EDXAPP_LMS_ENV_EXTRA`` variable.



Tutor Installation
------------------

To `install`_ this in the Open edX build, include it in the ``config.yml`` file using the ``OPENEDX_EXTRA_PIP_REQUIREMENTS`` variable.

You need to rebuild the Open edX image::

    tutor images build openedx


Add the required configuration settings using a `yaml tutor plugin`_:

.. code-block:: yaml

  name: events_sender_plugin
  version: 0.1.0
  patches:
    lms-env: |
        EVENT_SENDER_ENROLLMENT_URL: "https://webhook.site/xyz"
        EVENT_SENDER_ENROLLMENT_HEADERS: {"Authorization": "TEST DATA"}
        EVENT_SENDER_ENROLLMENT_FIELD_MAPPING: {
            "user_pii_email": "email",
            "course_course_key": "course_id",
            "is_active": "is_active"
        }


.. _install: https://docs.tutor.overhang.io/configuration.html?highlight=xblock#installing-extra-xblocks-and-requirements
.. _yaml tutor plugin: https://docs.tutor.overhang.io/plugins/v0/gettingstarted.html#yaml-file


Documentation
*************

Events
======

COURSE_ENROLLMENT_CHANGED
-------------------------

Example payload
^^^^^^^^^^^^^^^

.. code-block:: json

   {
    "event": "org.openedx.learning.course.enrollment.changed.v1",
    "user_id": 42,
    "user_is_active": true,
    "user_pii_username": "test",
    "user_pii_email": "test@example.com",
    "user_pii_name": "Test Name",
    "course_course_key": "course-v1:edX+DemoX+Demo_Course",
    "course_display_name": "Demonstration Course",
    "course_start": "2022-09-30 00:00:00",
    "course_end": null,
    "mode": "audit",
    "is_active": true,
    "creation_date": "2022-09-30 12:34:56",
    "created_by": null
   }

Configuration
^^^^^^^^^^^^^

To send this event, you need to set ``EVENT_SENDER_ENROLLMENT_URL`` in your settings.

You can pass custom headers by setting ``EVENT_SENDER_ENROLLMENT_HEADERS``.

You can define custom field mapping with ``EVENT_SENDER_ENROLLMENT_FIELD_MAPPING``.
E.g. If you would like to send ``email`` instead of ``user_pii_email``, set this to the following value:

.. code-block:: json

   {
    "user_pii_email": "email"
   }

You can also define event-specific metadata by prefixing variables from
`EventMetadata`_ with ``metadata_``. E.g. to get the event's name, you should
specify ``metadata_event_type`` as a key in the mapping.

**Note**: if you want to use custom mapping, you need to define **all** values that will be sent. If you define an empty field mapping, then an empty dict will be sent in the request.

.. _EventMetadata: https://openedx-events.readthedocs.io/en/latest/openedx_events.html#openedx_events.data.EventsMetadata

Getting Help
============

If you're having trouble, we have discussion forums at
https://discuss.openedx.org where you can connect with others in the
community.

Our real-time conversations are on Slack. You can request a `Slack
invitation`_, then join our `community Slack workspace`_.

For anything non-trivial, the best path is to open an issue in this
repository with as many details about the issue you are facing as you
can provide.

https://github.com/open-craft/openedx-events-sender/issues

For more information about these options, see the `Getting Help`_ page.

.. _Slack invitation: https://openedx.org/slack
.. _community Slack workspace: https://openedx.slack.com/
.. _Getting Help: https://openedx.org/getting-help

License
*******

The code in this repository is licensed under the AGPL 3.0 unless
otherwise noted.

Please see `LICENSE.txt <LICENSE.txt>`_ for details.

Contributing
************

Contributions are very welcome.
Please read `How To Contribute <https://openedx.org/r/how-to-contribute>`_ for details.

This project is currently accepting all types of contributions, bug fixes,
security fixes, maintenance work, or new features.  However, please make sure
to have a discussion about your new feature idea with the maintainers prior to
beginning development to maximize the chances of your change being accepted.
You can start a conversation by creating a new issue on this repo summarizing
your idea.

The Open edX Code of Conduct
****************************

All community members are expected to follow the `Open edX Code of Conduct`_.

.. _Open edX Code of Conduct: https://openedx.org/code-of-conduct/

People
******

The assigned maintainers for this component and other project details may be
found in `Backstage`_. Backstage pulls this data from the ``catalog-info.yaml``
file in this repo.

.. _Backstage: https://open-edx-backstage.herokuapp.com/catalog/default/component/openedx-events-sender

Reporting Security Issues
*************************

Please do not report security issues in public. Please email security@tcril.org.

.. |pypi-badge| image:: https://img.shields.io/pypi/v/openedx-events-sender.svg
    :target: https://pypi.python.org/pypi/openedx-events-sender/
    :alt: PyPI

.. |ci-badge| image:: https://github.com/open-craft/openedx-events-sender/workflows/Python%20CI/badge.svg?branch=main
    :target: https://github.com/open-craft/openedx-events-sender/actions
    :alt: CI

.. |codecov-badge| image:: https://codecov.io/github/open-craft/openedx-events-sender/coverage.svg?branch=main
    :target: https://codecov.io/github/open-craft/openedx-events-sender?branch=main
    :alt: Codecov

.. |doc-badge| image:: https://readthedocs.org/projects/openedx-events-sender/badge/?version=latest
    :target: https://openedx-events-sender.readthedocs.io/en/latest/
    :alt: Documentation

.. |pyversions-badge| image:: https://img.shields.io/pypi/pyversions/openedx-events-sender.svg
    :target: https://pypi.python.org/pypi/openedx-events-sender/
    :alt: Supported Python versions

.. |license-badge| image:: https://img.shields.io/github/license/open-craft/openedx-events-sender.svg
    :target: https://github.com/open-craft/openedx-events-sender/blob/main/LICENSE.txt
    :alt: License

.. |status-badge| image:: https://img.shields.io/badge/Status-Maintained-brightgreen
