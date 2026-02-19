"""Celery app bootstrap for Django."""

from __future__ import annotations

import os

from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "src.config.settings")

app = Celery("match_cv")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
