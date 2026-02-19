"""Celery tasks for asynchronous workflows."""

from __future__ import annotations

from celery import shared_task


@shared_task(name="core.ping")
def ping() -> str:
    """Simple health task to verify worker/broker wiring."""

    return "pong"


@shared_task(name="core.run_search_pipeline_task")
def run_search_pipeline_task(run_id: str, top_k: int = 20) -> str:
    """Placeholder async task for search orchestration."""

    # Placeholder task name/shape aligned with dev branch.
    # The actual pipeline execution will be added with the service layer.
    _ = top_k
    return run_id
