"""Celery tasks for asynchronous workflows."""

from __future__ import annotations

import copy
import traceback

from celery import shared_task
from django.utils import timezone

from src.core.retrieve.pipeline import CvScreenPipeline
from src.core.inject.injection import CVIngestionPipeline
from src.core.models import CVDocument, UploadBatch, UploadItem, JobStatus, SearchRun


def _refresh_batch_status(batch: UploadBatch) -> None:
    """Recompute aggregate batch counters/status from related UploadItem states."""
    total = batch.items.count()
    processed = batch.items.filter(status__in=[JobStatus.SUCCESS, JobStatus.FAILED]).count()
    failed = batch.items.filter(status=JobStatus.FAILED).count()

    if processed == 0:
        status = JobStatus.RUNNING
        completed_at = None
    elif failed == total:
        status = JobStatus.FAILED
        completed_at = timezone.now()
    elif processed == total and failed == 0:
        status = JobStatus.SUCCESS
        completed_at = timezone.now()
    elif processed == total and failed > 0:
        status = JobStatus.PARTIAL
        completed_at = timezone.now()
    else:
        status = JobStatus.RUNNING
        completed_at = None

    batch.total_files = total
    batch.processed_files = processed
    batch.failed_files = failed
    batch.status = status
    batch.completed_at = completed_at
    if batch.started_at is None:
        batch.started_at = timezone.now()
    batch.save(
        update_fields=[
            "total_files",
            "processed_files",
            "failed_files",
            "status",
            "started_at",
            "completed_at",
        ]
    )


@shared_task(bind=True, name="core.ingest_upload_item_task")
def ingest_upload_item_task(self, upload_item_id: str) -> str:
    """Ingest one UploadItem end-to-end and update both item and batch status."""
    max_retries = 3
    item = UploadItem.objects.select_related("batch", "document").filter(id=upload_item_id).first()
    ingestion_pipeline = CVIngestionPipeline()
    if item is None:
        if self.request.retries < max_retries:
            raise self.retry(
                exc=ValueError(f"UploadItem {upload_item_id} not found yet"),
                countdown=2 ** (self.request.retries + 1),
            )
        raise ValueError(f"UploadItem {upload_item_id} not found")
    if item.status == JobStatus.SUCCESS:
        return str(item.id)

    item.status = JobStatus.RUNNING
    item.started_at = timezone.now()
    item.error_message = ""
    item.save(update_fields=["status", "started_at", "error_message"])

    batch = item.batch
    if batch.started_at is None:
        batch.started_at = timezone.now()
        batch.status = JobStatus.RUNNING
        batch.save(update_fields=["started_at", "status"])

    try:
        if not item.document_id:
            raise ValueError("Upload item has no associated CVDocument")

        document = CVDocument.objects.get(id=item.document_id)
        ingestion_pipeline.ingest_cv_document(document)

        item.status = JobStatus.SUCCESS
        item.completed_at = timezone.now()
        item.save(update_fields=["status", "completed_at"])
    except Exception as exc:  # noqa: BLE001
        if self.request.retries < max_retries:
            item.status = JobStatus.PENDING
            item.error_message = f"retrying ({self.request.retries + 1}/{max_retries}): {exc}"
            item.save(update_fields=["status", "error_message"])
            _refresh_batch_status(batch)
            raise self.retry(exc=exc, countdown=2 ** (self.request.retries + 1))

        item.status = JobStatus.FAILED
        item.error_message = str(exc)
        item.completed_at = timezone.now()
        item.save(update_fields=["status", "error_message", "completed_at"])
    finally:
        _refresh_batch_status(batch)

    return str(item.id)


@shared_task(name="core.ping")
def ping() -> str:
    """Simple health task to verify worker/broker wiring."""

    return "pong"




DEFAULT_STEPS = [
    {"action": "split", "title": "Analizzo la job description", "finished": False, "description": "", "finished_at": None},
    {"action": "semantic", "title": "Ricerca semantica", "finished": False, "description": "", "finished_at": None},
    {"action": "metadata", "title": "Scoring metadata", "finished": False, "description": "", "finished_at": None},
    {"action": "merge", "title": "Unisco e normalizzo", "finished": False, "description": "", "finished_at": None},
    {"action": "scoring", "title": "Calcolo ranking finale", "finished": False, "description": "", "finished_at": None},
]

@shared_task(name="core.search_runs")
def search_run_task(run_id):
    run = SearchRun.objects.get(id=run_id)
    # run.status = JobStatus.PENDING
    #run.progress_steps = copy.deepcopy(DEFAULT_STEPS)
    run.save(update_fields=["status", "progress_steps", "updated_at"])

    def progress_step(action:str, finished: bool, description: str = ''):
        set_step_state(run_id, action=action, finished=finished, description=description)
    try:
        pipeline = CvScreenPipeline(progress_step)
        res = pipeline.run(run.job_offer_text, run.weights, run.top_k)
        run.status = JobStatus.SUCCESS
        run.results = res
        run.save(update_fields=["status", "results", "updated_at"])
    except Exception as exc:
        run.status = JobStatus.FAILED
        run.error = traceback.format_exc()
        run.save(update_fields=["status", "error", "updated_at"])


def set_step_state(run_id: str, action: str, finished: bool, description: str = ""):
    run = SearchRun.objects.get(id=run_id)
    steps = list(run.progress_steps or [])
    now = timezone.now().isoformat()

    for step in steps:
        if step.get("action") == action:
            step["finished"] = finished
            step["description"] = description
            step["finished_at"] = now if finished else None
            break

    run.progress_steps = steps
    run.save(update_fields=["progress_steps", "updated_at"])