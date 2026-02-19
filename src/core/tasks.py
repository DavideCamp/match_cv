"""Celery tasks for asynchronous workflows."""

from __future__ import annotations

from celery import shared_task
from django.utils import timezone

from src.core.models import CVDocument, UploadBatch, UploadItem, UploadStatus
from src.core.services.ingestion import ingest_cv_document


def _refresh_batch_status(batch: UploadBatch) -> None:
    """Recompute aggregate batch counters/status from related UploadItem states."""
    total = batch.items.count()
    processed = batch.items.filter(status__in=[UploadStatus.SUCCESS, UploadStatus.FAILED]).count()
    failed = batch.items.filter(status=UploadStatus.FAILED).count()

    if processed == 0:
        status = UploadStatus.RUNNING
        completed_at = None
    elif failed == total:
        status = UploadStatus.FAILED
        completed_at = timezone.now()
    elif processed == total and failed == 0:
        status = UploadStatus.SUCCESS
        completed_at = timezone.now()
    elif processed == total and failed > 0:
        status = UploadStatus.PARTIAL
        completed_at = timezone.now()
    else:
        status = UploadStatus.RUNNING
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
    if item is None:
        if self.request.retries < max_retries:
            raise self.retry(
                exc=ValueError(f"UploadItem {upload_item_id} not found yet"),
                countdown=2 ** (self.request.retries + 1),
            )
        raise ValueError(f"UploadItem {upload_item_id} not found")
    if item.status == UploadStatus.SUCCESS:
        return str(item.id)

    item.status = UploadStatus.RUNNING
    item.started_at = timezone.now()
    item.error_message = ""
    item.save(update_fields=["status", "started_at", "error_message"])

    batch = item.batch
    if batch.started_at is None:
        batch.started_at = timezone.now()
        batch.status = UploadStatus.RUNNING
        batch.save(update_fields=["started_at", "status"])

    try:
        if not item.document_id:
            raise ValueError("Upload item has no associated CVDocument")

        document = CVDocument.objects.get(id=item.document_id)
        ingest_cv_document(document)

        item.status = UploadStatus.SUCCESS
        item.completed_at = timezone.now()
        item.save(update_fields=["status", "completed_at"])
    except Exception as exc:  # noqa: BLE001
        if self.request.retries < max_retries:
            item.status = UploadStatus.PENDING
            item.error_message = f"retrying ({self.request.retries + 1}/{max_retries}): {exc}"
            item.save(update_fields=["status", "error_message"])
            _refresh_batch_status(batch)
            raise self.retry(exc=exc, countdown=2 ** (self.request.retries + 1))

        item.status = UploadStatus.FAILED
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
