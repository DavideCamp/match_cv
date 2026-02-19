"""Celery tasks for asynchronous workflows."""

from __future__ import annotations

from celery import shared_task
from django.utils import timezone

from src.core.db import PgVectorStore
from src.core.inject.inject import InjectDocument
from src.core.models import CVDocument, UploadBatch, UploadItem, UploadStatus


def _refresh_batch_status(batch: UploadBatch) -> None:
    """Recompute aggregate batch counters/status from related UploadItem states."""
    total = batch.items.count()
    processed = batch.items.filter(
        status__in=[UploadStatus.SUCCESS, UploadStatus.FAILED]
    ).count()
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


@shared_task(name="core.ingest_upload_item_task")
def ingest_upload_item_task(upload_item_id: str) -> str:
    """Ingest one UploadItem end-to-end and update both item and batch status."""
    item = UploadItem.objects.select_related("batch", "document").get(id=upload_item_id)
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
        inject_document = InjectDocument().from_yaml("src/core/inject/injestion_pipeline.yaml")
        vector_store = PgVectorStore()

        extracted = inject_document.extract_metadata(document.source_file.path)
        embed_doc = inject_document.run(
            extracted.get("text", ""), metadata=extracted.get("metadata", {})
        )

        document.ingested_at = timezone.now()
        document.raw_text = extracted.get("text", "") or ""
        document.metadata = extracted.get("metadata", {}) or {}
        document.candidate_name = document.metadata.get("candidate_name", "") or ""
        document.email = (
            document.metadata.get("contact", {}).get("email", "") or ""
            if isinstance(document.metadata, dict)
            else ""
        )
        document.save(
            update_fields=[
                "ingested_at",
                "updated_at",
                "raw_text",
                "metadata",
                "email",
                "candidate_name",
            ]
        )

        for chunk in embed_doc:
            chunk.metadata["document_id"] = str(document.id)
        vector_store.add(embed_doc)

        item.status = UploadStatus.SUCCESS
        item.completed_at = timezone.now()
        item.save(update_fields=["status", "completed_at"])
    except Exception as exc:  # noqa: BLE001
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