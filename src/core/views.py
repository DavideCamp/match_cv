"""API view stubs for document ingestion and search runs."""

import logging

from django.db import transaction
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from src.core.models import CVDocument, UploadBatch, UploadStatus
from src.core.retrieve.pipeline import CvScreenPipeline
from src.core.serializers import CVUploadSerializer
from src.core.tasks import ingest_upload_item_task

logger = logging.getLogger(__name__)


class CVUploadView(APIView):
    """Upload a CV document."""

    def post(self, request):
        """Validate and ingest a single uploaded CV synchronously."""
        serializer = CVUploadSerializer(data=request.data)
        if not serializer.is_valid():
            logger.warning("CV upload validation failed: %s", serializer.errors)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class SearchRunCreateView(APIView):
    """Create and execute a search run."""

    def post(self, request):
        """Run retrieval+scoring pipeline for a job offer and return ranked CVs."""
        job_offer_text = (request.data.get("job_offer_text") or "").strip()
        weights = request.data.get("weights", None)
        if not job_offer_text:
            return Response(
                {"error": "job_offer_text is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        try:
            top_k = int(request.data.get("top_k", 10))
        except (TypeError, ValueError):
            return Response({"error": "top_k must be integers"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            pipeline = CvScreenPipeline()
            res = pipeline.run(job_offer_text, weights, top_k)
            return Response(res, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CVBulkUploadView(APIView):
    """Upload multiple CV files and process ingestion asynchronously via Celery."""

    @transaction.atomic
    def post(self, request):
        """Create batch/items and enqueue one Celery ingestion task per file."""
        files = request.FILES.getlist("files")
        if not files:
            return Response(
                {"error": "files is required (multipart files[])"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        batch = UploadBatch.objects.create(
            status=UploadStatus.PENDING,
            total_files=len(files),
            processed_files=0,
            failed_files=0,
        )

        items_payload = []
        for file_obj in files:
            serializer = CVUploadSerializer(data={"source_file": file_obj})
            if not serializer.is_valid():
                logger.warning("Bulk upload validation failed for %s: %s", file_obj.name, serializer.errors)
                return Response(
                    {"error": "file validation failed", "filename": file_obj.name, "details": serializer.errors},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            document = CVDocument.objects.create(source_file=file_obj)
            item = batch.items.create(
                document=document,
                filename=file_obj.name,
                status=UploadStatus.PENDING,
            )
            ingest_upload_item_task.delay(str(item.id))
            items_payload.append(
                {
                    "upload_item_id": str(item.id),
                    "document_id": str(document.id),
                    "filename": item.filename,
                    "status": item.status,
                }
            )

        return Response(
            {
                "batch_id": str(batch.id),
                "status": batch.status,
                "total_files": batch.total_files,
                "items": items_payload,
            },
            status=status.HTTP_202_ACCEPTED,
        )


class CVBulkUploadStatusView(APIView):
    """Get async ingestion status for a bulk upload batch."""

    def get(self, request, batch_id):
        """Return batch-level and item-level ingestion progress for a bulk upload."""
        try:
            batch = UploadBatch.objects.get(id=batch_id)
        except UploadBatch.DoesNotExist:
            return Response({"error": "batch not found"}, status=status.HTTP_404_NOT_FOUND)

        items = batch.items.all().order_by("created_at")
        return Response(
            {
                "batch_id": str(batch.id),
                "status": batch.status,
                "total_files": batch.total_files,
                "processed_files": batch.processed_files,
                "failed_files": batch.failed_files,
                "started_at": batch.started_at,
                "completed_at": batch.completed_at,
                "items": [
                    {
                        "upload_item_id": str(item.id),
                        "document_id": str(item.document_id) if item.document_id else None,
                        "filename": item.filename,
                        "status": item.status,
                        "error_message": item.error_message,
                        "started_at": item.started_at,
                        "completed_at": item.completed_at,
                    }
                    for item in items
                ],
            },
            status=status.HTTP_200_OK,
        )
