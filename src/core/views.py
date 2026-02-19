"""API view stubs for document ingestion and search runs."""

import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from src.core.models import UploadBatch
from src.core.retrieve.pipeline import CvScreenPipeline
from src.core.serializers import (
    CVBulkUploadCreateSerializer,
    CVUploadSerializer,
    SearchRunRequestSerializer,
)

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
        serializer = SearchRunRequestSerializer(data=request.data)
        if not serializer.is_valid():
            if "job_offer_text" in serializer.errors:
                return Response(
                    {"error": "job_offer_text is required"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if "top_k" in serializer.errors:
                return Response(
                    {"error": "top_k must be integers"}, status=status.HTTP_400_BAD_REQUEST
                )
            return Response({"error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)

        try:
            payload = serializer.validated_data
            pipeline = CvScreenPipeline()
            results = pipeline.run(
                payload["job_offer_text"],
                payload["weights"],
                payload["top_k"],
            )
            return Response(results, status=status.HTTP_200_OK)
        except Exception:
            logger.exception("search pipeline execution failed")
            return Response(
                {"error": "internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class CVBulkUploadView(APIView):
    """Upload multiple CV files and process ingestion asynchronously via Celery."""

    def post(self, request):
        """Validate files and delegate bulk creation to serializer."""
        files = request.FILES.getlist("files")
        if not files:
            return Response(
                {"error": "files is required (multipart files[])"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        serializer = CVBulkUploadCreateSerializer(data={"files": files})
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        payload = serializer.save()
        return Response(payload, status=status.HTTP_202_ACCEPTED)


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
