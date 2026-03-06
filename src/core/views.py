"""API view stubs for document ingestion and search runs."""
import copy
import logging
import uuid

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet

from src.core.models import UploadBatch, CVDocument, SearchRun, JobStatus
from src.core.tasks import search_run_task, DEFAULT_STEPS
from src.core.serializers import (
    CVBulkUploadCreateSerializer,
    CvSerializer,
    SearchRunRequestSerializer,
)

logger = logging.getLogger(__name__)


class CVUploadView(APIView):
    """Upload a CV document."""

    def post(self, request):
        """Validate and ingest a single uploaded CV synchronously."""
        serializer = CvSerializer(data=request.data)
        if not serializer.is_valid():
            logger.warning("CV upload validation failed: %s", serializer.errors)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class SearchRunView(APIView):
    """Create and execute a search run."""

    def post(self, request):
        """Run retrieval+scoring pipeline for a job offer and return ranked CVs."""
        serializer = SearchRunRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data
        progress_steps = copy.deepcopy(DEFAULT_STEPS)
        search_run = SearchRun.objects.create(
            status=JobStatus.PENDING,
            progress_steps=progress_steps,
            job_offer_text=payload["job_offer_text"],
            weights=payload["weights"],
            top_k=payload["top_k"],
        )

        search_run_task.delay(str(search_run.id))

        return Response(
            {
                "run_id": str(search_run.id),
                "status": search_run.status,
                "steps": search_run.progress_steps,
            },
            status=status.HTTP_202_ACCEPTED,
        )

    def get(self, request, run_id: str):

        try:
            run = SearchRun.objects.get(id=run_id)
        except SearchRun.DoesNotExist:
            return Response(status=status.HTTP_404_NOT_FOUND)

        return Response({
            "run_id": str(run.id),
            "status": run.status,
            "steps": run.progress_steps,
            "results": run.results if run.status == JobStatus.SUCCESS else [],
            "error": run.error if run.status == JobStatus.FAILED else "",
        })


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


class CvViewSet(ModelViewSet):
    serializer_class = CvSerializer
    queryset = CVDocument.objects.all()

    def get_queryset(self):
        queryset = super().get_queryset()

        ids = self.request.GET.getlist("ids")
        if ids:
            try:
                ids = [str(uuid.UUID(x)) for x in ids]
                queryset = queryset.filter(id__in=ids)
            except ValueError:
                queryset = queryset.none()

        return queryset