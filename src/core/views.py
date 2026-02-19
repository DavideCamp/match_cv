"""API view stubs for document ingestion and search runs."""

import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from src.core.retrieve.pipeline import CvScreenPipeline
from src.core.serializers import CVUploadSerializer

logger = logging.getLogger(__name__)


class CVUploadView(APIView):
    """Upload a CV document."""

    def post(self, request):
        serializer = CVUploadSerializer(data=request.data)
        if not serializer.is_valid():
            logger.warning("CV upload validation failed: %s", serializer.errors)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class SearchRunCreateView(APIView):
    """Create and execute a search run."""

    def post(self, request):
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
