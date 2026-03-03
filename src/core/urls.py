"""Core API URL declarations."""

from django.urls import path
from rest_framework.routers import DefaultRouter

from src.core.views import (
    CVBulkUploadStatusView,
    CVBulkUploadView,
    CVUploadView,
    SearchRunCreateView,
    CvViewSet,
)

router = DefaultRouter()

urlpatterns = [
    path("cv/upload/", CVUploadView.as_view(), name="cv-upload"),
    path("cv/upload/bulk/", CVBulkUploadView.as_view(), name="cv-bulk-upload"),
    path(
        "cv/upload/bulk/<uuid:batch_id>/status/",
        CVBulkUploadStatusView.as_view(),
        name="cv-bulk-upload-status",
    ),
    path("search-runs/", SearchRunCreateView.as_view(), name="search-run-create"),
    path(
        "cv/",
        CvViewSet.as_view({"get": "list"}),
        name="cv-list",
    ),
]
