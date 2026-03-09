"""Core API URL declarations."""

from django.urls import path

from src.core.views import (
    CVBulkUploadStatusView,
    CVBulkUploadView,
    CVUploadView,
    JobDescriptionView,
    SearchRunCreateView,
)

urlpatterns = [
    path("cv-documents/", CVUploadView.as_view(), name="cv-upload"),
    path("cv-documents/bulk/", CVBulkUploadView.as_view(), name="cv-bulk-upload"),
    path(
        "cv-documents/bulk/<uuid:batch_id>/status/",
        CVBulkUploadStatusView.as_view(),
        name="cv-bulk-upload-status",
    ),
    path("job-descriptions/", JobDescriptionView.as_view(), name="job-description-create"),
    path("search-runs/", SearchRunCreateView.as_view(), name="search-run-create"),
]
