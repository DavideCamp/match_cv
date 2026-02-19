"""Core API URL declarations."""

from django.urls import path

from src.core.views import (
    CVUploadView,
    SearchRunCreateView,
)

urlpatterns = [
    path("cv-documents/", CVUploadView.as_view(), name="cv-upload"),
    path("search-runs/", SearchRunCreateView.as_view(), name="search-run-create"),
]
