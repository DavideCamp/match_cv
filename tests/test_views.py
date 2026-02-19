from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from django.urls import reverse
from rest_framework import status


@patch("src.core.views.CVUploadSerializer")
def test_cv_upload_view_success(mock_serializer_cls, api_client, make_uploaded_file):
    mock_serializer = MagicMock()
    mock_serializer.data = {"id": "doc-id"}
    mock_serializer_cls.return_value = mock_serializer

    response = api_client.post(
        reverse("cv-upload"),
        {"source_file": make_uploaded_file()},
        format="multipart",
    )

    assert response.status_code == status.HTTP_201_CREATED
    mock_serializer.is_valid.assert_called_once_with()
    mock_serializer.save.assert_called_once()


@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        ({}, "job_offer_text is required"),
        ({"job_offer_text": "   "}, "job_offer_text is required"),
        ({"job_offer_text": "Backend Engineer", "top_k": "x"}, "top_k must be integers"),
    ],
)
def test_search_run_create_view_bad_request(payload, expected_error, api_client):
    response = api_client.post(reverse("search-run-create"), payload, format="json")

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["error"] == expected_error


@patch("src.core.views.CvScreenPipeline")
def test_search_run_create_view_success(mock_pipeline_cls, api_client):
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = [{"candidate_name": "Mario Rossi", "score": 0.9}]
    mock_pipeline_cls.return_value = mock_pipeline

    payload = {
        "job_offer_text": "Backend engineer with Python",
        "weights": {"skill": 0.1, "experience": 0.7, "education": 0.2},
        "top_k": 5,
    }
    response = api_client.post(reverse("search-run-create"), payload, format="json")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == [{"candidate_name": "Mario Rossi", "score": 0.9}]
    mock_pipeline.run.assert_called_once_with(payload["job_offer_text"], payload["weights"], 5)


@patch("src.core.views.CvScreenPipeline")
def test_search_run_create_view_pipeline_error(mock_pipeline_cls, api_client):
    mock_pipeline = MagicMock()
    mock_pipeline.run.side_effect = RuntimeError("pipeline failed")
    mock_pipeline_cls.return_value = mock_pipeline

    response = api_client.post(
        reverse("search-run-create"),
        {"job_offer_text": "Backend engineer"},
        format="json",
    )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json()["error"] == "internal server error"
