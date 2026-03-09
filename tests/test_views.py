from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from django.urls import reverse
from rest_framework import status

from src.core.models import CVDocument, JobStatus
from src.core.tasks import DEFAULT_STEPS

@pytest.mark.django_db
@patch("src.core.views.CvSerializer")
def test_cv_upload_view_success(mock_serializer_cls, api_client, make_uploaded_file):
    mock_serializer = MagicMock()
    mock_serializer.data = {"id": "doc-id"}
    mock_serializer.is_valid.return_value = True
    mock_serializer_cls.return_value = mock_serializer

    response = api_client.post(
        reverse("cv-upload"),
        {"source_file": make_uploaded_file()},
        format="multipart",
    )

    assert response.status_code == status.HTTP_201_CREATED
    mock_serializer.is_valid.assert_called_once_with()
    mock_serializer.save.assert_called_once()

@pytest.mark.django_db
@pytest.mark.parametrize(
    ("payload", "error_key", "expected_error"),
    [
        ({}, "error", "job_offer_text or job_description_id is required"),
        (
            {"job_offer_text": "   "},
            "job_offer_text",
            "job_offer_text is required when job_description_id is not provided",
        ),
        ({"job_offer_text": "Backend Engineer", "top_k": "x"}, "top_k", "top_k must be integers"),
    ],
)
def test_search_run_create_view_bad_request(payload, error_key, expected_error, api_client):
    response = api_client.post(reverse("search-run-create"), payload, format="json")

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()[error_key][0] == expected_error

@pytest.mark.django_db
@patch("src.core.views.search_run_task.delay")
@patch("src.core.views.SearchRun.objects.create")
def test_search_run_create_view_success(mock_create, mock_delay, api_client):
    run_id = uuid.uuid4()
    mock_create.return_value = MagicMock(
        id=run_id,
        status=JobStatus.PENDING,
        progress_steps=DEFAULT_STEPS,
    )

    payload = {
        "job_offer_text": "Backend engineer with Python",
        "weights": {"skill": 0.1, "experience": 0.7, "education": 0.2},
        "top_k": 5,
    }
    response = api_client.post(reverse("search-run-create"), payload, format="json")

    assert response.status_code == status.HTTP_202_ACCEPTED
    assert response.json()["run_id"] == str(run_id)
    assert response.json()["status"] == JobStatus.PENDING
    assert response.json()["steps"] == DEFAULT_STEPS

    mock_create.assert_called_once_with(
        status=JobStatus.PENDING,
        progress_steps=DEFAULT_STEPS,
        job_offer_text=payload["job_offer_text"],
        weights=payload["weights"],
        top_k=5,
        job_description_id=None,
    )
    mock_delay.assert_called_once_with(str(run_id))

@pytest.mark.django_db
@patch("src.core.views.search_run_task.delay")
@patch("src.core.views.SearchRun.objects.create")
def test_search_run_create_view_success_with_job_description_id(mock_create, mock_delay, api_client):
    run_id = uuid.uuid4()
    mock_create.return_value = MagicMock(
        id=run_id,
        status=JobStatus.PENDING,
        progress_steps=DEFAULT_STEPS,
    )

    payload = {"job_description_id": "36ec8f27-17b1-4fdd-b3f6-ac6ca42f4c17", "top_k": 5}
    response = api_client.post(reverse("search-run-create"), payload, format="json")

    assert response.status_code == status.HTTP_202_ACCEPTED
    assert response.json()["run_id"] == str(run_id)

    mock_create.assert_called_once_with(
        status=JobStatus.PENDING,
        progress_steps=DEFAULT_STEPS,
        job_offer_text="",
        weights={"skill": 0.4, "experience": 0.3, "education": 0.3},
        top_k=5,
        job_description_id=uuid.UUID(payload["job_description_id"]),
    )
    mock_delay.assert_called_once_with(str(run_id))

@pytest.mark.django_db
@patch("src.core.views.search_run_task.delay", side_effect=RuntimeError("queue unavailable"))
@patch("src.core.views.SearchRun.objects.create")
def test_search_run_create_view_task_enqueue_error(mock_create, _mock_delay, api_client):
    mock_create.return_value = MagicMock(
        id=uuid.uuid4(),
        status=JobStatus.PENDING,
        progress_steps=DEFAULT_STEPS,
    )

    with pytest.raises(RuntimeError, match="queue unavailable"):
        api_client.post(
            reverse("search-run-create"),
            {"job_offer_text": "Backend engineer"},
            format="json",
        )


@pytest.mark.django_db
def test_list_cv(api_client):
    cv1 = CVDocument.objects.create()
    cv2 = CVDocument.objects.create()
    url = reverse("cv-list")
    response = api_client.get(url, {"ids": [str(cv1.id), str(cv2.id)]})

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 2

@pytest.mark.django_db
@patch("src.core.views.JobDescriptionView.serializer_class")
def test_job_description_view_success(mock_serializer_cls, api_client):
    mock_serializer = MagicMock()
    mock_serializer.data = {"id": "job-id"}
    mock_serializer.is_valid.return_value = True
    mock_serializer_cls.return_value = mock_serializer

    response = api_client.post(
        reverse("job-description-create"),
        {"text": "Backend engineer with Python"},
        format="json",
    )

    assert response.status_code == status.HTTP_201_CREATED
    mock_serializer.is_valid.assert_called_once_with()
    mock_serializer.save.assert_called_once()
