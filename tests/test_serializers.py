from __future__ import annotations

from unittest.mock import patch

import pytest

from src.core.models import CVDocument, JobDescription
from src.core.serializers import CVUploadSerializer, JobDescriptionSerializer


@pytest.mark.django_db
def test_cv_upload_serializer_create_delegates_to_ingestion_pipeline(
    temp_media_root,
    make_uploaded_file,
):
    with patch("src.core.serializers.CVIngestionPipeline") as mock_pipeline_cls:
        mock_pipeline = mock_pipeline_cls.return_value

        def _ingest_side_effect(document):
            document.raw_text = "Extracted CV text"
            document.metadata = {"candidate_name": "Mario Rossi", "contact": {"email": "mario@example.com"}}
            document.candidate_name = "Mario Rossi"
            document.email = "mario@example.com"
            document.save(
                update_fields=["raw_text", "metadata", "candidate_name", "email", "updated_at"]
            )
            return document

        mock_pipeline.ingest_cv_document.side_effect = _ingest_side_effect

        serializer = CVUploadSerializer(data={"source_file": make_uploaded_file()})
        assert serializer.is_valid(), serializer.errors
        document = serializer.save()

    stored = CVDocument.objects.get(id=document.id)
    assert stored.raw_text == "Extracted CV text"
    assert stored.candidate_name == "Mario Rossi"
    assert stored.email == "mario@example.com"
    mock_pipeline.ingest_cv_document.assert_called_once()


@pytest.mark.django_db
def test_job_description_serializer_create_delegates_to_ingestion_job():
    with patch("src.core.serializers.JobDescriptionIngestionJob") as mock_job_cls:
        mock_job = mock_job_cls.return_value
        mock_job.ingest_job_description.return_value = JobDescription(
            text="Backend engineer with Python",
            metadata={"split": {"skill": "python", "education": "", "experience": "3+ years"}},
            skill=[0.1] * 1536,
            education=[0.2] * 1536,
            experience=[0.3] * 1536,
        )
        serializer = JobDescriptionSerializer(data={"text": "Backend engineer with Python"})
        assert serializer.is_valid(), serializer.errors
        serializer.save()

    mock_job.ingest_job_description.assert_called_once_with("Backend engineer with Python")
