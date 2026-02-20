from __future__ import annotations

from unittest.mock import patch

import pytest

from src.core.models import CVDocument
from src.core.serializers import CVUploadSerializer


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
