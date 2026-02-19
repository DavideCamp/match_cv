from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import patch

import pytest

from src.core.models import CVDocument
from src.core.serializers import CVUploadSerializer


@dataclass
class FakeChunk:
    metadata: dict = field(default_factory=dict)


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("metadata", "expected_name", "expected_email"),
    [
        (
            {
                "candidate_name": "Mario Rossi",
                "contact": {"email": "mario.rossi@example.com"},
            },
            "Mario Rossi",
            "mario.rossi@example.com",
        ),
        ({}, "", ""),
    ],
)
def test_cv_upload_serializer_create_maps_extracted_metadata(
    metadata,
    expected_name,
    expected_email,
    temp_media_root,
    make_uploaded_file,
    build_mock_inject_document,
):
    chunks = [FakeChunk(), FakeChunk()]
    mock_inject = build_mock_inject_document(
        extracted_text="Extracted CV text",
        extracted_metadata=metadata,
        embedded_chunks=chunks,
    )

    with (
        patch("src.core.services.ingestion.InjectDocument", return_value=mock_inject),
        patch("src.core.services.ingestion.PgVectorStore") as mock_vector_store_cls,
    ):
        mock_vector_store = mock_vector_store_cls.return_value
        mock_vector_store.add.return_value = len(chunks)

        serializer = CVUploadSerializer(data={"source_file": make_uploaded_file()})
        assert serializer.is_valid(), serializer.errors
        document = serializer.save()

    stored = CVDocument.objects.get(id=document.id)
    assert stored.raw_text == "Extracted CV text"
    assert stored.metadata == metadata
    assert stored.candidate_name == expected_name
    assert stored.email == expected_email
    assert stored.ingested_at is not None

    assert all(chunk.metadata.get("document_id") == str(document.id) for chunk in chunks)
    mock_inject.from_yaml.assert_called_once_with("src/core/inject/injestion_pipeline.yaml")
    mock_inject.extract_metadata.assert_called_once()
    mock_inject.run.assert_called_once()
    mock_vector_store.add.assert_called_once_with(chunks)
