"""Shared ingestion services for CV documents."""

from __future__ import annotations

from typing import Any

from django.utils import timezone

from src.core.db import PgVectorStore
from src.core.inject.inject import InjectDocument
from src.core.models import CVDocument


def ingest_cv_document(
    document: CVDocument,
    *,
    inject_document: InjectDocument | None = None,
    vector_store: PgVectorStore | None = None,
) -> CVDocument:
    """Extract metadata, embed chunks, persist vectors, and update CVDocument fields."""
    inject_document = inject_document or InjectDocument().from_yaml(
        "src/core/inject/injestion_pipeline.yaml"
    )
    vector_store = vector_store or PgVectorStore()

    extracted: dict[str, Any] = inject_document.extract_metadata(document.source_file.path)
    embed_doc = inject_document.run(
        extracted.get("text", ""), metadata=extracted.get("metadata", {})
    )

    document.ingested_at = timezone.now()
    document.raw_text = extracted.get("text", "") or ""
    document.metadata = extracted.get("metadata", {}) or {}
    document.candidate_name = document.metadata.get("candidate_name", "") or ""
    document.email = (
        document.metadata.get("contact", {}).get("email", "") or ""
        if isinstance(document.metadata, dict)
        else ""
    )
    document.save(
        update_fields=[
            "ingested_at",
            "updated_at",
            "raw_text",
            "metadata",
            "email",
            "candidate_name",
        ]
    )

    for chunk in embed_doc:
        chunk.metadata["document_id"] = str(document.id)
    vector_store.add(embed_doc)

    return document
