"""Domain models for CV ingestion and search runs."""

from __future__ import annotations

import uuid

from django.db import models
from pgvector.django import VectorField


class CVDocument(models.Model):
    """Uploaded CV/resume document with full extracted text."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    candidate_name = models.CharField(max_length=255, blank=True)
    email = models.EmailField(blank=True)
    source_file = models.FileField(upload_to="cv_uploads/")
    source_checksum = models.CharField(max_length=64, blank=True, db_index=True)
    raw_text = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    ingested_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.candidate_name} ({self.id})"

class Chunks(models.Model):
    """Persisted text chunks and embeddings for each CV document."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey(CVDocument, on_delete=models.CASCADE, related_name="chunks")
    chunk_index = models.PositiveIntegerField()
    text_chunk = models.TextField()
    embedding = VectorField(dimensions=1536)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["id"]
