from django.db import transaction
from django.utils import timezone
from rest_framework import serializers

from src.core.db import PgVectorStore
from src.core.inject.inject import InjectDocument
from src.core.models import CVDocument


class CVUploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = CVDocument
        fields = "__all__"
        read_only_fields = (
            "id",
            "source_checksum",
            "ingested_at",
            "created_at",
            "updated_at",
        )

    @transaction.atomic
    def create(self, validated_data):
        vector_store = PgVectorStore()
        inject_document = InjectDocument().from_yaml("src/core/inject/injestion_pipeline.yaml")
        document = CVDocument.objects.create(**validated_data)
        extracted = inject_document.extract_metadata(document.source_file.path)
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

        for c in embed_doc:
            c.metadata["document_id"] = str(document.id)

        vector_store.add(embed_doc)
        return document
