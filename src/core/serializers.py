from django.db import transaction
from rest_framework import serializers

from src.core.inject.injection import CVIngestionPipeline
from src.core.models import CVDocument, UploadBatch, UploadStatus
from src.core.tasks import ingest_upload_item_task


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
        ingestion_pipeline = CVIngestionPipeline()
        document = CVDocument.objects.create(**validated_data)
        return ingestion_pipeline.ingest_cv_document(document)


class CVBulkUploadCreateSerializer(serializers.Serializer):
    """Create a bulk upload batch and enqueue one async ingestion task per file."""

    files = serializers.ListField(
        child=serializers.FileField(),
        allow_empty=False,
        write_only=True,
    )

    @staticmethod
    def validate_files(value):
        for file_obj in value:
            serializer = CVUploadSerializer(data={"source_file": file_obj})
            if not serializer.is_valid():
                raise serializers.ValidationError(
                    {
                        "error": "file validation failed",
                        "filename": file_obj.name,
                        "details": serializer.errors,
                    }
                )
        return value

    @transaction.atomic
    def create(self, validated_data):
        files = validated_data["files"]
        batch = UploadBatch.objects.create(
            status=UploadStatus.PENDING,
            total_files=len(files),
            processed_files=0,
            failed_files=0,
        )

        items_payload = []
        for file_obj in files:
            document = CVDocument.objects.create(source_file=file_obj)
            item = batch.items.create(
                document=document,
                filename=file_obj.name,
                status=UploadStatus.PENDING,
            )
            transaction.on_commit(
                lambda item_id=str(item.id): ingest_upload_item_task.delay(item_id)
            )
            items_payload.append(
                {
                    "upload_item_id": str(item.id),
                    "document_id": str(document.id),
                    "filename": item.filename,
                    "status": item.status,
                }
            )

        return {
            "batch_id": str(batch.id),
            "status": batch.status,
            "total_files": batch.total_files,
            "items": items_payload,
        }


class SearchRunRequestSerializer(serializers.Serializer):
    """Validate input payload for search endpoint."""

    job_offer_text = serializers.CharField(
        required=True,
        allow_blank=False,
        trim_whitespace=True,
        error_messages={
            "required": "job_offer_text is required",
            "blank": "job_offer_text is required",
        },
    )
    top_k = serializers.IntegerField(
        required=False,
        default=10,
        min_value=1,
        error_messages={"invalid": "top_k must be integers"},
    )
    weights = serializers.DictField(required=False)

    def validate_weights(self, value):
        required_keys = {"skill", "experience", "education"}
        if not isinstance(value, dict):
            raise serializers.ValidationError("weights must be an object")
        if set(value.keys()) != required_keys:
            raise serializers.ValidationError("weights must include skill, experience, education")

        normalized = {}
        for key in required_keys:
            try:
                normalized[key] = float(value[key])
            except (TypeError, ValueError):
                raise serializers.ValidationError(f"weight '{key}' must be numeric") from None
            if normalized[key] < 0:
                raise serializers.ValidationError(f"weight '{key}' must be >= 0")

        if abs(sum(normalized.values()) - 1.0) > 1e-6:
            raise serializers.ValidationError("weights must sum to 1.0")

        return normalized

    def validate(self, attrs):
        attrs.setdefault("weights", {"skill": 0.4, "experience": 0.3, "education": 0.3})
        return attrs
