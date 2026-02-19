from django.contrib import admin

from src.core.models import CVDocument, Chunk, UploadBatch, UploadItem


@admin.register(CVDocument)
class CVDocumentAdmin(admin.ModelAdmin):
    list_display = ("candidate_name", "email", "ingested_at", "created_at")
    list_filter = ("ingested_at", "created_at")
    search_fields = ("candidate_name", "email", "source_checksum")
    readonly_fields = ("id", "created_at", "updated_at", "ingested_at")


@admin.register(Chunk)
class ChunksAdmin(admin.ModelAdmin):
    list_display = ("id", "document", "chunk_index", "embedding_preview", "created_at")
    list_filter = ("created_at",)
    search_fields = ("id", "document__id")
    readonly_fields = ("id", "created_at", "updated_at")
    raw_id_fields = ("document",)

    @admin.display(description="Embedding")
    def embedding_preview(self, obj):
        value = getattr(obj, "embedding", None)
        if value is None:
            return "-"
        return f"vector[{len(value)}]"


@admin.register(UploadBatch)
class UploadBatchAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "total_files",
        "processed_files",
        "started_at",
        "created_at",
        "completed_at",
    )
    list_filter = ("created_at",)


@admin.register(UploadItem)
class UploadItemAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "batch_id",
        "document_id",
        "started_at",
        "created_at",
        "completed_at",
        "status",
        "error_message",
    )
    list_filter = ("created_at",)
