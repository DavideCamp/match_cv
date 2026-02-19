from django.contrib import admin

from src.core.models import CVDocument, Chunks


@admin.register(CVDocument)
class CVDocumentAdmin(admin.ModelAdmin):
    list_display = ("candidate_name", "email", "ingested_at", "created_at")
    list_filter = ("ingested_at", "created_at")
    search_fields = ("candidate_name", "email", "source_checksum")
    readonly_fields = ("id", "created_at", "updated_at", "ingested_at")

@admin.register(Chunks)
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
