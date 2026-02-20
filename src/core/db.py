from __future__ import annotations

import environ
from django.contrib.postgres.search import SearchQuery, SearchRank, SearchVector
from django.db import transaction
from django.db.models import TextField, Value
from django.db.models.functions import Cast, Coalesce
from datapizza.core.vectorstore import Vectorstore
from datapizza.type import Chunk as DpChunk
from pgvector.django import CosineDistance

from src.core.models import CVDocument, Chunk

env = environ.Env()

class PgVectorStore(Vectorstore):
    """Vectorstore implementation backed by PostgreSQL + pgvector."""

    DEFAULT_DIMENSIONS = env.int("EMBEDDING_DIM", 1536)

    def __init__(self, dimensions: int = DEFAULT_DIMENSIONS):
        super().__init__()
        self.dimensions = dimensions

    def _validate_embedding(self, vector: list[float]) -> None:
        if len(vector) != self.dimensions:
            raise ValueError(
                f"Invalid embedding dimension {len(vector)}, expected {self.dimensions}"
            )

    @transaction.atomic
    def add(self, chunk: DpChunk | list[DpChunk], collection_name: str | None = None):
        """
        Add chunks to the database.
        Collection_name is just a placeholder
        """
        chunks = chunk if isinstance(chunk, list) else [chunk]

        for i, c in enumerate(chunks):
            if not c.embeddings:
                continue
            vec = c.embeddings[0].vector
            self._validate_embedding(vec)
            Chunk.objects.update_or_create(
                id=c.id,
                defaults={
                    "document_id": c.metadata.get("document_id"),
                    "chunk_index": i,
                    "text_chunk": c.text,
                    "embedding": vec,
                    "metadata": c.metadata or {},
                },
            )

        return len(chunks)

    async def a_add(self, chunk: DpChunk | list[DpChunk], collection_name: str | None = None):
        # Async interface delegates to sync DB implementation.
        return self.add(chunk, collection_name)

    def update(
        self,
        collection_name: str,
        payload: dict,
        points: list[int],
        **kwargs,
    ):
        """Update existing chunk rows by ids."""
        for pid in points:
            Chunk.objects.filter(id=pid).update(**payload)
        return len(points)

    def remove(
        self,
        collection_name: str,
        ids: list[str],
        **kwargs,
    ):
        """Remove chunks by ids."""
        deleted, _ = Chunk.objects.filter(id__in=ids).delete()
        return deleted

    def search(
        self,
        collection_name: str,
        query_vector: list[float],
        k: int = 10,
        vector_name: str | None = None,
        **kwargs,
    ) -> list[DpChunk]:
        """Search chunks with cosine distance over pgvector embeddings."""
        self._validate_embedding(query_vector)

        qs = Chunk.objects.all()
        hits = (
            qs.annotate(distance=CosineDistance("embedding", query_vector))
            .order_by("distance")[:k]
            .values("id", "document_id", "chunk_index", "text_chunk", "metadata", "distance")
        )

        results: list[DpChunk] = []
        for r in hits:
            # pgvector returns distance; convert to similarity in [0, 1].
            sim = max(0.0, 1.0 - float(r["distance"]))
            metadata = r["metadata"] if "metadata" in r else {}
            metadata["similarity"] = sim
            metadata["document_id"] = str(r["document_id"])
            results.append(
                DpChunk(
                    id=str(r["id"]),
                    text=r["text_chunk"],
                    embeddings=[],
                    metadata=metadata,
                )
            )

        return results

    async def a_search(
        self,
        collection_name: str,
        query_vector: list[float],
        k: int = 10,
        vector_name: str | None = None,
        **kwargs,
    ) -> list[DpChunk]:
        # Async interface delegates to sync DB implementation.
        return self.search(collection_name, query_vector, k, vector_name, **kwargs)

    def retrieve(self, collection_name: str, ids: list[str], **kwargs) -> list[DpChunk]:
        """Retrieve chunks by ids."""
        dbrows = Chunk.objects.filter(id__in=ids).values("id", "text_chunk", "metadata")
        return [
            DpChunk(
                id=str(r["id"]), text=r["text_chunk"], embeddings=[], metadata=r["metadata"] or {}
            )
            for r in dbrows
        ]

    @staticmethod
    def search_metadata(query: str, category: str, k: int = 10) -> dict:
        """
        Full-text search over CVDocument metadata+text using PostgreSQL ts_rank.
        Returns an object compatible with retrieve pipeline shape:
        {"retriever": [{"metadata": {"document_id": "...", "similarity": rank}}, ...]}
        """
        q = (query or "").strip()
        if not q:
            return {"retriever": []}

        query_obj = SearchQuery(q, search_type="plain")
        text_expr = Coalesce("raw_text", Value(""), output_field=TextField())
        metadata_expr = Coalesce(Cast("metadata", TextField()), Value(""), output_field=TextField())

        if category == "skill":
            vector = SearchVector(text_expr, weight="A") + SearchVector(metadata_expr, weight="A")
        elif category == "experience":
            vector = SearchVector(text_expr, weight="A") + SearchVector(metadata_expr, weight="B")
        elif category == "education":
            vector = SearchVector(text_expr, weight="B") + SearchVector(metadata_expr, weight="A")
        else:
            vector = SearchVector(text_expr, weight="A") + SearchVector(metadata_expr, weight="A")

        rows = (
            CVDocument.objects.annotate(search=vector)
            .annotate(rank=SearchRank(vector, query_obj))
            .filter(rank__gt=0.0)
            .order_by("-rank")[: max(1, int(k))]
            .values("id", "rank")
        )

        retriever = [
            {"metadata": {"document_id": str(r["id"]), "similarity": float(r["rank"])}}
            for r in rows
        ]
        return {"retriever": retriever}
