from __future__ import annotations
from django.db import transaction
from pgvector.django import CosineDistance

from datapizza.core.vectorstore import Vectorstore
from datapizza.type import Chunk

from src.core.models import Chunks


class PgVectorStore(Vectorstore):
    """
    Esteso il vectorStore per utilizzare PGVector
    """

    DEFAULT_DIMENSIONS = 1536

    def __init__(self, dimensions: int = DEFAULT_DIMENSIONS):
        super().__init__()
        self.dimensions = dimensions

    def _validate_embedding(self, vector: list[float]) -> None:
        if len(vector) != self.dimensions:
            raise ValueError(
                f"Invalid embedding dimension {len(vector)}, expected {self.dimensions}"
            )

    @transaction.atomic
    def add(self, chunk: Chunk | list[Chunk], collection_name: str | None = None):
        """
        Aggiunge uno o più chunk nella tabella Chunks.
        collection_name non è usato qui ma presente nell'interfaccia.
        """
        chunks = chunk if isinstance(chunk, list) else [chunk]

        for i, c in enumerate(chunks):
            if not c.embeddings:
                continue
            vec = c.embeddings[0].vector
            self._validate_embedding(vec)
            Chunks.objects.update_or_create(
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

    async def a_add(self, chunk: Chunk | list[Chunk], collection_name: str | None = None):
        # asincrono ma usa DB sync quindi chiama sync, solo un override
        return self.add(chunk, collection_name)

    def update(
        self,
        collection_name: str,
        payload: dict,
        points: list[int],
        **kwargs,
    ):
        """
        Aggiorna chunks esistenti.
        payload può includere nuovi campi da aggiornare (testo / metadati).
        points è la lista di chunk id da aggiornare.
        """
        for pid in points:
            Chunks.objects.filter(id=pid).update(**payload)
        return len(points)

    def remove(
        self,
        collection_name: str,
        ids: list[str],
        **kwargs,
    ):
        """
        Rimuove i chunk con id nella lista.
        """
        deleted, _ = Chunks.objects.filter(id__in=ids).delete()
        return deleted

    def search(
        self,
        collection_name: str,
        query_vector: list[float],
        k: int = 10,
        vector_name: str | None = None,
        **kwargs,
    ) -> list[Chunk]:
        """
        cosine distance
        su embedding pgvector.
        """
        self._validate_embedding(query_vector)

        qs = Chunks.objects.all()
        hits = (
            qs.annotate(distance=CosineDistance("embedding", query_vector))
            .order_by("distance")[:k]
            .values("id", "document_id", "chunk_index", "text_chunk", "metadata", "distance")
        )

        results: list[Chunk] = []
        for r in hits:
            sim = max(0.0, 1.0 - float(r["distance"]))
            metadata = r["metadata"] if "metadata" in r else {}
            # metadata["similarity"] = sim
            metadata["similarity"] = sim
            metadata["document_id"] = str(r["document_id"])
            results.append(
                Chunk(
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
    ) -> list[Chunk]:
        # asincrono: delega alla sync per pgVector
        return self.search(collection_name, query_vector, k, vector_name, **kwargs)

    def retrieve(self, collection_name: str, ids: list[str], **kwargs) -> list[Chunk]:
        """
        Recupera chunk per ID.
        """
        dbrows = Chunks.objects.filter(id__in=ids).values("id", "text_chunk", "metadata")
        return [
            Chunk(
                id=str(r["id"]), text=r["text_chunk"], embeddings=[], metadata=r["metadata"] or {}
            )
            for r in dbrows
        ]
