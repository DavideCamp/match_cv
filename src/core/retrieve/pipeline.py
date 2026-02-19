import dataclasses
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Tuple

import environ
from datapizza.clients.openai import OpenAIClient
from pydantic import BaseModel, Field

from src.core.retrieve.rag import RagPipeline
from src.core.models import CVDocument

env = environ.Env()

API_KEY = env("OPENAI_API_KEY", default="")
EMBED_MODEL = env("EMBEDDING_MODEL_NAME", default="text-embedding-3-small")


@dataclasses.dataclass
class JobProposalSplit(BaseModel):
    skill: str = Field(default="")
    education: str = Field(default="")
    experience: str = Field(default="")


def calculate_score(occ: Tuple[str, dict], weights: dict[str, float]) -> dict[str, Any]:
    """for a cv, multiply per-category similarity by weights, then return cv with final score"""
    cv_id = occ[0]
    categories = occ[1]
    score = 0.0
    for category_name, similarity in categories.items():
        score += float(weights.get(category_name, 0.0)) * float(similarity)

    cv = CVDocument.objects.get(id=cv_id)
    return {
        "candidate_name": cv.candidate_name,
        "cv": cv.raw_text,
        "candidate_email": cv.email,
        "score": score,
    }


def find_occurrences(semantic_result: dict[str, Any]) -> dict[str, dict[str, float]]:
    """
    Aggregate by document_id across skill/education/experience.
    Output format:
    [
      {
        "<cvId>": {
          "skill": <distance or 0>,
          "education": <distance or 0>,
          "experience": <distance or 0>
        }
      }
    ]
    """
    categories = ("skill", "education", "experience")
    occ = {}

    def _category_doc_distances(category: str) -> dict[str, float]:
        payload = semantic_result.get(category, {}) if isinstance(semantic_result, dict) else {}
        chunks = payload.get("retriever", []) if isinstance(payload, dict) else []

        best_by_doc: dict[str, float] = {}
        for chunk in chunks:
            metadata = getattr(chunk, "metadata", None)
            if metadata is None and isinstance(chunk, dict):
                metadata = chunk.get("metadata", {})
            if not isinstance(metadata, dict):
                continue

            doc_id = metadata.get("document_id")
            if doc_id is None:
                continue
            doc_id = str(doc_id)

            similarity = metadata.get("similarity")
            prev = best_by_doc.get(doc_id)
            if prev is None or similarity > prev:
                best_by_doc[doc_id] = similarity

        return best_by_doc

    by_category = {category: _category_doc_distances(category) for category in categories}

    all_doc_ids = set()
    for category in categories:
        all_doc_ids.update(by_category[category].keys())

    for document_id in all_doc_ids:
        occ[document_id] = {
            "skill": by_category["skill"].get(document_id, 0.0),
            "education": by_category["education"].get(document_id, 0.0),
            "experience": by_category["experience"].get(document_id, 0.0),
        }
    return occ


def _run_category_search(query: str, k: int) -> dict[str, Any]:
    q = (query or "").strip()
    if not q:
        return {}
    rag_pipeline = RagPipeline()
    system_query = (
        "Retrieve CVs that satisfy ALL hard requirements in this job request. "
        "Do not relax constraints such as max years of experience. "
        f"Job request: {q}"
    )
    print(system_query)
    return rag_pipeline.run(
        {
            "rewriter": {"user_prompt": system_query},
            "prompt": {"user_prompt": system_query},
            "retriever": {"collection_name": "sample", "k": max(1, int(k))},
            "generator": {"input": system_query},
        }
    )


def semantic_search(job_details: JobProposalSplit, k: int = 25) -> dict[str, Any]:
    category_queries = {
        "skill": job_details.skill,
        "education": job_details.education,
        "experience": job_details.experience,
    }

    out: dict[str, Any] = {"skill": {}, "education": {}, "experience": {}}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {
            executor.submit(_run_category_search, query, k): category
            for category, query in category_queries.items()
        }
        print(as_completed(future_map))
        for future in as_completed(future_map):
            category = future_map[future]
            out[category] = future.result()

    return out


class CvScreenPipeline:
    def __init__(self):
        self.client = OpenAIClient(model="gpt-4o-mini", api_key=API_KEY)

    def split_job_description(self, job_offer_text: str) -> JobProposalSplit:
        resp = self.client.structured_response(
            input=job_offer_text,
            output_cls=JobProposalSplit,
            system_prompt=(
                "Extract search-focused fields from the job offer. "
                "Return short query strings: skill, education, experience. "
                "For skill, prefer concrete technical/domain terms (e.g. backend python fastapi), "
                "not only generic words like 'engineer'. "
                "Do not invent requirements not present in the input."
            ),
        )

        return resp.structured_data[0]  # TODO FIX THE LIBARY TYPE - MA FUNZIONA

    def run(self, job_description, weights: dict[str, float], top_k: int) -> list[dict[str, Any]]:
        job_details = self.split_job_description(job_description)
        semantic_result = semantic_search(job_details)
        occurrences = find_occurrences(semantic_result)
        final_res = []
        for occ in occurrences.items():
            final_res.append(calculate_score(occ, weights))

        return final_res
