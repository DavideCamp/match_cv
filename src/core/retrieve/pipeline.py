import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import StrEnum
from typing import Any

import environ
from datapizza.clients.openai import OpenAIClient
from pydantic import BaseModel, Field

from src.core.db import PgVectorStore
from src.core.models import CVDocument
from src.core.retrieve.rag import RagPipeline

env = environ.Env()
API_KEY = env("OPENAI_API_KEY", default="")


class JobProposalSplit(BaseModel):
    skill: str = Field(default="")
    education: str = Field(default="")
    experience: str = Field(default="")


class Category(StrEnum):
    SKILL = "skill"
    EDUCATION = "education"
    EXPERIENCE = "experience"


CATEGORIES = (Category.SKILL.value, Category.EDUCATION.value, Category.EXPERIENCE.value)


def _parse_experience_constraints(experience_query: str) -> tuple[float | None, float | None]:
    """Extract soft min/max years constraints from natural language."""
    q = (experience_query or "").lower()
    nums = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", q)]
    if not nums:
        return None, None

    if "less than" in q or "under" in q or "max" in q or "up to" in q or "no more than" in q:
        return None, nums[0]
    if "more than" in q or "at least" in q or "over" in q:
        return nums[0], None
    if "+" in q:
        return nums[0], None
    if len(nums) >= 2 and ("-" in q or " to " in q or "between" in q or "from" in q):
        low = min(nums[0], nums[1])
        high = max(nums[0], nums[1])
        return low, high
    if len(nums) == 1:
        return nums[0], None

    return None, None


def _fallback_seniority_range(experience_query: str) -> tuple[float | None, float | None]:
    """Use seniority words as weak constraints when years are missing."""
    q = (experience_query or "").lower()
    if "senior" in q:
        return 5.0, None
    if "mid" in q:
        return 2.0, 6.0
    if "junior" in q:
        return None, 3.0
    return None, None


def _score_years_against_constraints(
    years: float,
    min_years: float | None,
    max_years: float | None,
) -> float:
    """Return a soft [0,1] score for years experience against min/max constraints."""
    y = max(0.0, float(years))
    low = min_years
    high = max_years

    if low is None and high is None:
        return 0.0

    if low is not None and high is not None:
        if low <= y <= high:
            return 1.0
        if y < low:
            return max(0.0, y / low) if low > 0 else 1.0
        return max(0.0, high / y) if y > 0 else 1.0

    if low is not None:
        return min(1.0, y / low) if low > 0 else 1.0

    return 1.0 if y <= high else max(0.0, high / y) if high and y > 0 else 0.0


def compute_experience_metadata_score(job_details: JobProposalSplit) -> dict[str, float]:
    """Compute soft experience score from metadata.seniority.years_experience_estimate."""
    min_years, max_years = _parse_experience_constraints(job_details.experience)
    if min_years is None and max_years is None:
        min_years, max_years = _fallback_seniority_range(job_details.experience)
    if min_years is None and max_years is None:
        return {}

    out: dict[str, float] = {}
    for doc in CVDocument.objects.only("id", "metadata"):
        metadata = doc.metadata if isinstance(doc.metadata, dict) else {}
        seniority = metadata.get("seniority", {}) if isinstance(metadata, dict) else {}
        years = seniority.get("years_experience_estimate")
        try:
            years_f = float(years)
        except (TypeError, ValueError):
            years_f = 0.0
        out[str(doc.id)] = _score_years_against_constraints(years_f, min_years, max_years)
    return out


def apply_experience_metadata_boost(
    occ: dict[str, dict[str, float]],
    exp_meta_scores: dict[str, float],
) -> dict[str, dict[str, float]]:
    """Blend retrieval experience signal and metadata experience signal with equal weight."""
    if not exp_meta_scores:
        return occ

    out: dict[str, dict[str, float]] = {}
    for doc_id, values in occ.items():
        retrieval_exp = float(values.get(Category.EXPERIENCE.value, 0.0))
        metadata_exp = float(exp_meta_scores.get(doc_id, 0.0))
        out[doc_id] = dict(values)
        out[doc_id][Category.EXPERIENCE.value] = (retrieval_exp + metadata_exp) / 2.0
    return out


def normalize_occurrences(occ: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    """Min-max normalize each category to [0,1] across all candidates."""
    categories = CATEGORIES
    if not occ:
        return occ

    mins = {category: float("inf") for category in categories}
    maxs = {category: float("-inf") for category in categories}

    for values in occ.values():
        for category in categories:
            value = float(values.get(category, 0.0))
            mins[category] = min(mins[category], value)
            maxs[category] = max(maxs[category], value)

    normalized: dict[str, dict[str, float]] = {}
    for doc_id, values in occ.items():
        normalized[doc_id] = {}
        for category in categories:
            value = float(values.get(category, 0.0))
            low = mins[category]
            high = maxs[category]
            if high <= low:
                normalized[doc_id][category] = 0.0
            else:
                normalized[doc_id][category] = (value - low) / (high - low)

    return normalized


def calculate_score(
    doc_id: str,
    categories: dict[str, float],
    weights: dict[str, float],
    cv_lookup: dict[str, CVDocument],
) -> dict[str, Any]:
    """Compute weighted score for one CV from per-category similarity values."""
    score = 0.0
    for category_name, similarity in categories.items():
        score += float(weights.get(category_name, 0.0)) * float(similarity)

    cv = cv_lookup.get(doc_id)
    if cv is None:
        raise ValueError(f"CVDocument not found for id={doc_id}")

    return {
        "cv_id": str(cv.id),
        "candidate_name": cv.candidate_name,
        "cv": cv.raw_text,
        "candidate_email": cv.email,
        "score": score,
    }


def dedup_results_by_email(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep best-scored row per candidate_email (fallback candidate_name)."""
    best: dict[str, dict[str, Any]] = {}
    for row in results:
        key = (row.get("candidate_email") or "").strip().lower() or (
            row.get("candidate_name") or ""
        ).strip().lower()
        if not key:
            key = row.get("cv_id", "")
        prev = best.get(key)
        if prev is None or float(row.get("score", 0.0)) > float(prev.get("score", 0.0)):
            best[key] = row
    return sorted(best.values(), key=lambda x: x["score"], reverse=True)


class CvScreenPipeline:
    def __init__(self):
        self.client = OpenAIClient(model="gpt-4o-mini", api_key=API_KEY)
        self.vector_store = PgVectorStore()
        self.rag_pipeline = RagPipeline()

    @staticmethod
    def find_occurrences(search_result: dict[str, Any]) -> dict[str, dict[str, float]]:
        """Aggregate best similarity per document across skill/education/experience searches."""
        categories = CATEGORIES

        def _category_doc_similarities(category: str) -> dict[str, float]:
            payload = search_result.get(category, {}) if isinstance(search_result, dict) else {}
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

                similarity = float(metadata.get("similarity", 0.0))
                prev = best_by_doc.get(doc_id)
                if prev is None or similarity > prev:
                    best_by_doc[doc_id] = similarity

            return best_by_doc

        by_category = {category: _category_doc_similarities(category) for category in categories}
        all_doc_ids: set[str] = set()
        for category in categories:
            all_doc_ids.update(by_category[category].keys())

        return {
            doc_id: {
                Category.SKILL.value: by_category[Category.SKILL.value].get(doc_id, 0.0),
                Category.EDUCATION.value: by_category[Category.EDUCATION.value].get(doc_id, 0.0),
                Category.EXPERIENCE.value: by_category[Category.EXPERIENCE.value].get(doc_id, 0.0),
            }
            for doc_id in all_doc_ids
        }

    @staticmethod
    def merge_occurrences(
        semantic_occ: dict[str, dict[str, float]],
        metadata_occ: dict[str, dict[str, float]],
    ) -> dict[str, dict[str, float]]:
        """Merge semantic and metadata occurrences with equal 50/50 contribution."""
        categories = CATEGORIES
        all_ids = set(semantic_occ.keys()) | set(metadata_occ.keys())
        merged: dict[str, dict[str, float]] = {}
        for doc_id in all_ids:
            merged[doc_id] = {}
            for category in categories:
                semantic_score = float(semantic_occ.get(doc_id, {}).get(category, 0.0))
                metadata_score = float(metadata_occ.get(doc_id, {}).get(category, 0.0))
                merged[doc_id][category] = (semantic_score + metadata_score) / 2.0
        return merged

    def run_category_metadata_search(self, query: str, category: str, k: int) -> dict[str, Any]:
        """Run category-specific metadata full-text search (ts_rank)."""
        q = (query or "").strip()
        if not q:
            return {"retriever": []}
        return self.vector_store.search_metadata(query=q, category=category, k=k)

    def compute_metadata(self, job_details: JobProposalSplit, k: int = 25) -> dict[str, Any]:
        """Compute metadata-only retrieval for skill/education/experience in parallel."""
        category_queries = {
            Category.SKILL.value: job_details.skill,
            Category.EDUCATION.value: job_details.education,
            Category.EXPERIENCE.value: job_details.experience,
        }
        out: dict[str, Any] = {
            Category.SKILL.value: {"retriever": []},
            Category.EDUCATION.value: {"retriever": []},
            Category.EXPERIENCE.value: {"retriever": []},
        }
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_map = {
                executor.submit(self.run_category_metadata_search, query, category, k): category
                for category, query in category_queries.items()
            }
            for future in as_completed(future_map):
                category = future_map[future]
                out[category] = future.result()
        return out

    def run_category_search(self, query: str, k: int) -> dict[str, Any]:
        """Run one RAG retrieval for a single category query."""
        q = (query or "").strip()
        if not q:
            return {}

        system_query = (
            "Retrieve CVs relevant to this hiring query. "
            "Preserve important constraints in the wording, but do not apply hard filtering. "
            f"Job request: {q}"
        )
        return self.rag_pipeline.run(
            {
                "rewriter": {"user_prompt": system_query},
                "prompt": {"user_prompt": system_query},
                "retriever": {"collection_name": "sample", "k": max(1, int(k))},
                "retrieve_cvs": {"user_prompt": system_query},
            }
        )

    def semantic_search(self, job_details: JobProposalSplit, k: int = 25) -> dict[str, Any]:
        """Run skill/education/experience searches in parallel and return grouped outputs."""
        category_queries = {
            Category.SKILL.value: job_details.skill,
            Category.EDUCATION.value: job_details.education,
            Category.EXPERIENCE.value: job_details.experience,
        }

        out: dict[str, Any] = {
            Category.SKILL.value: {},
            Category.EDUCATION.value: {},
            Category.EXPERIENCE.value: {},
        }
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_map = {
                executor.submit(self.run_category_search, query, k): category
                for category, query in category_queries.items()
            }
            for future in as_completed(future_map):
                category = future_map[future]
                out[category] = future.result()

        return out

    def split_job_description(self, job_offer_text: str) -> JobProposalSplit:
        """Extract normalized search queries for the three retrieval categories."""
        response = self.client.structured_response(
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
        # TODO - Open issue: Runtime is correct but static typing in datapizza is stricter.
        return response.structured_data[0]

    def run(self, job_description, weights: dict[str, float], top_k: int) -> list[dict[str, Any]]:
        """Execute full retrieval pipeline and return scored CV results."""
        job_details = self.split_job_description(job_description)
        semantic_result = self.semantic_search(job_details, k=top_k)
        metadata_result = self.compute_metadata(job_details, k=top_k)

        semantic_occ = self.find_occurrences(semantic_result)
        metadata_occ = self.find_occurrences(metadata_result)
        occurrences = self.merge_occurrences(semantic_occ, metadata_occ)

        exp_meta_scores = compute_experience_metadata_score(job_details)
        occurrences = apply_experience_metadata_boost(occurrences, exp_meta_scores)
        occurrences = normalize_occurrences(occurrences)

        cvs = {
            str(doc.id): doc
            for doc in CVDocument.objects.filter(id__in=list(occurrences.keys())).only(
                "id", "candidate_name", "raw_text", "email"
            )
        }

        final_results = [
            calculate_score(doc_id, categories, weights, cvs)
            for doc_id, categories in occurrences.items()
            if doc_id in cvs
        ]
        return dedup_results_by_email(final_results)
