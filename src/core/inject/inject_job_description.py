from __future__ import annotations

import environ
from datapizza.clients.openai import OpenAIClient
from datapizza.embedders.openai import OpenAIEmbedder
from pydantic import BaseModel, Field
from typing import cast

from src.core.models import JobDescription

env = environ.Env()


class JobProposalSplit(BaseModel):
    skill: str = Field(default="")
    education: str = Field(default="")
    experience: str = Field(default="")


class JobDescriptionIngestionJob:
    """Split a job description by category, embed each section, and persist the row."""

    def __init__(self):
        api_key = env.str("OPENAI_API_KEY", default=env.str("OPENAIE_API_KEY", default=""))
        if not api_key:
            raise ValueError("Missing OPENAI_API_KEY environment variable")

        self.embedding_model_name = env.str("EMBEDDING_MODEL_NAME")
        self.client = OpenAIClient(model="gpt-4o-mini", api_key=api_key)
        self.embedder = OpenAIEmbedder(api_key=api_key, model_name=self.embedding_model_name)

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
        # datapizza currently types structured_data as BaseModel; runtime is JobProposalSplit.
        return cast(JobProposalSplit, response.structured_data[0])

    @staticmethod
    def _normalize_text(value: str, fallback: str) -> str:
        text = (value or "").strip()
        return text if text else fallback

    def ingest_job_description(self, job_offer_text: str) -> JobDescription:
        split = self.split_job_description(job_offer_text)

        skill_text = self._normalize_text(split.skill, job_offer_text)
        education_text = self._normalize_text(split.education, job_offer_text)
        experience_text = self._normalize_text(split.experience, job_offer_text)

        skill_embedding = self.embedder.embed(skill_text)
        education_embedding = self.embedder.embed(education_text)
        experience_embedding = self.embedder.embed(experience_text)

        return JobDescription.objects.create(
            text=job_offer_text,
            skill_text=skill_text,
            education_text=education_text,
            experience_text=experience_text,
            metadata={
                "split": split.model_dump(),
                "embedding_model": self.embedding_model_name,
            },
            skill=skill_embedding,
            education=education_embedding,
            experience=experience_embedding,
        )
