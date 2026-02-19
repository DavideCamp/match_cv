import json
from typing import Any

import environ
from datapizza.clients.openai import OpenAIClient
from datapizza.embedders.openai import OpenAIEmbedder
from datapizza.pipeline import IngestionPipeline
from datapizza.type import MediaBlock, Media

env = environ.Env()


class InjectDocument(IngestionPipeline):
    DEFAULT_VECTOR_DIMENSIONS = 1536

    def __init__(self):
        super().__init__()
        self.embedding_model_name = env.str("EMBEDDING_MODEL_NAME")
        api_key = env.str("OPENAI_API_KEY", default=env.str("OPENAIE_API_KEY", default=""))
        if not api_key:
            raise ValueError("Missing OPENAI_API_KEY environment variable")

        self.embedder_client = OpenAIEmbedder(
            api_key=api_key,
            model_name=self.embedding_model_name,
        )

        self.client = OpenAIClient(
            api_key=api_key,
        )

    @staticmethod
    def _parse_extraction_response(raw_text: str) -> dict[str, Any]:
        full_text_marker = "FULL_TEXT:"
        metadata_marker = "METADATA_JSON:"
        metadata: dict[str, Any] = {}

        full_text_index = raw_text.find(full_text_marker)
        metadata_index = raw_text.find(metadata_marker)

        if full_text_index != -1:
            full_start = full_text_index + len(full_text_marker)
            full_end = metadata_index if metadata_index != -1 else len(raw_text)
            full_text = raw_text[full_start:full_end].strip()
        else:
            full_text = raw_text.strip()

        if metadata_index != -1:
            metadata_raw = raw_text[metadata_index + len(metadata_marker) :].strip()
            try:
                parsed_metadata = json.loads(metadata_raw)
                if isinstance(parsed_metadata, dict):
                    metadata = parsed_metadata
            except json.JSONDecodeError:
                metadata = {}

        return {"text": full_text, "metadata": metadata}

    def extract_metadata(self, pdf_path: str) -> dict[str, Any]:
        pdf_doc = Media(media_type="pdf", source_type="path", source=pdf_path, extension="pdf")

        response = self.client.invoke(
            system_prompt="""

                You are an assistant specialized in CV/Resume PDF analysis. You will receive as input the extracted text content from a PDF CV (or raw text obtained via a PDF extraction tool).

Your tasks are:
1) Return ALL the text from the PDF faithfully and completely (do NOT summarize).
2) Extract structured metadata and produce a valid JSON object containing: skills, education, and seniority (plus related relevant CV fields).

GENERAL RULES (MANDATORY)
- Do NOT hallucinate or invent information. If a field is not explicitly present or cannot be inferred with high confidence, use null or an empty array.
- Do NOT add content that does not appear in the CV.
- If the CV contains multiple columns, reconstruct the most natural reading order (top-to-bottom, left-to-right).
- Remove obvious noise (e.g., repeated headers/footers, page numbers) ONLY in the JSON interpretation if needed; NEVER remove anything from the full extracted text.
- Preserve the original language of the CV in both FULL_TEXT and JSON values (except for enumerated fields like seniority.level).
- Always output in two sections: FULL_TEXT first, then METADATA_JSON.
- The JSON must be strictly valid (double quotes, no comments, no trailing commas).

OUTPUT FORMAT

1) First write:

FULL_TEXT:
<complete CV text>

2) Then write:

METADATA_JSON:
<valid JSON object>

FULL_TEXT REQUIREMENTS
- Return the complete CV text exactly as extracted.
- Preserve line breaks and section separations where possible.
- Preserve bullet points using "-" or "•".
- If tables are present, serialize them into readable rows (e.g., using " | " as separator).
- Do NOT summarize or clean the text.

METADATA_JSON SCHEMA

Use the following structure:

{
  "candidate_name": string|null,
  "contact": {
    "email": string|null,
    "phone": string|null,
    "location": string|null,
    "links": [string]
  },
  "seniority": {
    "level": "intern"|"junior"|"mid"|"senior"|"staff"|"principal"|"lead"|"manager"|"director"|"executive"|null,
    "years_experience_estimate": number|null,
    "rationale": string|null
  },
  "skills": {
    "hard_skills": [string],
    "soft_skills": [string],
    "tools_technologies": [string],
    "languages": [
      { "language": string, "proficiency": string|null }
    ],
    "certifications": [string]
  },
  "education": [
    {
      "degree": string|null,
      "field": string|null,
      "institution": string|null,
      "location": string|null,
      "start_date": string|null,
      "end_date": string|null,
      "grade": string|null,
      "notes": string|null
    }
  ],
  "experience_summary": {
    "current_title": string|null,
    "current_company": string|null,
    "industries": [string],
    "top_roles": [string]
  },
  "extraction_quality": {
    "is_text_complete": boolean,
    "suspected_columns_or_tables": boolean,
    "missing_sections_guess": [string],
    "notes": string
  }
}

SKILLS GUIDELINES
- hard_skills: domain or technical competencies (e.g., “Machine Learning”, “Accounting”, “Java”).
- tools_technologies: specific tools, platforms, frameworks, software (e.g., “AWS”, “Docker”, “SAP”, “Excel”, “Kubernetes”).
- soft_skills: include ONLY if explicitly stated or clearly described (e.g., “teamwork”, “leadership”, “public speaking”).
- languages: human languages only (e.g., English, Italian) with proficiency level if stated (e.g., B2, fluent, native).
- certifications: official certifications explicitly mentioned.

EDUCATION GUIDELINES
- Create one object per relevant degree.
- Normalize dates to ISO format:
  - "YYYY-MM" if month available
  - "YYYY" if only year available
  - null if missing
- If marked as "Ongoing" or "In progress", set end_date = null and explain in notes.

SENIORITY GUIDELINES
- If total years of experience are explicitly mentioned, use them.
- If not explicit, estimate from employment timeline only if clearly reconstructable.
- level mapping guidance:
  - intern: mostly internships/traineeships
  - junior: ~0–2 years or entry-level roles
  - mid: ~2–5 years
  - senior: ~5–8+ years or advanced responsibilities
  - staff/principal: high technical leadership across teams
  - lead/manager/director/executive: only if explicitly stated in the CV
- rationale: short explanation referencing textual evidence (no fabrication).

QUALITY CHECK
- The JSON must always be present, even if mostly empty.
- If a section is missing, use empty arrays and null values appropriately.
- extraction_quality.is_text_complete = true only if the CV appears complete (contact, experience, education present) and no clear truncation signals exist.
- Do not include personal data that is not explicitly present in the CV.

EXAMPLE STRUCTURE (structure only, not content):

FULL_TEXT:
...

METADATA_JSON:

               """,
            input=[MediaBlock(media=pdf_doc)],
            max_tokens=3000,
        )
        return self._parse_extraction_response(getattr(response, "text", "") or "")
