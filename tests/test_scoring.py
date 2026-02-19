from __future__ import annotations

import pytest

from src.core.retrieve.pipeline import (
    _parse_experience_constraints,
    _score_years_against_constraints,
    dedup_results_by_email,
    normalize_occurrences,
)
from src.core.serializers import SearchRunRequestSerializer


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("less than 3 years", (None, 3.0)),
        ("at least 5 years", (5.0, None)),
        ("2-4 years", (2.0, 4.0)),
        ("5+ years", (5.0, None)),
        ("no years mentioned", (None, None)),
    ],
)
def test_parse_experience_constraints(query, expected):
    assert _parse_experience_constraints(query) == expected


@pytest.mark.parametrize(
    ("years", "min_years", "max_years", "expected"),
    [
        (2, None, 3, 1.0),
        (5, None, 3, 0.6),
        (6, 5, None, 1.0),
        (3, 5, None, 0.6),
        (4, 2, 6, 1.0),
        (1, 2, 6, 0.5),
        (10, 2, 6, 0.6),
    ],
)
def test_score_years_against_constraints(years, min_years, max_years, expected):
    assert _score_years_against_constraints(years, min_years, max_years) == pytest.approx(expected)


def test_normalize_occurrences_min_max_per_category():
    occ = {
        "doc_a": {"skill": 0.2, "education": 0.3, "experience": 0.5},
        "doc_b": {"skill": 0.6, "education": 0.3, "experience": 0.1},
        "doc_c": {"skill": 0.4, "education": 0.9, "experience": 0.3},
    }

    normalized = normalize_occurrences(occ)

    assert normalized["doc_a"]["skill"] == pytest.approx(0.0)
    assert normalized["doc_b"]["skill"] == pytest.approx(1.0)
    assert normalized["doc_c"]["skill"] == pytest.approx(0.5)

    assert normalized["doc_a"]["education"] == pytest.approx(0.0)
    assert normalized["doc_b"]["education"] == pytest.approx(0.0)
    assert normalized["doc_c"]["education"] == pytest.approx(1.0)

    assert normalized["doc_b"]["experience"] == pytest.approx(0.0)
    assert normalized["doc_a"]["experience"] == pytest.approx(1.0)
    assert normalized["doc_c"]["experience"] == pytest.approx(0.5)


@pytest.mark.parametrize(
    ("weights", "is_valid"),
    [
        ({"skill": 0.1, "experience": 0.7, "education": 0.2}, True),
        ({"skill": 1, "experience": 0, "education": 0}, True),
        ({"skill": 0.2, "experience": 0.2, "education": 0.2}, False),
        ({"skill": 0.5, "experience": 0.6, "education": -0.1}, False),
    ],
)
def test_search_request_weights_sum_to_one(weights, is_valid):
    serializer = SearchRunRequestSerializer(
        data={"job_offer_text": "backend engineer", "weights": weights}
    )
    assert serializer.is_valid() is is_valid


def test_dedup_results_by_email_keeps_best_score():
    rows = [
        {
            "cv_id": "1",
            "candidate_name": "Mario Rossi",
            "candidate_email": "mario@example.com",
            "score": 0.5,
        },
        {
            "cv_id": "2",
            "candidate_name": "Mario Rossi",
            "candidate_email": "mario@example.com",
            "score": 0.8,
        },
        {
            "cv_id": "3",
            "candidate_name": "Sara Neri",
            "candidate_email": "sara@example.com",
            "score": 0.7,
        },
    ]

    deduped = dedup_results_by_email(rows)

    assert len(deduped) == 2
    assert deduped[0]["cv_id"] == "2"
    assert deduped[0]["score"] == pytest.approx(0.8)
    assert deduped[1]["cv_id"] == "3"


def test_dedup_results_by_email_fallback_to_name_when_email_missing():
    rows = [
        {"cv_id": "1", "candidate_name": "Valentina Greco", "candidate_email": "", "score": 0.3},
        {"cv_id": "2", "candidate_name": "Valentina Greco", "candidate_email": None, "score": 0.6},
        {"cv_id": "3", "candidate_name": "Chiara Moretti", "candidate_email": None, "score": 0.5},
    ]

    deduped = dedup_results_by_email(rows)

    assert len(deduped) == 2
    assert deduped[0]["cv_id"] == "2"
    assert deduped[1]["cv_id"] == "3"


def test_dedup_results_by_email_fallback_to_cv_id_when_name_and_email_missing():
    rows = [
        {"cv_id": "a", "candidate_name": "", "candidate_email": "", "score": 0.2},
        {"cv_id": "b", "candidate_name": "", "candidate_email": "", "score": 0.9},
    ]

    deduped = dedup_results_by_email(rows)

    assert len(deduped) == 2
    assert deduped[0]["cv_id"] == "b"
    assert deduped[1]["cv_id"] == "a"
