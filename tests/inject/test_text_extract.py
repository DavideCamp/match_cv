from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.conftest import MockInjectDocument


@pytest.mark.parametrize(
    ("raw_response_text", "expected_text", "expected_metadata"),
    [
        (
            "FULL_TEXT:\nMario Rossi CV\n\nMETADATA_JSON:\n"
            '{"candidate_name":"Mario Rossi","contact":{"email":"mario.rossi@example.com"}}',
            "Mario Rossi CV",
            {"candidate_name": "Mario Rossi", "contact": {"email": "mario.rossi@example.com"}},
        ),
        (
            "FULL_TEXT:\nCV content\n\nMETADATA_JSON:\n{invalid_json}",
            "CV content",
            {},
        ),
    ],
)
def test_extract_metadata_parsing(raw_response_text, expected_text, expected_metadata):
    inject_doc = MockInjectDocument()
    inject_doc.client.invoke = MagicMock(return_value=SimpleNamespace(text=raw_response_text))

    result = inject_doc.extract_metadata("/tmp/fake.pdf")

    assert result["text"] == expected_text
    assert result["metadata"] == expected_metadata
    inject_doc.client.invoke.assert_called_once()
