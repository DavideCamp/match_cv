from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from datapizza.clients.mock_client import MockClient
from django.core.files.uploadedfile import SimpleUploadedFile

from src.core.inject.inject import InjectDocument


class MockInjectDocument(InjectDocument):
    """
    Test double for `InjectDocument`.
    Mirrors methods used in upload flow: from_yaml, extract_metadata, run.
    """

    def __init__(
        self,
        *,
        extracted_text: str = "Mock CV text",
        extracted_metadata: dict[str, Any] | None = None,
        embedded_chunks: list[Any] | None = None,
    ) -> None:
        # Do NOT call super().__init__() to avoid creating real external clients.
        self.client = MockClient()
        self.embedder_client = MockClient()
        self.embedding_model_name = "mock-embedding"
        self._extracted_text = extracted_text
        self._extracted_metadata = extracted_metadata or {}
        self._embedded_chunks = embedded_chunks or []
        self.yaml_path: str | None = None

    def from_yaml(self, yaml_path: str) -> "MockInjectDocument":
        self.yaml_path = yaml_path
        return self

    def run(self, text: str, metadata: dict[str, Any] | None = None):
        _ = text
        _ = metadata
        return self._embedded_chunks


@pytest.fixture
def api_client():
    from rest_framework.test import APIClient

    return APIClient()


@pytest.fixture
def make_uploaded_file():
    def _make(
        name: str = "cv_test.txt",
        content: bytes = b"Test CV content",
        content_type: str = "text/plain",
    ) -> SimpleUploadedFile:
        return SimpleUploadedFile(name, content, content_type=content_type)

    return _make


@pytest.fixture
def temp_media_root(settings, tmp_path):
    media_root = tmp_path / "media"
    settings.MEDIA_ROOT = str(media_root)
    return media_root


@pytest.fixture
def build_mock_inject_document():
    def _build(
        *,
        extracted_text: str = "Mock CV text",
        extracted_metadata: dict[str, Any] | None = None,
        embedded_chunks: list[Any] | None = None,
    ) -> MagicMock:
        mock = MagicMock()
        mock.from_yaml.return_value = mock
        mock.extract_metadata.return_value = {
            "text": extracted_text,
            "metadata": extracted_metadata or {},
        }
        mock.run.return_value = embedded_chunks or []
        return mock

    return _build
