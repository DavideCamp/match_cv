from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from celery.exceptions import Retry

from src.core.models import UploadStatus
from src.core.tasks import ingest_upload_item_task, ping


def _build_item(*, status: str = UploadStatus.PENDING, with_document: bool = True):
    batch = SimpleNamespace(
        started_at=None,
        status=UploadStatus.PENDING,
        save=MagicMock(),
    )
    item = SimpleNamespace(
        id="item-1",
        status=status,
        document_id="doc-1" if with_document else None,
        batch=batch,
        started_at=None,
        completed_at=None,
        error_message="",
        save=MagicMock(),
    )
    return item, batch


def _mock_upload_item_queryset(mock_upload_item_cls, item):
    select_related_qs = MagicMock()
    filtered_qs = MagicMock()
    mock_upload_item_cls.objects.select_related.return_value = select_related_qs
    select_related_qs.filter.return_value = filtered_qs
    filtered_qs.first.return_value = item


def test_ping_task_without_worker():
    assert ping() == "pong"


@patch("src.core.tasks._refresh_batch_status")
@patch("src.core.tasks.ingest_cv_document")
@patch("src.core.tasks.CVDocument")
@patch("src.core.tasks.UploadItem")
def test_ingest_upload_item_task_success_without_worker(
    mock_upload_item_cls,
    mock_cv_document_cls,
    mock_ingest,
    mock_refresh_batch_status,
):
    item, batch = _build_item()
    _mock_upload_item_queryset(mock_upload_item_cls, item)
    mock_cv_document_cls.objects.get.return_value = SimpleNamespace(id="doc-1")

    out = ingest_upload_item_task.apply(args=("item-1",), throw=True).get()

    assert out == "item-1"
    assert item.status == UploadStatus.SUCCESS
    assert item.started_at is not None
    assert item.completed_at is not None
    assert batch.started_at is not None
    mock_ingest.assert_called_once()
    mock_refresh_batch_status.assert_called_once_with(batch)


@patch("src.core.tasks._refresh_batch_status")
@patch("src.core.tasks.ingest_cv_document", side_effect=RuntimeError("temporary failure"))
@patch("src.core.tasks.CVDocument")
@patch("src.core.tasks.UploadItem")
def test_ingest_upload_item_task_retry_without_worker(
    mock_upload_item_cls,
    mock_cv_document_cls,
    _mock_ingest,
    mock_refresh_batch_status,
):
    item, batch = _build_item()
    _mock_upload_item_queryset(mock_upload_item_cls, item)
    mock_cv_document_cls.objects.get.return_value = SimpleNamespace(id="doc-1")

    with pytest.raises(Retry):
        ingest_upload_item_task.apply(args=("item-1",), throw=True)

    assert item.status == UploadStatus.PENDING
    assert "retrying (1/3)" in item.error_message
    # Called once in except retry branch and once in finally.
    assert mock_refresh_batch_status.call_count == 2
    assert mock_refresh_batch_status.call_args.args[0] == batch


@patch("src.core.tasks._refresh_batch_status")
@patch("src.core.tasks.ingest_cv_document", side_effect=RuntimeError("hard failure"))
@patch("src.core.tasks.CVDocument")
@patch("src.core.tasks.UploadItem")
def test_ingest_upload_item_task_failed_after_max_retries_without_worker(
    mock_upload_item_cls,
    mock_cv_document_cls,
    _mock_ingest,
    mock_refresh_batch_status,
):
    item, batch = _build_item()
    _mock_upload_item_queryset(mock_upload_item_cls, item)
    mock_cv_document_cls.objects.get.return_value = SimpleNamespace(id="doc-1")

    out = ingest_upload_item_task.apply(args=("item-1",), throw=True, retries=3).get()

    assert out == "item-1"
    assert item.status == UploadStatus.FAILED
    assert item.completed_at is not None
    assert item.error_message == "hard failure"
    mock_refresh_batch_status.assert_called_once_with(batch)
