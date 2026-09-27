import pytest
from pydantic import ValidationError

from app.errors import PermanentError, TransientError, is_transient, safe_error
from app.models import STAGES, Status, new_job_document, utcnow
from tests.helpers import make_request


def test_job_id_combines_session_and_recording():
    assert make_request(sessionId="S-9", recordingId="rec-1").job_id == "S-9:rec-1"


def test_new_job_document_starts_pending_and_flagged_for_send():
    now = utcnow()
    doc = new_job_document(make_request(), now, now)
    assert list(doc["stages"]) == list(STAGES)
    assert all(s["status"] == Status.PENDING and s["tries"] == 0 for s in doc["stages"].values())
    assert doc["status"] == Status.PENDING
    assert doc["sendMe"] is True
    assert doc["owner"] is None
    assert doc["attempts"] == 0
    assert "faults" not in doc["key"]


def test_utcnow_has_millisecond_precision():
    assert utcnow().microsecond % 1000 == 0


def test_unknown_fault_type_is_rejected():
    with pytest.raises(ValidationError):
        make_request(faults={"summary": {"type": "nope"}})


def test_classification_of_our_errors():
    assert is_transient(TransientError("x")) is True
    assert is_transient(RuntimeError("unknown")) is True
    assert is_transient(PermanentError("x")) is False


def test_safe_error_keeps_only_our_messages():
    assert safe_error(PermanentError("audio is empty")) == "PermanentError: audio is empty"
    assert safe_error(RuntimeError("customer said something private")) == "RuntimeError"
