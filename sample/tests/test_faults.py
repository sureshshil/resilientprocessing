import openai
import pytest

from app.errors import PermanentError, is_transient, safe_error
from app.faults import apply_fault, build_error
from app.stages import run_stage


@pytest.mark.parametrize("fault, transient", [
    ("llm_429", True), ("llm_timeout", True), ("llm_500", True),
    ("llm_content_blocked", False), ("bad_audio", False),
])
def test_fault_classification(fault, transient):
    assert is_transient(build_error(fault)) is transient


def test_provider_messages_are_not_stored():
    assert safe_error(build_error("llm_500")) == "InternalServerError"


async def test_fault_fires_only_for_first_n_tries():
    spec = {"type": "llm_429", "times": 2}
    for tries in (1, 2):
        with pytest.raises(openai.RateLimitError):
            await apply_fault(spec, tries)
    await apply_fault(spec, 3)


async def test_times_zero_means_every_try():
    with pytest.raises(PermanentError):
        await apply_fault({"type": "bad_audio", "times": 0}, 99)


async def test_no_fault_and_slow_fault_do_not_raise():
    await apply_fault(None, 1)
    await apply_fault({"type": "slow", "times": 1}, 1, slow_seconds=0)


async def test_stage_uses_the_job_fault():
    job = {"_id": "S-1:rec-1", "faults": {"summary": {"type": "llm_timeout", "times": 1}}}
    with pytest.raises(openai.APITimeoutError):
        await run_stage("summary", job, 1, stage_seconds=0)
    assert "summary" in await run_stage("summary", job, 2, stage_seconds=0)
