"""Demo fault injection: makes a stage fail the way a real LLM call would."""
import asyncio

import httpx
import openai

from app.errors import PermanentError

SLOW_SECONDS = 60
_REQUEST = httpx.Request("POST", "https://fake-llm.invalid/v1/chat/completions")


def _response(status_code: int) -> httpx.Response:
    return httpx.Response(status_code, request=_REQUEST)


def build_error(fault_type: str) -> Exception:
    """The same exception classes the real OpenAI SDK raises, so retry code can't tell the difference."""
    if fault_type == "llm_429":
        return openai.RateLimitError("Injected rate limit", response=_response(429), body=None)
    if fault_type == "llm_timeout":
        return openai.APITimeoutError(request=_REQUEST)
    if fault_type == "llm_500":
        return openai.InternalServerError("Injected server error", response=_response(500), body=None)
    if fault_type == "llm_content_blocked":
        return openai.BadRequestError("Injected content filter", response=_response(400), body=None)
    if fault_type == "bad_audio":
        return PermanentError("audio is empty or unreadable")
    raise ValueError(f"Unknown fault type: {fault_type}")


async def apply_fault(spec: dict | None, tries: int, slow_seconds: float = SLOW_SECONDS) -> None:
    """Fail (or stall) this try if the job's fault spec says so.
    `tries` is the stage's total try count, so faults survive worker restarts."""
    if not spec:
        return
    times = spec.get("times", 1)
    if times and tries > times:
        return
    if spec["type"] == "slow":
        await asyncio.sleep(slow_seconds)
        return
    raise build_error(spec["type"])
