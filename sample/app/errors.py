"""Error types and the transient/permanent decision."""
import openai


class TransientError(Exception):
    """A failure that may succeed if tried again later."""


class PermanentError(Exception):
    """A failure that will never succeed for this input."""


class LeaseLost(Exception):
    """This worker no longer owns the job; another worker has claimed it."""


class RepositoryUnavailable(Exception):
    """MongoDB/Cosmos kept failing after the repository's own retries."""


def is_transient(exc: BaseException) -> bool:
    """Rate limits, timeouts, 5xx and unknown errors are retried (bounded by the attempt budget).
    Invalid input and content-filter rejections are not."""
    return not isinstance(exc, (PermanentError, openai.BadRequestError))


def safe_error(exc: BaseException) -> str:
    """Short text for the job document. Provider messages can contain customer data,
    so only our own exceptions keep their message."""
    if isinstance(exc, (TransientError, PermanentError)):
        return f"{type(exc).__name__}: {exc}"[:300]
    return type(exc).__name__
