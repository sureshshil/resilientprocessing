"""Replace with the existing FPA pipeline and its durable checkpoint logic."""

PIPELINE_IMPLEMENTED = False


async def run_pipeline(interaction_id: str) -> None:
    """Return only after the real pipeline durably records completion."""
    raise NotImplementedError("The real FPA pipeline has not been connected")
