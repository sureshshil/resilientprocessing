"""Five fake stages. Replace `run_stage` with the real FPA calls; keep the contract:
return the stage output, or raise (see app.errors.is_transient for how errors are treated)."""
import asyncio

from app.faults import apply_fault


async def run_stage(stage: str, job: dict, tries: int, stage_seconds: float) -> str:
    await apply_fault(job.get("faults", {}).get(stage), tries)
    await asyncio.sleep(stage_seconds)
    return f"{stage} result for {job['_id']} (try {tries})"
