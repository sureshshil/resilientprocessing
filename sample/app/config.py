"""Settings loaded from environment variables (and an optional .env file)."""
import os
import socket
from dataclasses import dataclass, fields

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "fpa_sample"
    servicebus_connection_string: str = ""
    servicebus_namespace: str = ""
    servicebus_queue: str = "fpa-pipeline"
    worker_id: str = ""
    lease_seconds: float = 60
    heartbeat_seconds: float = 20
    stage_tries: int = 3
    stage_backoff_seconds: float = 1
    max_attempts: int = 5
    retry_base_seconds: float = 30
    retry_cap_seconds: float = 300
    publish_interval_seconds: float = 2
    publish_grace_seconds: float = 5
    sweep_interval_seconds: float = 60
    stuck_running_seconds: float = 120
    stuck_unsent_seconds: float = 600
    stage_seconds: float = 3
    unavailable_pause_seconds: float = 10
    mongo_tries: int = 3
    mongo_backoff_seconds: float = 0.5
    fault_cosmos_429_rate: float = 0.0


def load_settings() -> Settings:
    """Each field can be overridden by the upper-case environment variable of the same name."""
    load_dotenv()
    values = {}
    for field in fields(Settings):
        raw = os.environ.get(field.name.upper())
        if raw:
            values[field.name] = field.type(raw)
    values.setdefault("worker_id", f"{socket.gethostname()}-{os.getpid()}")
    return Settings(**values)
