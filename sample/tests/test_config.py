from app.config import load_settings


def test_env_overrides_defaults(monkeypatch):
    monkeypatch.setenv("LEASE_SECONDS", "90")
    monkeypatch.setenv("STAGE_TRIES", "5")
    monkeypatch.setenv("WORKER_ID", "w1")
    settings = load_settings()
    assert settings.lease_seconds == 90.0
    assert settings.stage_tries == 5
    assert settings.worker_id == "w1"


def test_worker_id_has_a_default(monkeypatch):
    monkeypatch.delenv("WORKER_ID", raising=False)
    assert load_settings().worker_id
