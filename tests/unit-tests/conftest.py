import pytest

from app.settings.settings import settings


@pytest.fixture(autouse=True)
def _risk_files_in_tmp(tmp_path, monkeypatch):
    """Keep the kill switch's halt/state files out of the repo root during tests."""
    monkeypatch.setattr(settings, "RISK_HALT_FILE", str(tmp_path / "risk.HALT"))
