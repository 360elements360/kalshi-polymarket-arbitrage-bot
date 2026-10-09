from decimal import Decimal

import pytest

from app.domain.models.opportunity import ArbitrageOpportunity
from app.domain.primitives import Platform
from app.services import risk
from app.settings.settings import settings

OK_BALANCES = (100, 100)


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "RISK_HALT_FILE", str(tmp_path / "risk.HALT"))
    monkeypatch.setattr(settings, "NTFY_TOPIC", "")
    monkeypatch.setattr(risk, "_start_balances", None)
    monkeypatch.setattr(risk, "_shutdown_event", None)


def opp(yes="0.40", no="0.50", edge="0.08"):
    return ArbitrageOpportunity(
        market_id="m", buy_yes_platform=Platform.KALSHI, buy_yes_price=Decimal(yes),
        buy_no_platform=Platform.POLYMARKET, buy_no_price=Decimal(no), profit_margin=Decimal(edge),
        potential_trade_size=Decimal("10"), kalshi_ticker="T", polymarket_yes_token_id="y",
        polymarket_no_token_id="n",
    )


def halted():
    return risk._halt_path().exists()


def test_clean_trade_passes():
    assert risk.check(opp(), 10, OK_BALANCES) is None


def test_per_trade_cap():
    assert "notional" in risk.check(opp(no="0.50"), 21, OK_BALANCES)  # 21 * 0.50 = 10.50 > 10


def test_hourly_cap():
    for _ in range(3):
        risk.record_sent(opp(), 1)
    assert "hourly" in risk.check(opp(), 1, OK_BALANCES)


def test_open_exposure_cap(monkeypatch):
    monkeypatch.setattr(settings, "RISK_MAX_TRADES_PER_HOUR", 99)
    for _ in range(3):
        risk.record_sent(opp(), 20)  # 3 * 20 * 0.90 = 54
    assert "exposure" in risk.check(opp(), 10, OK_BALANCES)  # +9 > 60


@pytest.mark.parametrize("edge", ["0.001", "0.30"])
def test_edge_band(edge):
    assert "edge" in risk.check(opp(edge=edge), 1, OK_BALANCES)


def test_balance_drop_and_missing_balances():
    assert risk.check(opp(), 1, (100, 100)) is None  # captures start
    assert "kalshi" in risk.check(opp(), 1, (70, 100))
    assert "unavailable" in risk.check(opp(), 1, None)


def test_consecutive_leg_failures_halt():
    risk.record_leg_failure()
    assert not halted()
    risk.record_leg_failure()
    assert halted()


def test_leg_success_resets_failures():
    risk.record_leg_failure()
    risk.record_leg_success()
    risk.record_leg_failure()
    assert not halted()


def test_mismatch_halts_and_sets_shutdown():
    class Ev:
        is_set = False
        def set(self): self.is_set = True
    ev = Ev()
    risk.set_shutdown_event(ev)
    risk.record_sent(opp(), 1)
    risk.record_resolution(0.0, mismatch=True)
    assert halted() and ev.is_set


def test_realized_loss_floor_halts():
    risk.record_resolution(-10.0, mismatch=False)
    assert not halted()
    risk.record_resolution(-5.0, mismatch=False)
    assert halted()


def test_halt_file_blocks_check():
    risk.halt("test")
    assert "halt file" in risk.check(opp(), 1, OK_BALANCES)
    risk._halt_path().unlink()
    assert risk.check(opp(), 1, OK_BALANCES) is None
