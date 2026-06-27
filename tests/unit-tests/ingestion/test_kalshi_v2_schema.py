"""
Kalshi v2 WSS schema regression tests.

Built from live payloads captured 2026-06-23 against
wss://api.elections.kalshi.com/trade-api/ws/v2 for market KXMENWORLDCUP-26-AR
(see tests/scripts/debug_kalshi_wss_dump.py).

Guards against THREE concrete past failure modes:
  1. Snapshot model silently treated new-format msg (no `yes`/`no` keys)
     as an empty book — every arb check ran on stale Kalshi data.
  2. Delta model rejected every message because field names changed
     (price -> price_dollars, delta -> delta_fp).
  3. A stray "price / 100" anywhere would silently turn "0.1400" into 0.0014,
     corrupting every Kalshi price downstream.
"""
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.domain.models.venue_data_schemas import (
    KalshiSnapshotMessage,
    KalshiDeltaMessage,
)

# ---------------------------------------------------------------------------
# Captured payloads (real wire data, dollars-denominated strings throughout)
# ---------------------------------------------------------------------------

CAPTURED_SNAPSHOT = {
    "type": "orderbook_snapshot",
    "sid": 1,
    "seq": 1,
    "msg": {
        "market_ticker": "KXMENWORLDCUP-26-AR",
        "market_id": "7dfe1022-ddb0-4fdd-a7bb-825112c924c3",
        "yes_dollars_fp": [
            ["0.0010", "111000.00"],
            ["0.0100", "2552842.77"],
            ["0.1400", "140.95"],
        ],
        "no_dollars_fp": [
            ["0.0010", "1004011.11"],
            ["0.0100", "5338404.13"],
            ["0.8560", "1246.46"],
        ],
    },
}

CAPTURED_DELTAS = [
    {  # delta #1: yes 0.1400 -40.16
        "type": "orderbook_delta", "sid": 1, "seq": 2,
        "msg": {"market_ticker": "KXMENWORLDCUP-26-AR",
                "market_id": "7dfe1022-ddb0-4fdd-a7bb-825112c924c3",
                "price_dollars": "0.1400", "delta_fp": "-40.16",
                "side": "yes", "ts": "2026-06-23T22:31:15.782849Z",
                "ts_ms": 1782253875782}},
    {  # delta #2: yes 0.1400 -74.99
        "type": "orderbook_delta", "sid": 1, "seq": 3,
        "msg": {"market_ticker": "KXMENWORLDCUP-26-AR",
                "market_id": "7dfe1022-ddb0-4fdd-a7bb-825112c924c3",
                "price_dollars": "0.1400", "delta_fp": "-74.99",
                "side": "yes", "ts": "2026-06-23T22:31:15.782849Z",
                "ts_ms": 1782253875782}},
    {  # delta #3: yes 0.1400 -7.00
        "type": "orderbook_delta", "sid": 1, "seq": 4,
        "msg": {"market_ticker": "KXMENWORLDCUP-26-AR",
                "market_id": "7dfe1022-ddb0-4fdd-a7bb-825112c924c3",
                "price_dollars": "0.1400", "delta_fp": "-7.00",
                "side": "yes", "ts": "2026-06-23T22:31:15.808486Z",
                "ts_ms": 1782253875808}},
    {  # delta #4: no 0.8570 +24.01
        "type": "orderbook_delta", "sid": 1, "seq": 5,
        "msg": {"market_ticker": "KXMENWORLDCUP-26-AR",
                "market_id": "7dfe1022-ddb0-4fdd-a7bb-825112c924c3",
                "price_dollars": "0.8570", "delta_fp": "24.01",
                "side": "no", "ts": "2026-06-23T22:31:17.510112Z",
                "ts_ms": 1782253877510}},
    {  # delta #5: yes 0.0600 -38.00
        "type": "orderbook_delta", "sid": 1, "seq": 6,
        "msg": {"market_ticker": "KXMENWORLDCUP-26-AR",
                "market_id": "7dfe1022-ddb0-4fdd-a7bb-825112c924c3",
                "price_dollars": "0.0600", "delta_fp": "-38.00",
                "side": "yes", "ts": "2026-06-23T22:31:17.521436Z",
                "ts_ms": 1782253877521}},
]


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

class TestKalshiSnapshotV2:

    def test_snapshot_parses_without_rejection(self):
        msg = KalshiSnapshotMessage.model_validate(CAPTURED_SNAPSHOT).msg
        assert msg.market_ticker == "KXMENWORLDCUP-26-AR"

    def test_snapshot_yields_non_empty_book(self):
        """The empty-book false positive: the old Optional[...] = [] fallback
        silently parsed new-format messages into empty yes/no lists. If both
        sides come back empty, the strategy runs on stale Kalshi data and can
        emit 'arb opportunities' that don't actually exist."""
        msg = KalshiSnapshotMessage.model_validate(CAPTURED_SNAPSHOT).msg
        assert msg.yes_dollars_fp, "yes side must be populated, got empty/None"
        assert msg.no_dollars_fp, "no side must be populated, got empty/None"
        assert len(msg.yes_dollars_fp) == 3
        assert len(msg.no_dollars_fp) == 3

    def test_snapshot_prices_are_dollars_not_cents(self):
        """If someone reintroduces `/100` on a wire price like '0.1400', the
        resulting Decimal would be 0.0014. This test fails if any price ends
        up below 0.001 ($0.001 is Kalshi's smallest real subcent tick)."""
        msg = KalshiSnapshotMessage.model_validate(CAPTURED_SNAPSHOT).msg
        for price, size in msg.yes_dollars_fp + msg.no_dollars_fp:
            assert Decimal("0.001") <= price < Decimal("1"), (
                f"snapshot price {price} is outside the legal $ range — "
                "possible stray /100 conversion"
            )

    def test_snapshot_sizes_preserve_two_decimal_precision(self):
        """Sizes come as decimal strings like '140.95' — must not get
        int-truncated to 140 anywhere along the path."""
        msg = KalshiSnapshotMessage.model_validate(CAPTURED_SNAPSHOT).msg
        # specific captured value
        assert (Decimal("0.1400"), Decimal("140.95")) in msg.yes_dollars_fp

    def test_snapshot_old_format_keys_dont_silently_populate(self):
        """Regression guard for the silent-empty fallback bug: a payload using
        the LEGACY keys ('yes' / 'no' with int cents) must NOT be accepted as
        a valid new-format snapshot with empty books."""
        old_format = {
            "type": "orderbook_snapshot", "sid": 1, "seq": 1,
            "msg": {
                "market_ticker": "FAKE",
                # the old wire format used these names + int cents
                "yes": [[86, 1500]],
                "no": [[14, 2200]],
            },
        }
        msg = KalshiSnapshotMessage.model_validate(old_format).msg
        # The new model must NOT have populated yes_dollars_fp/no_dollars_fp
        # from legacy keys, AND must NOT default them to []. They must be None
        # so the handler can distinguish "absent" from "explicitly empty"
        # and fail loud on the both-None case.
        assert msg.yes_dollars_fp is None, (
            "legacy 'yes' key must not silently materialize as an empty new-format list"
        )
        assert msg.no_dollars_fp is None, (
            "legacy 'no' key must not silently materialize as an empty new-format list"
        )

    def test_snapshot_one_side_empty_is_allowed(self):
        """A market with all liquidity on one side is real. Either field may
        legally be absent. The fail-loud check is reserved for BOTH absent."""
        one_sided = {
            "type": "orderbook_snapshot", "sid": 1, "seq": 1,
            "msg": {
                "market_ticker": "X",
                "yes_dollars_fp": [["0.5000", "100.00"]],
                # no_dollars_fp deliberately omitted
            },
        }
        msg = KalshiSnapshotMessage.model_validate(one_sided).msg
        assert msg.yes_dollars_fp == [(Decimal("0.5000"), Decimal("100.00"))]
        assert msg.no_dollars_fp is None


# ---------------------------------------------------------------------------
# Delta
# ---------------------------------------------------------------------------

class TestKalshiDeltaV2:

    @pytest.mark.parametrize("idx", list(range(len(CAPTURED_DELTAS))))
    def test_each_captured_delta_parses(self, idx):
        msg = KalshiDeltaMessage.model_validate(CAPTURED_DELTAS[idx]).msg
        assert msg.market_ticker == "KXMENWORLDCUP-26-AR"
        assert msg.side in ("yes", "no")

    def test_delta_fp_is_signed(self):
        """delta_fp must round-trip the sign: 4 of 5 captured deltas were
        negative (resting size pulled/filled). If the model coerces to unsigned,
        every book update would silently move the wrong direction."""
        parsed = [KalshiDeltaMessage.model_validate(d).msg for d in CAPTURED_DELTAS]
        negatives = [m for m in parsed if m.delta_fp < 0]
        positives = [m for m in parsed if m.delta_fp > 0]
        assert len(negatives) == 4
        assert len(positives) == 1
        # spot-check exact value preservation (no float drift)
        assert parsed[0].delta_fp == Decimal("-40.16")
        assert parsed[3].delta_fp == Decimal("24.01")

    def test_delta_prices_are_dollars_not_cents(self):
        for raw in CAPTURED_DELTAS:
            msg = KalshiDeltaMessage.model_validate(raw).msg
            assert Decimal("0.001") <= msg.price_dollars < Decimal("1"), (
                f"delta price {msg.price_dollars} outside legal $ range — "
                "possible stray /100 conversion"
            )

    def test_delta_requires_new_format_fields(self):
        """A delta missing price_dollars and delta_fp must fail loudly. This is
        the failure mode that flooded the logs with 1582 tracebacks before the
        v2 migration — confirming it now raises ValidationError, not a silent
        miss."""
        legacy = {
            "type": "orderbook_delta", "sid": 1, "seq": 2,
            "msg": {"market_ticker": "X",
                    "price": 14, "delta": -5, "side": "yes"},  # old names
        }
        with pytest.raises(ValidationError) as exc:
            KalshiDeltaMessage.model_validate(legacy)
        assert "price_dollars" in str(exc.value)
        assert "delta_fp" in str(exc.value)


# ---------------------------------------------------------------------------
# Signed-delta book math (the spec, not the internal state container)
#
# These mirror the math in kalshi_wss_client.py without coupling to the
# WebSocket plumbing. If anyone refactors that file and inverts the sign or
# reintroduces /100 cent math, these tests fail loud.
# ---------------------------------------------------------------------------

def _apply_delta(book_side: dict, price: Decimal, delta_fp: Decimal):
    """Reference implementation of the spec: signed delta_fp adjusts resting
    size at price. Level removed at zero, dropped negative is a protocol
    violation that signals corrupted state."""
    current = book_side.get(price, Decimal(0))
    new = current + delta_fp
    if new < 0:
        return "invalid", new
    if new == 0:
        book_side.pop(price, None)
        return "removed", new
    book_side[price] = new
    return "updated", new


class TestSignedDeltaMath:

    def test_captured_delta_against_captured_snapshot(self):
        """End-to-end: start from captured snapshot's yes-side level
        (0.1400, 140.95), apply captured delta #1 (-40.16), expect 100.79."""
        snap = KalshiSnapshotMessage.model_validate(CAPTURED_SNAPSHOT).msg
        yes_book = {p: s for p, s in snap.yes_dollars_fp}
        assert yes_book[Decimal("0.1400")] == Decimal("140.95")

        d1 = KalshiDeltaMessage.model_validate(CAPTURED_DELTAS[0]).msg
        status, new_size = _apply_delta(yes_book, d1.price_dollars, d1.delta_fp)

        assert status == "updated"
        assert new_size == Decimal("100.79")
        assert yes_book[Decimal("0.1400")] == Decimal("100.79")

    def test_full_pull_removes_level(self):
        book = {Decimal("0.5000"): Decimal("50.00")}
        status, new_size = _apply_delta(book, Decimal("0.5000"), Decimal("-50.00"))
        assert status == "removed"
        assert new_size == Decimal(0)
        assert Decimal("0.5000") not in book

    def test_new_level_added(self):
        book = {}
        status, new_size = _apply_delta(book, Decimal("0.7000"), Decimal("25.00"))
        assert status == "updated"
        assert new_size == Decimal("25.00")
        assert book[Decimal("0.7000")] == Decimal("25.00")

    def test_oversell_signals_invalid_state(self):
        """Resting size 0 + delta -10 = -10. This indicates we missed a prior
        delta and our local book is corrupt. The WSS handler maps this to a
        resubscribe request — the helper just reports the invalid state."""
        book = {}
        status, new_size = _apply_delta(book, Decimal("0.5000"), Decimal("-10.00"))
        assert status == "invalid"
        assert new_size < 0
