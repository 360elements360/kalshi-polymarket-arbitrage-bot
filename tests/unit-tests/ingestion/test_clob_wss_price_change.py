"""Polymarket CLOB WSS routing regression tests.

Guards the price_change fix landed 2026-06-23 after the v2 audit showed
94 % of Polymarket WSS frames were silently dropped. Captured payloads live
in tests/scripts/runs/2026-06-23-payload-capture/frames.jsonl; the
fixed payload below is one real entry from that run.

The single most important test is `test_price_change_side_routing_per_entry`
— it asserts each entry inside a price_change frame is routed to the
correct YES/NO book via the entry's own asset_id, NOT a frame-level
outcome. Getting that wrong silently corrupts books, which is worse than
the original silent-drop bug.
"""
from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import List
from unittest.mock import MagicMock

import pytest

from app.domain.events import (
    OrderBookDeltaReceived,
    OrderBookSnapshotReceived,
)
from app.domain.primitives import Platform, SIDES
from app.ingestion.clob_wss import PolymarketWebSocketClient
from app.settings.env import Environment

# -- Token / condition_id constants taken directly from the captured payloads.
COND_A = "0x4f3421fb2daf5cca7430ed8d8132463963081572d75434393a1808fdb8829fe8"
A_YES_TOKEN = "31940783580344558651011323787577288681658737625185216525249046282994042503801"
A_NO_TOKEN  = "45415751658241142530386585138386640503488308219341470020075667342738719018629"

COND_B = "0x0c4cd2055d6ea89354ffddc55d6dbcef9355748112ea952fc925f3db6a5c457f"
B_YES_TOKEN = "18812649149814341758733697580460697418474693998558159483117100240528657629879"
B_NO_TOKEN  = "11542815374699689221179899936630889707872311763405978342337518804390370374906"

MARKETS_CONFIG = [
    {
        "id": "MKT_A",
        "kalshi_ticker": "MKT_A",
        "polymarket_yes_token_id": A_YES_TOKEN,
        "polymarket_no_token_id":  A_NO_TOKEN,
        "polymarket_condition_id": COND_A,
    },
    {
        "id": "MKT_B",
        "kalshi_ticker": "MKT_B",
        "polymarket_yes_token_id": B_YES_TOKEN,
        "polymarket_no_token_id":  B_NO_TOKEN,
        "polymarket_condition_id": COND_B,
    },
]

CAPTURED_FRAMES_PATH = (
    Path(__file__).resolve().parents[3]
    / "tests" / "scripts" / "runs" / "2026-06-23-payload-capture" / "frames.jsonl"
)

# Real captured price_change for COND_A — both legs in one frame.
CAPTURED_PRICE_CHANGE_BOTH_LEGS = {
    "market": COND_A,
    "price_changes": [
        {"asset_id": A_YES_TOKEN, "price": "0.1", "size": "4100",
         "side": "BUY",  "hash": "9ef112f9ff", "best_bid": "0.92", "best_ask": "0.921"},
        {"asset_id": A_NO_TOKEN,  "price": "0.9", "size": "4100",
         "side": "SELL", "hash": "ff811f66b0", "best_bid": "0.079", "best_ask": "0.08"},
    ],
    "timestamp": "1782270984706",
    "event_type": "price_change",
}

# Real captured last_trade_price (no order-book mutation expected).
CAPTURED_LAST_TRADE_PRICE = {
    "market": COND_B, "asset_id": B_YES_TOKEN,
    "price": "0.14", "size": "714.285713",
    "fee_rate_bps": "0", "side": "BUY", "timestamp": "1782270993821",
    "event_type": "last_trade_price",
    "transaction_hash": "0x9d707a739e9a99667d27c51eaca3330fa02eb3d5c203559283f0fa6616df7cb0",
}

# Synthetic small book frame — shape verified against the real payload, sizes
# trimmed for test ergonomics.
SYNTHETIC_BOOK_YES_LEG = {
    "market": COND_B,
    "asset_id": B_YES_TOKEN,
    "timestamp": "1782270984008",
    "bids": [{"price": "0.001", "size": "2978202.52"}],
    "asks": [{"price": "0.86", "size": "300"}],
    "event_type": "book",
}

SYNTHETIC_BOOK_NO_LEG = {
    "market": COND_A,
    "asset_id": A_NO_TOKEN,
    "timestamp": "1782270984008",
    "bids": [{"price": "0.07", "size": "500"}],
    "asks": [{"price": "0.90", "size": "200"}],
    "event_type": "book",
}


class FakeBus:
    """Captures every publish so tests can assert on bus traffic."""
    def __init__(self):
        self.published: List = []

    async def publish(self, message):
        self.published.append(message)


def make_client() -> PolymarketWebSocketClient:
    client = PolymarketWebSocketClient(environment=Environment.PROD)
    client.set_market_config(MARKETS_CONFIG)
    client.set_message_bus(FakeBus())
    return client


# ---------------------------------------------------------------------------
# Side routing — the test the user most wants. Per-entry asset_id MUST drive
# YES/NO selection inside the loop. Frame-level outcome would corrupt books.
# ---------------------------------------------------------------------------

def test_price_change_side_routing_per_entry():
    """Both legs in one frame route to the correct outcome via per-entry asset_id."""
    client = make_client()
    asyncio.run(client._process_and_publish_event(CAPTURED_PRICE_CHANGE_BOTH_LEGS))

    deltas = [e for e in client.bus.published if isinstance(e, OrderBookDeltaReceived)]
    assert len(deltas) == 2, f"expected 2 deltas (one per entry), got {len(deltas)}"

    by_outcome = {d.outcome: d for d in deltas}
    assert "YES" in by_outcome and "NO" in by_outcome, (
        f"expected one YES and one NO delta, got {sorted(by_outcome)}"
    )

    yes_delta = by_outcome["YES"]
    no_delta = by_outcome["NO"]

    # YES entry was the BUY 0.1 4100; NO entry was the SELL 0.9 4100.
    assert yes_delta.market_id == "MKT_A"
    assert yes_delta.side == SIDES.BUY
    assert yes_delta.price == Decimal("0.1")
    assert yes_delta.size == Decimal("4100")

    assert no_delta.market_id == "MKT_A"
    assert no_delta.side == SIDES.SELL
    assert no_delta.price == Decimal("0.9")
    assert no_delta.size == Decimal("4100")


def test_price_change_yes_only_routes_only_yes_book():
    """A price_change with one entry on the YES token must NOT touch the NO book."""
    client = make_client()
    frame = {
        "market": COND_A,
        "price_changes": [
            {"asset_id": A_YES_TOKEN, "price": "0.42", "size": "11", "side": "BUY"},
        ],
        "event_type": "price_change",
    }
    asyncio.run(client._process_and_publish_event(frame))
    deltas = [e for e in client.bus.published if isinstance(e, OrderBookDeltaReceived)]
    assert len(deltas) == 1
    assert deltas[0].outcome == "YES"
    assert deltas[0].market_id == "MKT_A"
    assert deltas[0].price == Decimal("0.42")
    assert deltas[0].size == Decimal("11")
    assert deltas[0].side == SIDES.BUY
    # And nothing on any other book.
    other = [d for d in deltas if d.outcome != "YES" or d.market_id != "MKT_A"]
    assert other == []


def test_price_change_no_only_routes_only_no_book():
    client = make_client()
    frame = {
        "market": COND_B,
        "price_changes": [
            {"asset_id": B_NO_TOKEN, "price": "0.13", "size": "7", "side": "SELL"},
        ],
        "event_type": "price_change",
    }
    asyncio.run(client._process_and_publish_event(frame))
    deltas = [e for e in client.bus.published if isinstance(e, OrderBookDeltaReceived)]
    assert len(deltas) == 1
    assert deltas[0].outcome == "NO"
    assert deltas[0].market_id == "MKT_B"
    assert deltas[0].side == SIDES.SELL
    assert deltas[0].price == Decimal("0.13")


# ---------------------------------------------------------------------------
# No regression on book + last_trade_price paths
# ---------------------------------------------------------------------------

def test_book_event_routes_correctly_for_yes_leg():
    client = make_client()
    asyncio.run(client._process_and_publish_event(SYNTHETIC_BOOK_YES_LEG))
    snaps = [e for e in client.bus.published if isinstance(e, OrderBookSnapshotReceived)]
    assert len(snaps) == 1
    s = snaps[0]
    assert s.platform == Platform.POLYMARKET
    assert s.market_id == "MKT_B"
    assert s.outcome == "YES"
    assert len(s.bids) == 1 and s.bids[0].price == Decimal("0.001")
    assert len(s.asks) == 1 and s.asks[0].price == Decimal("0.86")


def test_book_event_routes_correctly_for_no_leg():
    client = make_client()
    asyncio.run(client._process_and_publish_event(SYNTHETIC_BOOK_NO_LEG))
    snaps = [e for e in client.bus.published if isinstance(e, OrderBookSnapshotReceived)]
    assert len(snaps) == 1
    s = snaps[0]
    assert s.market_id == "MKT_A"
    assert s.outcome == "NO"


def test_last_trade_price_does_not_emit_book_event():
    """last_trade_price has no order-book mutation; it must accept the frame
    without raising and without publishing any book event (preserves the
    pre-fix behaviour for that path)."""
    client = make_client()
    asyncio.run(client._process_and_publish_event(CAPTURED_LAST_TRADE_PRICE))
    snaps = [e for e in client.bus.published if isinstance(e, OrderBookSnapshotReceived)]
    deltas = [e for e in client.bus.published if isinstance(e, OrderBookDeltaReceived)]
    assert snaps == []
    assert deltas == []


# ---------------------------------------------------------------------------
# Fail-loud paths
# ---------------------------------------------------------------------------

def test_price_change_unknown_token_warns_and_skips(caplog):
    """If a tracked-market price_change contains an entry whose asset_id is not
    in market_map, we must WARN (not silently drop the entry) and continue
    processing the rest of the entries."""
    client = make_client()
    unknown_token = "99999999999999999999999999999999999999999999999999999999999999999999999999"
    frame = {
        "market": COND_A,
        "price_changes": [
            # One legit YES entry
            {"asset_id": A_YES_TOKEN, "price": "0.5", "size": "10", "side": "BUY"},
            # One entry for a token we don't know about
            {"asset_id": unknown_token, "price": "0.5", "size": "99", "side": "BUY"},
        ],
        "event_type": "price_change",
    }
    with caplog.at_level(logging.WARNING, logger="app.ingestion.clob_wss"):
        asyncio.run(client._process_and_publish_event(frame))

    # Only the legitimate YES entry should produce an event.
    deltas = [e for e in client.bus.published if isinstance(e, OrderBookDeltaReceived)]
    assert len(deltas) == 1
    assert deltas[0].outcome == "YES"

    # And the unknown token must have been warned about (not silent).
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert any(unknown_token in r.getMessage() for r in warnings), (
        f"expected a WARNING mentioning the unknown asset_id, got: "
        f"{[r.getMessage() for r in warnings]}"
    )


def test_price_change_market_token_mismatch_refuses(caplog):
    """If a tracked-market price_change contains an entry whose asset_id maps
    to a DIFFERENT tracked market, we must refuse to apply (would otherwise
    corrupt the wrong market's book) and log at ERROR."""
    client = make_client()
    # COND_A frame but the entry's asset_id is MKT_B's YES token.
    frame = {
        "market": COND_A,
        "price_changes": [
            {"asset_id": B_YES_TOKEN, "price": "0.5", "size": "10", "side": "BUY"},
        ],
        "event_type": "price_change",
    }
    with caplog.at_level(logging.ERROR, logger="app.ingestion.clob_wss"):
        asyncio.run(client._process_and_publish_event(frame))

    deltas = [e for e in client.bus.published if isinstance(e, OrderBookDeltaReceived)]
    assert deltas == [], "must refuse to publish when condition_id and asset_id resolve to different markets"

    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert any(
        "MKT_A" in r.getMessage() and "MKT_B" in r.getMessage()
        for r in errors
    ), f"expected ERROR mentioning both markets, got: {[r.getMessage() for r in errors]}"


def test_price_change_with_untracked_condition_id_drops_silently():
    """If the frame's condition_id is not in condition_id_map, the bot is not
    subscribed to that market — drop silently (don't warn for every unrelated
    market we happen to receive frames for)."""
    client = make_client()
    frame = {
        "market": "0xdeadbeef" + "00" * 28,
        "price_changes": [
            {"asset_id": "12345", "price": "0.5", "size": "1", "side": "BUY"},
        ],
        "event_type": "price_change",
    }
    asyncio.run(client._process_and_publish_event(frame))
    deltas = [e for e in client.bus.published if isinstance(e, OrderBookDeltaReceived)]
    assert deltas == []


# ---------------------------------------------------------------------------
# Aggregate: feed the CAPTURED frame disposition log through and confirm the
# acceptance rate for tracked condition_ids goes from ~6 % to ~100 %.
# ---------------------------------------------------------------------------

def test_replay_captured_frames_acceptance_rate_after_fix():
    """Replay the captured raw payloads through the FIXED handler and assert
    that every price_change for a tracked condition_id is now accepted (was
    100 % dropped pre-fix)."""
    if not CAPTURED_FRAMES_PATH.exists():
        pytest.skip(f"captured frames not available: {CAPTURED_FRAMES_PATH}")

    client = make_client()
    bus: FakeBus = client.bus

    tracked_conds = {COND_A, COND_B}
    tracked_tokens = {A_YES_TOKEN, A_NO_TOKEN, B_YES_TOKEN, B_NO_TOKEN}

    seen_pc_frames = 0
    seen_pc_entries = 0
    seen_pc_entries_for_tracked = 0

    with CAPTURED_FRAMES_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("venue") != "polymarket": continue
            note = rec.get("note")
            if not note: continue
            try:
                data = json.loads(note)
            except json.JSONDecodeError:
                continue  # truncated payloads (e.g. large book snapshots)

            if data.get("event_type") == "price_change" and data.get("market") in tracked_conds:
                seen_pc_frames += 1
                for ch in data.get("price_changes", []):
                    seen_pc_entries += 1
                    if ch.get("asset_id") in tracked_tokens:
                        seen_pc_entries_for_tracked += 1

            asyncio.run(client._process_and_publish_event(data))

    deltas = [e for e in bus.published if isinstance(e, OrderBookDeltaReceived)]
    if seen_pc_entries_for_tracked == 0:
        pytest.skip("captured frames had no price_change entries for tracked markets")

    # Every entry that names a tracked token should produce exactly one delta.
    assert len(deltas) == seen_pc_entries_for_tracked, (
        f"expected {seen_pc_entries_for_tracked} deltas (1 per tracked entry), "
        f"got {len(deltas)} — accepted frames={seen_pc_frames}, "
        f"total entries={seen_pc_entries}"
    )
