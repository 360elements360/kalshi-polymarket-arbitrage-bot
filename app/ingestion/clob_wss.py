import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Optional, Any

import websockets

# Observation-only raw-frame sink. Active when FRAME_LOG_PATH env var is set.
# Each line: {ts, venue, kind, event_type|None, asset_id|None}
_FRAME_LOG_PATH = os.environ.get("FRAME_LOG_PATH")


def _record_frame(venue: str, kind: str, event_type: Optional[str] = None,
                  asset_id: Optional[str] = None, note: Optional[str] = None) -> None:
    if not _FRAME_LOG_PATH:
        return
    try:
        with open(_FRAME_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "venue": venue,
                "kind": kind,
                "event_type": event_type,
                "asset_id": asset_id,
                "note": note,
            }) + "\n")
    except Exception:
        pass
from pydantic import ValidationError
from websockets.legacy.client import WebSocketClientProtocol

from app.clients.polymarket.poly_market_base import PolymBaseClient
from app.domain.events import PriceLevelData, OrderBookSnapshotReceived, OrderBookDeltaReceived
from app.domain.models.venue_data_schemas import PolyBookMessage, PolyPriceChangeMessage
from app.domain.primitives import Platform, SIDES
from app.message_bus import MessageBus
from app.settings.env import Environment
from app.utils.web_socket_utils import require_initialized


class PolymarketWebSocketClient(PolymBaseClient):
    """
    Handles connection, subscription, and data normalization for Polymarket.
    Publishes standardized Domain Events to the Message Bus.
    """
    MARKET_PATH = "/market"
    PLATFORM_NAME = "polymarket"

    def __init__(
        self,
        *,
        polym_wallet_pk: str | None = None,
        polym_clob_api_key: str | None = None,
        environment: Environment = Environment.DEMO,
        connection_timeout_seconds: int = 10,
    ) -> None:
        super().__init__(
            polym_wallet_pk=polym_wallet_pk,
            polym_clob_api_key=polym_clob_api_key,
            environment=environment.value,
        )
        self.asset_ids: Optional[List[str]] = None
        self.connection_timeout_seconds = connection_timeout_seconds
        self.bus: Optional[MessageBus] = None
        # Keyed by asset_id (token_id, decimal string). Used for `book` and
        # `last_trade_price` events which identify the OUTCOME directly.
        self.market_map: Optional[Dict[str, Dict[str, str]]] = None
        # Keyed by condition_id (hex, 0x...). Used for `price_change` events
        # which identify the MARKET (not the outcome). Side routing within a
        # price_change frame is done per-entry via each change's own asset_id.
        self.condition_id_map: Optional[Dict[str, str]] = None

        # internal state from your version
        self._ws: Optional[WebSocketClientProtocol] = None
        self.logger = logging.getLogger(__name__)

    def set_market_config(self, markets_config: List[Dict[str, str]]) -> None:
        new_map: Dict[str, Dict[str, str]] = {}
        new_cid_map: Dict[str, str] = {}
        for m in markets_config:
            if 'polymarket_yes_token_id' in m:
                new_map[m['polymarket_yes_token_id']] = {'id': m['id'], 'outcome': 'YES'}
            if 'polymarket_no_token_id' in m:
                new_map[m['polymarket_no_token_id']] = {'id': m['id'], 'outcome': 'NO'}
            cid = m.get('polymarket_condition_id')
            if cid:
                new_cid_map[cid] = m['id']
        self.market_map = new_map
        self.condition_id_map = new_cid_map

    def set_asset_ids(self, asset_ids: List[str]) -> None:
        self.asset_ids = asset_ids

    def set_message_bus(self, bus: MessageBus) -> None:
        """Sets the message bus for publishing events."""
        self.bus = bus

    async def _process_and_publish_event(self, data: Dict[str, Any]):
        """Parses a raw message, creates a domain event, and publishes it to the bus.

        Polymarket's WSS uses TWO different identifier conventions across
        event types, and the routing path differs accordingly:

          * `book` and `last_trade_price` carry a top-level `asset_id`
            (decimal token_id) that identifies BOTH the market and the leg
            (YES vs NO). Look these up directly in `market_map`.

          * `price_change` carries a top-level `market` (condition_id, hex)
            that identifies ONLY the market. Each entry inside
            `price_changes[]` carries its own `asset_id` (token_id) which is
            what identifies the leg. Per-entry asset_id MUST be used for
            side routing — applying a frame-level outcome to all entries
            would silently corrupt the book.
        """
        if not self.bus:
            self.logger.error("[Polymarket] Message bus not set on PolymarketWebSocketClient")
            return

        event_type = data.get("event_type")

        if event_type == "price_change":
            await self._handle_price_change(data)
            return

        # Token-keyed events (book, last_trade_price): identify outcome by
        # the top-level asset_id. NOTE: do NOT fall back to `data.get("market")`
        # here — that field is condition_id, not token_id, and falling back
        # was the source of the "all price_change frames silently dropped" bug.
        asset_id = data.get("asset_id")
        if not asset_id or not self.market_map or asset_id not in self.market_map:
            _record_frame("polymarket", "dropped_unknown_asset",
                          event_type=event_type, asset_id=asset_id)
            return

        _record_frame("polymarket", "accepted_event",
                      event_type=event_type, asset_id=asset_id)

        market_info = self.market_map[asset_id]
        common_market_id = market_info['id']
        outcome = market_info['outcome']

        assert outcome in ('YES', 'NO'), f"Invalid outcome '{outcome}' found in market_map"

        try:
            if event_type == "book":
                msg = PolyBookMessage.model_validate(data)
                bids = [PriceLevelData(price=Decimal(level.price), size=Decimal(level.size)) for level in msg.bids]
                asks = [PriceLevelData(price=Decimal(level.price), size=Decimal(level.size)) for level in msg.asks]
                event = OrderBookSnapshotReceived(
                    platform=Platform.POLYMARKET, market_id=common_market_id, outcome=outcome, bids=bids, asks=asks
                )
                await self.bus.publish(event)
            # last_trade_price has no order-book side-effect under the current
            # design; accept the frame (for visibility / counters) but do not
            # mutate any book. Preserves the pre-fix behaviour for that path.
        except ValidationError as e:
            self.logger.debug(f"[Polymarket] Ignoring Polymarket message due to validation error: {e}")
        except Exception:
            self.logger.exception(f"[Polymarket] Failed to process Polymarket message: {data}")

    async def _handle_price_change(self, data: Dict[str, Any]) -> None:
        """Handle a `price_change` frame.

        Frame identifies the market by `market` (condition_id, hex). Each
        entry in `price_changes[]` carries its own `asset_id` (token_id) which
        is what determines the leg (YES vs NO). The routing MUST be per-entry;
        a frame-level outcome would silently corrupt books.
        """
        condition_id = data.get("market")
        if not condition_id or not self.condition_id_map or condition_id not in self.condition_id_map:
            _record_frame("polymarket", "dropped_unknown_asset",
                          event_type="price_change", asset_id=condition_id)
            return

        expected_market_id = self.condition_id_map[condition_id]

        try:
            msg = PolyPriceChangeMessage.model_validate(data)
        except ValidationError as e:
            self.logger.debug(f"[Polymarket] Ignoring price_change due to validation error: {e}")
            return

        for change in msg.price_changes:
            # Per-entry asset_id → outcome lookup. If the token isn't tracked,
            # warn loudly (not silent-skip) — that indicates either an
            # unexpected Polymarket wire change or a market_map setup gap, and
            # silent skipping here would let the book drift undetected.
            token_info = (self.market_map or {}).get(change.asset_id)
            if not token_info:
                _record_frame("polymarket", "price_change_unknown_token",
                              event_type="price_change", asset_id=change.asset_id,
                              note=f"condition_id={condition_id}")
                self.logger.warning(
                    "[Polymarket] price_change entry asset_id=%s not in market_map "
                    "(frame condition_id=%s, market=%s). Skipping this entry; "
                    "the corresponding book may drift until the next book snapshot.",
                    change.asset_id, condition_id, expected_market_id,
                )
                continue

            # Defence-in-depth: if a token_id resolves to a DIFFERENT common
            # market than the condition_id did, that is a wire anomaly. Refuse
            # to apply rather than corrupt a book belonging to a different
            # market.
            if token_info['id'] != expected_market_id:
                _record_frame("polymarket", "price_change_market_mismatch",
                              event_type="price_change", asset_id=change.asset_id,
                              note=f"frame_market={expected_market_id} token_market={token_info['id']}")
                self.logger.error(
                    "[Polymarket] price_change entry asset_id=%s resolves to market "
                    "%s but frame condition_id=%s resolves to market %s. Refusing "
                    "to apply this entry.",
                    change.asset_id, token_info['id'], condition_id, expected_market_id,
                )
                continue

            outcome = token_info['outcome']
            assert outcome in ('YES', 'NO'), f"Invalid outcome '{outcome}' in market_map"

            _record_frame("polymarket", "accepted_event",
                          event_type="price_change", asset_id=change.asset_id)

            event = OrderBookDeltaReceived(
                platform=Platform.POLYMARKET,
                market_id=expected_market_id,
                outcome=outcome,
                side=SIDES.BUY if change.side == "BUY" else SIDES.SELL,
                price=Decimal(change.price),
                size=Decimal(change.size),
            )
            await self.bus.publish(event)

    @require_initialized
    async def connect_forever(self, channel_path: str) -> None:
        """Run forever, reconnecting on any failure."""
        host = self.CLOB_WS_BASE_URL.rstrip("/") + channel_path
        self.logger.info(f"[Polymarket] Attempting connection to {host}")
        while True:
            try:
                async with websockets.connect(host, ping_interval=15, ping_timeout=10) as ws:
                    self.logger.info("[Polymarket] Polymarket websocket connected")
                    self._ws = ws
                    if self.asset_ids:
                        await self._ws.send(json.dumps({"assets_ids": self.asset_ids, "type": "market"}))
                        self.logger.info(f"[Polymarket] Sent subscription request for asset IDs: {self.asset_ids}")
                    await self._handle_subscription_confirmation() # Wait for confirmation
                    self.logger.info("[Polymarket] Beginning listening process")
                    await self._listen() # Start listening for data
            except RuntimeError as e:
                self.logger.error(f"[Polymarket] Initialization error: {e}. The orchestrator may not have configured the client correctly. Retrying...")
            except Exception as exc:
                self.logger.error("[Polymarket] Polymarket WS error %s; reconnecting in 3 seconds", exc, exc_info=True)
            finally:
                self._ws = None
                await asyncio.sleep(3)

    @require_initialized
    async def _handle_subscription_confirmation(self) -> None:
        """Handles initial snapshot messages on subscription."""
        if not self._ws: return
        try:
            # Wait for the first message, which should be the confirmation.
            sub_message_raw = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
            messages = json.loads(sub_message_raw)

            if isinstance(messages, dict):
                messages = [messages]

            for data in messages:
                await self._process_and_publish_event(data)
        except asyncio.TimeoutError:
            self.logger.warning("[Polymarket] Did not receive subscription confirmation from Polymarket within 10 seconds.")
        except Exception as e:
            self.logger.error(f"[Polymarket] Error processing subscription confirmation: {e}", exc_info=True)

    @require_initialized
    async def _listen(self) -> None:
        """Listen for data messages and emit domain events."""
        assert self._ws is not None
        self.logger.info("[Polymarket] Beginning listening process")
        async for raw_message in self._ws:
            if raw_message in {"PING", "PONG"}: continue
            _record_frame("polymarket", "raw_frame")
            try:
                messages = json.loads(raw_message)

                if isinstance(messages, dict):
                    messages = [messages]

                for data in messages:
                    await self._process_and_publish_event(data)

            except json.JSONDecodeError:
                self.logger.warning(f"[Polymarket] Received non-JSON message: {raw_message}")