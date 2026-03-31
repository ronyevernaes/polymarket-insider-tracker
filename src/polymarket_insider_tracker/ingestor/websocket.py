"""Trade stream poller for Polymarket trade events.

Polls the Polymarket Data API for recent trades on a short interval,
replacing the previous WebSocket approach which lacked wallet-level data.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

import httpx

from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)

# Constants
TRADES_API_URL = "https://data-api.polymarket.com/trades"
DEFAULT_POLL_INTERVAL = 10  # seconds between polls
DEFAULT_BATCH_LIMIT = 50    # max trades per poll (API maximum)
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 5.0   # seconds


class ConnectionState(Enum):
    """Trade stream connection states."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"


@dataclass
class StreamStats:
    """Statistics about the trade stream."""

    trades_received: int = 0
    reconnect_count: int = 0
    last_trade_time: float | None = None
    connected_since: float | None = None
    last_error: str | None = None


class TradeStreamError(Exception):
    """Base exception for trade stream errors."""


class ConnectionError(TradeStreamError):
    """Raised when the trade stream cannot be started."""


TradeCallback = Callable[[TradeEvent], Awaitable[None]]
StateCallback = Callable[[ConnectionState], Awaitable[None]]


class TradeStreamHandler:
    """Polls Polymarket Data API for live trade events.

    Periodically fetches new trades since the last poll using the
    data-api.polymarket.com/trades endpoint, which provides full trade
    data including wallet addresses needed for insider detection.

    The same on_trade callback interface is preserved so the rest of
    the pipeline requires no changes.

    Example:
        >>> async def on_trade(trade: TradeEvent):
        ...     print(f"Trade: {trade.side} {trade.size} @ {trade.price}")
        ...
        >>> handler = TradeStreamHandler(on_trade=on_trade)
        >>> await handler.start()  # Blocks until stop() is called
    """

    def __init__(
        self,
        on_trade: TradeCallback,
        *,
        host: str = TRADES_API_URL,  # kept for API compatibility, unused
        on_state_change: StateCallback | None = None,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        batch_limit: int = DEFAULT_BATCH_LIMIT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        event_filter: str | None = None,
        market_filter: str | None = None,
    ) -> None:
        """Initialize the trade stream handler.

        Args:
            on_trade: Async callback invoked for each trade event.
            host: Unused, kept for API compatibility with previous WS version.
            on_state_change: Optional callback for connection state changes.
            poll_interval: Seconds between API polls (default 10).
            batch_limit: Max trades to fetch per poll (default 50).
            max_retries: Retries on consecutive API failures before giving up.
            retry_delay: Seconds to wait between retries on failure.
            event_filter: Optional event slug to filter trades by event.
            market_filter: Optional market condition ID to filter trades.
        """
        self._on_trade = on_trade
        self._on_state_change = on_state_change
        self._poll_interval = poll_interval
        self._batch_limit = batch_limit
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._event_filter = event_filter
        self._market_filter = market_filter

        self._state = ConnectionState.DISCONNECTED
        self._stats = StreamStats()
        self._running = False
        self._last_ts: int = int(time.time())  # only process trades from now on
        self._seen_hashes: set[str] = set()    # dedup within current poll window
        self._http = httpx.AsyncClient(timeout=15.0)

    @property
    def state(self) -> ConnectionState:
        """Current connection state."""
        return self._state

    @property
    def stats(self) -> StreamStats:
        """Stream statistics."""
        return self._stats

    async def _set_state(self, new_state: ConnectionState) -> None:
        """Update state and notify callback."""
        if self._state != new_state:
            old_state = self._state
            self._state = new_state
            logger.info("Connection state: %s -> %s", old_state.value, new_state.value)
            if self._on_state_change:
                try:
                    await self._on_state_change(new_state)
                except Exception as e:
                    logger.error("Error in state change callback: %s", e)

    def _build_params(self) -> dict[str, Any]:
        """Build query params for the trades API request."""
        params: dict[str, Any] = {
            "limit": self._batch_limit,
            "start_ts": self._last_ts,
        }
        if self._event_filter:
            params["event_slug"] = self._event_filter
        if self._market_filter:
            params["market"] = self._market_filter
        return params

    async def _fetch_trades(self) -> list[dict]:
        """Fetch new trades from the Data API since last poll."""
        params = self._build_params()
        resp = await self._http.get(TRADES_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])

    async def _poll_once(self) -> None:
        """Run one poll cycle: fetch trades and fire callbacks."""
        raw_trades = await self._fetch_trades()

        if not raw_trades:
            return

        # Trades come newest-first; process oldest-first so callbacks fire in order
        new_trades = []
        for raw in reversed(raw_trades):
            tx_hash = str(raw.get("transactionHash", ""))
            if tx_hash and tx_hash in self._seen_hashes:
                continue
            ts = int(raw.get("timestamp", 0))
            if ts <= self._last_ts and tx_hash in self._seen_hashes:
                continue
            new_trades.append(raw)

        if not new_trades:
            return

        for raw in new_trades:
            try:
                trade = TradeEvent.from_websocket_message(raw)
                tx_hash = str(raw.get("transactionHash", ""))
                if tx_hash:
                    self._seen_hashes.add(tx_hash)

                self._stats.trades_received += 1
                self._stats.last_trade_time = time.time()

                logger.debug(
                    "Trade: %s %s @ %s on %s",
                    trade.side,
                    trade.size,
                    trade.price,
                    trade.market_slug,
                )

                await self._on_trade(trade)

            except Exception as e:
                logger.error("Error processing trade: %s", e)

        # Advance cursor to the timestamp of the most recent trade
        latest_ts = max(int(r.get("timestamp", 0)) for r in new_trades)
        if latest_ts > self._last_ts:
            self._last_ts = latest_ts

        # Bound the seen_hashes set to avoid unbounded memory growth
        if len(self._seen_hashes) > 10_000:
            self._seen_hashes.clear()

    async def start(self) -> None:
        """Begin polling for trades. Blocks until stop() is called.

        Raises:
            ConnectionError: If initial API check fails.
        """
        if self._running:
            logger.warning("Handler already running")
            return

        self._running = True
        await self._set_state(ConnectionState.CONNECTING)

        # Verify the API is reachable before declaring connected
        consecutive_failures = 0
        try:
            await self._fetch_trades()
            consecutive_failures = 0
        except Exception as e:
            logger.error("Initial trade fetch failed: %s", e)
            self._stats.last_error = str(e)
            raise ConnectionError(f"Failed to connect to {TRADES_API_URL}: {e}") from e

        await self._set_state(ConnectionState.CONNECTED)
        self._stats.connected_since = time.time()
        logger.info("Trade stream polling started (interval=%ds)", self._poll_interval)

        while self._running:
            await asyncio.sleep(self._poll_interval)
            if not self._running:
                break

            try:
                await self._poll_once()
                if consecutive_failures > 0:
                    consecutive_failures = 0
                    await self._set_state(ConnectionState.CONNECTED)
                    self._stats.reconnect_count += 1

            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_failures += 1
                self._stats.last_error = str(e)
                logger.warning(
                    "Poll failed (%d/%d): %s",
                    consecutive_failures,
                    self._max_retries,
                    e,
                )
                if consecutive_failures >= self._max_retries:
                    await self._set_state(ConnectionState.RECONNECTING)
                await asyncio.sleep(self._retry_delay)

        await self._cleanup()

    async def stop(self) -> None:
        """Gracefully stop polling."""
        if not self._running:
            return
        logger.info("Stopping trade stream handler...")
        self._running = False
        await self._cleanup()

    async def _cleanup(self) -> None:
        """Clean up resources."""
        await self._http.aclose()
        await self._set_state(ConnectionState.DISCONNECTED)
        logger.info("Trade stream handler stopped")

    async def __aenter__(self) -> "TradeStreamHandler":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()
