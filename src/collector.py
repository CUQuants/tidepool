"""
TidePool Data Collector
Real-time orderbook and trade data collector for the TidePool dataset.

**The connection loop.** ``websockets`` owns the connection to Kraken's public
WebSocket v2: ``async for conn in connect(url)`` gives a new connection each
time the last one is gone, with backoff between failed attempts, and its pings
close a socket that is open but dead. The books live for one connection only:

    connected → subscribe book and trade → store frames in arrival order
    dropped   → open gaps → clear the books → reconnect → subscribe again

Kraken sends a fresh snapshot on every book subscribe, so a new connection
always starts from a correct book. A dropped connection never ends the process.

**The maintenance pass**, once a second while connected, does the work that
depends on time: it closes a connection with no frame for
``heartbeat_timeout`` (Kraken sends a heartbeat every second), resubscribes
the book every ``snapshot_interval_seconds``, and resubscribes a book that
lost rows to a full queue.

**Gaps.** A gap is a span where one stream (``book`` or ``trade``) of one
symbol has missing or untrusted data. Each symbol and stream has at most one
open gap, and the earliest start wins. A gap opens on start, disconnect, resync and dropped
rows. A book gap closes when the next snapshot is stored. A trade gap closes
when Kraken confirms the trade subscription or a trade is stored. Each closed
gap becomes one ``gap`` row.

Every row carries ``run_id``, which names one collector process. ``seq``
counts messages within a run.
"""
import asyncio
import json
import logging
import secrets
import time
from collections.abc import AsyncIterable, Callable
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from websockets.exceptions import ConnectionClosed

from src.storage.hot_writer import BOOK_DEPTH, HotParquetWriter, parse_ts_ns

logger = logging.getLogger(__name__)

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"
MAINTAIN_INTERVAL_S = 1.0
RECONNECT_PAUSE_S = 1.0                   # After a drop; websockets reconnects at once otherwise

BOOK, TRADE = "book", "trade"

# (url, note_error) → connections, one per (re)connect. ``note_error`` is told
# about every failed attempt.
Connect = Callable[[str, Callable[[Exception], None]], AsyncIterable[Any]]


def ws_connect(url: str, note_error: Callable[[Exception], None]) -> AsyncIterable[Any]:
    """``websockets``' own reconnecting iterator (websockets 14+). Its default
    decides what is worth retrying (network errors, 5xx). Anything else ends the
    iteration, and the collector retries after ``reconnect_interval``."""
    from websockets.asyncio.client import connect, process_exception

    def classify(exc: Exception) -> Optional[Exception]:
        note_error(exc)
        return process_exception(exc)

    return connect(url, open_timeout=10, ping_interval=20, ping_timeout=20, process_exception=classify)


def new_run_id() -> str:
    """Start time plus a random suffix, so run ids sort by time and are unique."""
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


class TidePoolCollector:
    """
    TidePool Data Collector
    Real-time orderbook and trade data collector for the TidePool dataset.
    Maintains full orderbook state in memory.
    """
    def __init__(self, markets: list[str], storage_cfg: Optional[Dict[str, Any]] = None,
                 kraken_cfg: Optional[Dict[str, Any]] = None, *, connect: Connect = ws_connect):
        storage_cfg = storage_cfg or {}
        kraken_cfg = kraken_cfg or {}
        self.markets = markets
        self.url = kraken_cfg.get('ws_url', KRAKEN_WS_URL)
        self.retry_s = kraken_cfg.get('reconnect_interval', 30)
        self.heartbeat_timeout_s = kraken_cfg.get('heartbeat_timeout', 60)
        self.run_id = new_run_id()
        self._connect = connect

        # Dictionary to store full orderbook state for each symbol
        # Format: {symbol: {'bids': {price: qty}, 'asks': {price: qty}}}
        self.orderbooks: Dict[str, Dict[str, Dict[float, float]]] = {}

        # Rows go websocket handler -> queue -> hot-tier Parquet writer task
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=storage_cfg.get('queue_maxsize', 200_000))
        self.writer = HotParquetWriter(
            self.queue,
            base_dir=storage_cfg.get('hot_dir', 'data/hot'),
            roll_interval_s=storage_cfg.get('roll_interval_seconds', 60),
            max_rows=storage_cfg.get('max_rows_per_file', 500_000),
            compression=storage_cfg.get('compression', 'lz4'),
        )
        self.dropped_rows = 0
        self._last_drop_log = 0.0

        # Every resync re-requests the full book in case updates were missed
        self.snapshot_interval_s = storage_cfg.get('snapshot_interval_seconds', 300)
        # Incremented per websocket data message; orders book rows for replay
        self.seq = 0

        # (symbol, stream) → (kind, start_ts, detail) for each open gap
        self.gaps: Dict[Tuple[str, str], Tuple[str, Optional[int], Optional[str]]] = {}
        for symbol in markets:
            for stream in (BOOK, TRADE):
                self._open_gap(symbol, stream, 'start', None)

        # Per connection (reset by _session)
        self._last_frame_ns = 0
        self._close_reason: Optional[str] = None
        self._requests: Dict[int, Tuple[str, str]] = {}   # req_id → (method, channel)
        self._next_req_id = 0
        self._next_resync = 0.0
        self._needs_resync: set[str] = set()

    # -- the connection loop ------------------------------------------------------

    async def run(self) -> None:
        """Collect until cancelled. A dropped connection is reconnected, not fatal."""
        self.writer.start()
        try:
            while True:
                try:
                    async for conn in self._connect(self.url, self._connect_failed):
                        await self._session(conn)
                        await asyncio.sleep(RECONNECT_PAUSE_S)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # websockets judged it not worth retrying (for example HTTP 403 or a bad URL)
                    logger.error(f"Connection to {self.url} gave up ({type(exc).__name__}: {exc}), "
                                 f"retrying in {self.retry_s}s")
                    await asyncio.sleep(self.retry_s)
        finally:
            for (symbol, stream), (kind, start_ts, detail) in list(self.gaps.items()):
                self._write_gap(symbol, stream, kind, start_ts, None, detail)
            self.gaps.clear()
            await self.writer.stop()

    def _connect_failed(self, exc: Exception) -> None:
        logger.warning(f"Connect to {self.url} failed, websockets will retry: {type(exc).__name__}: {exc}")

    async def _session(self, conn: Any) -> None:
        """One connection, from open to close. Returns when it's gone."""
        self.orderbooks.clear()
        self._requests.clear()
        self._needs_resync.clear()
        self._close_reason = None
        self._last_frame_ns = time.time_ns()
        self._next_resync = time.monotonic() + (self.snapshot_interval_s or 0)
        logger.info(f"Connected to {self.url} (run {self.run_id})")
        maintain = asyncio.create_task(self._maintain(conn))
        reason = "closed by Kraken"
        try:
            await self._send(conn, 'subscribe', BOOK, self.markets)
            await self._send(conn, 'subscribe', TRADE, self.markets)
            async for raw in conn:
                recv_ts = time.time_ns()
                self._last_frame_ns = recv_ts
                problem = await self.handle_message(raw, recv_ts)
                if problem is not None:
                    reason = problem
                    await conn.close()    # Start over on a fresh connection
                    break
        except asyncio.CancelledError:
            raise
        except ConnectionClosed as exc:
            reason = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            # A frame we failed to store leaves the book in doubt, so start over too
            logger.exception("Error while reading the connection")
            reason = f"{type(exc).__name__}: {exc}"
        finally:
            maintain.cancel()
        reason = self._close_reason or reason
        for symbol in self.markets:
            for stream in (BOOK, TRADE):
                self._open_gap(symbol, stream, 'disconnect', self._last_frame_ns, reason)
        logger.warning(f"Connection lost ({reason}), reconnecting")

    async def _maintain(self, conn: Any) -> None:
        while True:
            await asyncio.sleep(MAINTAIN_INTERVAL_S)
            try:
                await self._maintain_once(conn)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Maintenance pass failed")

    async def _maintain_once(self, conn: Any) -> None:
        silent_s = (time.time_ns() - self._last_frame_ns) / 1e9
        if self.heartbeat_timeout_s and silent_s > self.heartbeat_timeout_s:
            self._close_reason = f"no frame for {silent_s:.1f}s"
            await conn.close()
            return
        if self.snapshot_interval_s and self.snapshot_interval_s > 0 and time.monotonic() >= self._next_resync:
            self._next_resync = time.monotonic() + self.snapshot_interval_s
            self._needs_resync.clear()
            await self.resync(conn, self.markets)
            # A trade feed Kraken hasn't confirmed since the connection opened: ask again
            unconfirmed = [s for s in self.markets if self.gaps.get((s, TRADE), ('',))[0] in ('start', 'disconnect')]
            if unconfirmed:
                await self._send(conn, 'subscribe', TRADE, unconfirmed)
        elif self._needs_resync:
            symbols = sorted(self._needs_resync)
            self._needs_resync.clear()
            await self.resync(conn, symbols)

    async def resync(self, conn: Any, symbols: list[str]) -> None:
        """Unsubscribe and subscribe the book again, so Kraken sends a fresh snapshot.
        Kraken v2 can't send a snapshot on a live subscription."""
        now = time.time_ns()
        for symbol in symbols:
            self._open_gap(symbol, BOOK, 'resync', now)
        logger.info(f"Resyncing order books from a fresh snapshot: {', '.join(symbols)}")
        await self._send(conn, 'unsubscribe', BOOK, symbols)
        await self._send(conn, 'subscribe', BOOK, symbols)

    async def _send(self, conn: Any, method: str, channel: str, symbols: list[str]) -> None:
        params: Dict[str, Any] = {'channel': channel, 'symbol': list(symbols)}
        if channel == BOOK:
            # Kraken finds a book subscription by its depth, so unsubscribe sends it too
            params['depth'] = BOOK_DEPTH
        self._next_req_id += 1
        self._requests[self._next_req_id] = (method, channel)
        await conn.send(json.dumps({'method': method, 'params': params, 'req_id': self._next_req_id}))

    # -- inbound frames -----------------------------------------------------------

    async def handle_message(self, message, recv_ts: Optional[int] = None) -> Optional[str]:
        """
        Store one frame. Returns why the connection should be dropped, if it should.
        """
        recv_ts = recv_ts or time.time_ns()
        if isinstance(message, (str, bytes)):
            try:
                data = json.loads(message)
            except ValueError:
                return f"unreadable frame: {message[:100]!r}"
        else:
            data = message
        if not isinstance(data, dict):
            return None

        if 'method' in data:
            self._on_reply(data, recv_ts)
        elif 'channel' in data and 'type' in data and 'data' in data:
            self.seq += 1
            if data['type'] == 'snapshot':
                self.process_snapshot_message(data, recv_ts)
            elif data['type'] == 'update':
                self.process_update_message(data, recv_ts)
        # Anything else (heartbeat) only counts as a frame for the silence check
        return None

    def _on_reply(self, data: dict, recv_ts: int) -> None:
        """Kraken answers a request once per symbol."""
        method, channel = self._requests.get(data.get('req_id'), (data.get('method'), ''))
        result = data.get('result') or {}
        symbol = result.get('symbol') or data.get('symbol')
        if data.get('success'):
            if method == 'subscribe' and result.get('channel', channel) == TRADE and symbol:
                self._close_gap(symbol, TRADE, recv_ts)
            return
        error = data.get('error', 'unknown error')
        if method == 'unsubscribe':
            # Expected for a market whose subscribe was refused; the subscribe after it tries again
            logger.warning(f"Kraken refused unsubscribe {channel} {symbol}: {error}")
        else:
            logger.error(f"Kraken refused {method} {channel} {symbol}: {error}")

    def process_snapshot_message(self, message: dict, recv_ts: int):
        """
        Process snapshot messages: replace the book state and store the full book
        """
        if message['channel'] != BOOK:
            return    # Trade snapshots (Kraken's last 50 trades) are not stored
        for item in message['data']:
            symbol = item.get('symbol', '')

            # A snapshot replaces whatever we had, including state drifted by missed updates
            book: Dict[str, Dict[float, float]] = {'bids': {}, 'asks': {}}
            for key in ('bids', 'asks'):
                for level in item.get(key, []):
                    book[key][float(level['price'])] = float(level['qty'])
            self.orderbooks[symbol] = book
            logger.debug(f"Initialized orderbook for {symbol} with {len(book['bids'])} bids "
                         f"and {len(book['asks'])} asks")

            if self.enqueue_orderbook_snapshot(symbol, item, recv_ts):
                self._close_gap(symbol, BOOK, recv_ts)

    def process_update_message(self, message: dict, recv_ts: int):
        """
        Process update messages and apply changes to orderbook state
        """
        channel = message['channel']
        for item in message['data']:
            symbol = item.get('symbol', '')
            if channel == BOOK:
                self.update_orderbook(symbol, item, recv_ts)
            elif channel == TRADE:
                self.enqueue_trade(symbol, item, recv_ts)

    def update_orderbook(self, symbol: str, orderbook_item: dict, recv_ts: int):
        """
        Apply incremental changes to the orderbook state and store each change as a delta row
        """
        # Ensure we have an orderbook for this symbol
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = {'bids': {}, 'asks': {}}
            logger.warning(f"No snapshot received for {symbol}, initializing empty orderbook")

        ts = self._exchange_ts(orderbook_item, recv_ts)
        base = {'ts': ts, 'recv_ts': recv_ts, 'seq': self.seq, 'symbol': symbol,
                'checksum': orderbook_item.get('checksum')}

        for side, key in (('bid', 'bids'), ('ask', 'asks')):
            levels = self.orderbooks[symbol][key]
            for level in orderbook_item.get(key, []):
                price = float(level['price'])
                qty = float(level['qty'])

                if qty == 0:
                    # Remove this price level
                    levels.pop(price, None)
                else:
                    # Update/add this price level
                    levels[price] = qty

                self._enqueue('book_delta', {**base, 'side': side, 'price': price, 'qty': qty})

        # Kraken never sends qty 0 for levels pushed past the subscribed depth, so trim them here
        self._truncate(self.orderbooks[symbol])

    @staticmethod
    def _truncate(orderbook: Dict[str, Dict[float, float]]):
        """Drop levels beyond BOOK_DEPTH on each side."""
        for key, reverse in (('bids', True), ('asks', False)):
            levels = orderbook[key]
            if len(levels) > BOOK_DEPTH:
                for price in sorted(levels, reverse=reverse)[BOOK_DEPTH:]:
                    del levels[price]

    def _enqueue(self, stream: str, row: dict) -> bool:
        """
        Hand a row to the Parquet writer without blocking the websocket loop.
        If the queue is full (writer can't keep up), drop the row and open a gap.
        A book that lost a row is resynced, so the stored book is correct again.
        """
        row['run_id'] = self.run_id
        try:
            self.queue.put_nowait((stream, row))
            return True
        except asyncio.QueueFull:
            self.dropped_rows += 1
            now = time.monotonic()
            if now - self._last_drop_log >= 10:
                logger.warning(f"Write queue full, dropped {self.dropped_rows} rows so far")
                self._last_drop_log = now
            symbol = row['symbol']
            if stream == 'trade':
                self._open_gap(symbol, TRADE, 'dropped_rows', row['recv_ts'])
            else:
                self._open_gap(symbol, BOOK, 'dropped_rows', row['recv_ts'])
                self._needs_resync.add(symbol)
            return False

    def _exchange_ts(self, item: dict, recv_ts: int) -> int:
        """
        Kraken's timestamp for this item in ns. Kraken sends one on every book and trade
        message, so a missing or unparseable one is logged and replaced with recv_ts.
        """
        try:
            return parse_ts_ns(item['timestamp'])
        except (KeyError, TypeError, ValueError):
            logger.warning(f"Missing/bad exchange timestamp {item.get('timestamp')!r} for "
                           f"{item.get('symbol')}, using receive time")
            return recv_ts

    def enqueue_orderbook_snapshot(self, symbol: str, item: dict, recv_ts: int) -> bool:
        """
        Enqueue current top 25 bids and asks as a single row. Returns whether it was stored.
        """
        if symbol not in self.orderbooks:
            return False

        orderbook = self.orderbooks[symbol]

        # Get top 25 bids (highest prices first) and top 25 asks (lowest prices first)
        top_bids = sorted(orderbook['bids'].keys(), reverse=True)[:BOOK_DEPTH]
        top_asks = sorted(orderbook['asks'].keys())[:BOOK_DEPTH]

        row = {'ts': self._exchange_ts(item, recv_ts), 'recv_ts': recv_ts, 'seq': self.seq,
               'symbol': symbol, 'checksum': item.get('checksum')}

        # Level 1 is the best price; missing levels stay null
        for i, price in enumerate(top_bids, start=1):
            row[f'bid_{i}_price'] = price
            row[f'bid_{i}_qty'] = orderbook['bids'][price]
        for i, price in enumerate(top_asks, start=1):
            row[f'ask_{i}_price'] = price
            row[f'ask_{i}_qty'] = orderbook['asks'][price]

        return self._enqueue('book_snapshot', row)

    def enqueue_trade(self, symbol: str, trade_item: dict, recv_ts: int):
        """
        Enqueue a trade row
        """
        ts = self._exchange_ts(trade_item, recv_ts)
        price = trade_item.get('price')
        qty = trade_item.get('qty')
        trade_id = trade_item.get('trade_id')

        stored = self._enqueue('trade', {
            'ts': ts,
            'recv_ts': recv_ts,
            'symbol': symbol,
            'side': trade_item.get('side'),
            'price': float(price) if price is not None else None,
            'qty': float(qty) if qty is not None else None,
            'ord_type': trade_item.get('ord_type'),
            'trade_id': int(trade_id) if trade_id is not None else None,
        })
        if stored:
            self._close_gap(symbol, TRADE, recv_ts)

    # -- gaps ---------------------------------------------------------------------

    def _open_gap(self, symbol: str, stream: str, kind: str, start_ts: Optional[int],
                  detail: Optional[str] = None) -> None:
        """Open a gap unless one is open already: the earliest start wins."""
        self.gaps.setdefault((symbol, stream), (kind, start_ts, detail))

    def _close_gap(self, symbol: str, stream: str, end_ts: int) -> None:
        gap = self.gaps.pop((symbol, stream), None)
        if gap is not None:
            kind, start_ts, detail = gap
            self._write_gap(symbol, stream, kind, start_ts, end_ts, detail)

    def _write_gap(self, symbol: str, stream: str, kind: str, start_ts: Optional[int],
                   end_ts: Optional[int], detail: Optional[str]) -> None:
        # Straight to the writer: a gap row must not be lost to a full queue
        self.writer.append('gap', {'ts': time.time_ns(), 'run_id': self.run_id, 'symbol': symbol,
                                   'stream': stream, 'kind': kind, 'start_ts': start_ts,
                                   'end_ts': end_ts, 'detail': detail})
        span = f"{(end_ts - start_ts) / 1e9:.3f}s" if start_ts and end_ts else "start unknown or open at shutdown"
        # A resync gap is routine (every snapshot_interval_seconds), so it logs at debug
        level = logging.INFO if kind in ('disconnect', 'dropped_rows') else logging.DEBUG
        logger.log(level, f"Gap {kind} {symbol} {stream}: {span}")
