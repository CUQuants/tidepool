"""Shared storage, queue and gap handling for exchange collectors."""

import asyncio
import logging
import secrets
import time
from datetime import datetime, timezone

from src.storage.hot_writer import HotParquetWriter

BOOK, TRADE = 'book', 'trade'
logger = logging.getLogger(__name__)


def new_run_id():
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


class TidePoolCollector:
    def __init__(self, markets, storage_cfg=None):
        storage = storage_cfg or {}
        self.markets = markets
        self.run_id = new_run_id()
        self.orderbooks = {}
        self.queue = asyncio.Queue(maxsize=storage.get('queue_maxsize', 200_000))
        self.writer = HotParquetWriter(
            self.queue, base_dir=storage.get('hot_dir', 'data/hot'),
            roll_interval_s=storage.get('roll_interval_seconds', 60),
            max_rows=storage.get('max_rows_per_file', 500_000),
            compression=storage.get('compression', 'lz4'),
        )
        self.dropped_rows = 0
        self._last_drop_log = 0.0
        self.snapshot_interval_s = storage.get('snapshot_interval_seconds', 300)
        self.seq = 0
        self._needs_resync = set()
        self.gaps = {}
        for symbol in markets:
            for stream in (BOOK, TRADE):
                self._open_gap(symbol, stream, 'start', None)

    def _enqueue(self, stream, row):
        row['run_id'] = self.run_id
        try:
            self.queue.put_nowait((stream, row))
            return True
        except asyncio.QueueFull:
            self.dropped_rows += 1
            now = time.monotonic()
            if now - self._last_drop_log >= 10:
                logger.warning('Write queue full, dropped %s rows so far', self.dropped_rows)
                self._last_drop_log = now
            self._open_gap(row['symbol'], TRADE if stream == 'trade' else BOOK,
                           'dropped_rows', row['recv_ts'])
            if stream != 'trade':
                self._needs_resync.add(row['symbol'])
            return False

    def _open_gap(self, symbol, stream, kind, start_ts, detail=None):
        self.gaps.setdefault((symbol, stream), (kind, start_ts, detail))

    def _close_gap(self, symbol, stream, end_ts):
        gap = self.gaps.pop((symbol, stream), None)
        if gap is not None:
            self._write_gap(symbol, stream, *gap[:2], end_ts, gap[2])

    def _write_gap(self, symbol, stream, kind, start_ts, end_ts, detail):
        # Gap rows bypass the queue so they survive a full queue.
        self.writer.append('gap', {'ts': time.time_ns(), 'run_id': self.run_id,
                                   'symbol': symbol, 'stream': stream, 'kind': kind,
                                   'start_ts': start_ts, 'end_ts': end_ts, 'detail': detail})
        span = f'{(end_ts - start_ts) / 1e9:.3f}s' if start_ts and end_ts else 'start unknown or open at shutdown'
        level = logging.INFO if kind in ('disconnect', 'dropped_rows') else logging.DEBUG
        logger.log(level, 'Gap %s %s %s: %s', kind, symbol, stream, span)

    async def _finish(self):
        for (symbol, stream), (kind, start, detail) in list(self.gaps.items()):
            self._write_gap(symbol, stream, kind, start, None, detail)
        self.gaps.clear()
        await self.writer.stop()
