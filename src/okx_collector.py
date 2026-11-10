"""Record OKX spot books and trades received through the trading gateway."""

import asyncio
import logging
import os
import time
from pathlib import Path

from src.collector_base import BOOK, TRADE, TidePoolCollector
from src.storage.hot_writer import BOOK_DEPTH

logger = logging.getLogger(__name__)


class OKXCollector(TidePoolCollector):
    def __init__(self, markets, storage_cfg=None, okx_cfg=None, *, client_factory=None):
        storage = dict(storage_cfg or {})
        storage['hot_dir'] = str(Path(storage.get('hot_dir', 'data/hot')) / 'okx')
        super().__init__(markets, storage)
        self.okx_cfg = okx_cfg or {}
        self.client_factory = client_factory or self._make_client
        self.exchange_seq = {}
        self.exchange_ts = {}
        self.last_good = {}
        self.client = None
        self._restart_task = None

    def _make_client(self):
        from proxy_client.websocket import ProxyWebSocketClient

        names = ('CUQ_PROXY_URL', 'CUQ_PROXY_API_KEY', 'CUQ_PROXY_SECRET',
                 'CUQ_PROXY_OPERATOR_ID', 'CUQ_PROXY_OPERATOR_NAME')
        values = [os.getenv(name, '').strip() for name in names]
        missing = [name for name, value in zip(names, values) if not value]
        if missing:
            raise ValueError('Missing proxy settings: ' + ', '.join(missing))
        return ProxyWebSocketClient.for_operator(*values, exchange='okx', reconnect=None)

    def _invalidate(self, reason):
        # Keep the gap open until a fresh exchange snapshot arrives.
        for symbol in self.markets:
            for stream in (BOOK, TRADE):
                self._open_gap(symbol, stream, 'disconnect', self.last_good.get((symbol, stream)), reason)
        self.orderbooks.clear()
        self.exchange_seq.clear()
        self.exchange_ts.clear()

    def _restart(self, reason):
        logger.warning('Restarting OKX feed: %s', reason)
        self._invalidate(reason)
        if self.client and (self._restart_task is None or self._restart_task.done()):
            self._restart_task = asyncio.create_task(self.client.stop())

    async def handle_okx(self, message):
        recv_ts = time.time_ns()
        if message.get('event') == 'upstream_status':
            if message.get('status') == 'reconnecting':
                self._restart('gateway upstream disconnected')
            return
        arg = message.get('arg') or {}
        channel, native = arg.get('channel'), arg.get('instId', '')
        symbol = native.replace('-', '/')
        if symbol not in self.markets or channel not in ('books', 'trades'):
            return
        try:
            for item in message.get('data', []):
                ts = int(item['ts']) * 1_000_000
                if channel == 'books':
                    self._book(symbol, message.get('action'), item, ts, recv_ts)
                else:
                    trade_id = int(item['tradeId'])
                    if not 0 <= trade_id < 2**63:
                        raise ValueError('trade ID outside int64 range')
                    stored = self._enqueue('trade', {
                        'ts': ts, 'recv_ts': recv_ts, 'symbol': symbol,
                        'side': item['side'], 'price': float(item['px']),
                        'qty': float(item['sz']), 'ord_type': None, 'trade_id': trade_id,
                    })
                    if stored:
                        self._close_gap(symbol, TRADE, recv_ts)
                        self.last_good[(symbol, TRADE)] = recv_ts
        except (KeyError, IndexError, TypeError, ValueError, OverflowError) as exc:
            self._restart(f'invalid {channel} message: {exc}')

    def _book(self, symbol, action, item, ts, recv_ts):
        seq = int(item['seqId'])
        if action == 'snapshot':
            book = {'bids': {}, 'asks': {}}
            before = None
        elif action == 'update' and symbol in self.orderbooks:
            if int(item['prevSeqId']) != self.exchange_seq[symbol]:
                self._restart(f'book sequence gap for {symbol}')
                return
            book = self.orderbooks[symbol]
            before = self._top_levels(book)
        else:
            # A shared gateway subscription may not send a snapshot to a late joiner.
            return
        for side in ('bids', 'asks'):
            for level in item[side]:
                price, qty = float(level[0]), float(level[1])
                if qty == 0:
                    book[side].pop(price, None)
                else:
                    book[side][price] = qty
            # OKX sends changes for its full 400-level book, not just our top 25.
            book[side] = dict(sorted(book[side].items(), reverse=side == 'bids')[:400])
        self.orderbooks[symbol] = book
        self.exchange_seq[symbol] = seq
        self.exchange_ts[symbol] = ts
        self.seq += 1
        if action == 'snapshot':
            stored = self._write_snapshot(symbol, ts, recv_ts)
        else:
            stored = self._write_deltas(symbol, before, self._top_levels(book), ts, recv_ts)
        if stored:
            self._close_gap(symbol, BOOK, recv_ts)
            self.last_good[(symbol, BOOK)] = recv_ts
        else:
            # A partially written message is untrusted from the last complete one.
            self.gaps[(symbol, BOOK)] = ('dropped_rows', self.last_good.get((symbol, BOOK)), None)
            self._restart('book row dropped')

    @staticmethod
    def _top_levels(book):
        return {side: dict(list(book[side].items())[:BOOK_DEPTH]) for side in ('bids', 'asks')}

    def _write_snapshot(self, symbol, ts, recv_ts):
        row = {'ts': ts, 'recv_ts': recv_ts, 'seq': self.seq, 'symbol': symbol, 'checksum': None}
        for side, levels in self._top_levels(self.orderbooks[symbol]).items():
            for index, (price, qty) in enumerate(levels.items(), 1):
                row[f'{side[:-1]}_{index}_price'] = price
                row[f'{side[:-1]}_{index}_qty'] = qty
        return self._enqueue('book_snapshot', row)

    def _write_deltas(self, symbol, before, after, ts, recv_ts):
        base = {'ts': ts, 'recv_ts': recv_ts, 'seq': self.seq, 'symbol': symbol, 'checksum': None}
        for side in ('bids', 'asks'):
            # Include promotions from deeper levels, even if OKX didn't update them.
            prices = set(before[side]) | set(after[side])
            for price in sorted(prices, reverse=side == 'bids'):
                qty = after[side].get(price, 0.0)
                if before[side].get(price, 0.0) != qty:
                    if not self._enqueue('book_delta', {**base, 'side': side[:-1], 'price': price, 'qty': qty}):
                        return False
        return True

    async def _periodic_snapshots(self):
        while True:
            await asyncio.sleep(self.snapshot_interval_s)
            for symbol in list(self.orderbooks):
                if (symbol, BOOK) in self.gaps:
                    continue
                self.seq += 1
                if not self._write_snapshot(symbol, self.exchange_ts[symbol], time.time_ns()):
                    self._restart('periodic snapshot dropped')
                    break

    async def run(self):
        self.writer.start()
        periodic = (asyncio.create_task(self._periodic_snapshots())
                    if self.snapshot_interval_s and self.snapshot_interval_s > 0 else None)
        try:
            while True:
                self.client = self.client_factory()
                self.client.add_handler('*', self.handle_okx)
                try:
                    await self.client.start()
                    for symbol in self.markets:
                        native = symbol.replace('/', '-')
                        for channel in ('books', 'trades'):
                            await self.client.subscribe(channel, {'instId': native})
                    wait = asyncio.create_task(self.client.run_forever())
                    try:
                        while not wait.done():
                            done, _ = await asyncio.wait([wait], timeout=self.okx_cfg.get('snapshot_timeout', 30))
                            if not done and any(symbol not in self.orderbooks for symbol in self.markets):
                                self._restart('timed out waiting for a fresh book snapshot')
                        await wait
                    finally:
                        if not wait.done():
                            wait.cancel()
                finally:
                    self._invalidate('gateway disconnected')
                    await self.client.stop()
                await asyncio.sleep(self.okx_cfg.get('reconnect_interval', 5))
        finally:
            if periodic:
                periodic.cancel()
                await asyncio.gather(periodic, return_exceptions=True)
            await self._finish()
