"""The collector's connection loop and its gap rows, against a fake Kraken.

``FakeKraken`` hands out connections the way ``websockets``' reconnecting
iterator does. ``FakeConn`` answers requests the way Kraken does: one reply per
symbol, a snapshot after a book subscribe, a refused unsubscribe at a depth
that wasn't subscribed, and "Already subscribed" for a second subscribe.
"""
import asyncio
import json
import os

import pyarrow.dataset as ds
import pytest

import src.collector as collector
from src.collector import TidePoolCollector
from src.storage.hot_writer import BOOK_DEPTH

TS = "2025-09-01T20:11:35.911390Z"
MARKETS = ["BTC/USD", "ETH/USD"]


class FakeConn:
    _CLOSED = object()

    def __init__(self, bids=((100.0, 1.0),)):
        self.sent: list[dict] = []
        self.closed = False
        self.bids = list(bids)
        self.subscribed: dict = {}            # (channel, symbol) → depth
        self._inbox: asyncio.Queue = asyncio.Queue()

    async def send(self, text):
        req = json.loads(text)
        self.sent.append(req)
        method, params = req["method"], req["params"]
        channel, depth = params["channel"], params.get("depth", 10)
        for symbol in params["symbol"]:
            key = (channel, symbol)
            reply = {"method": method, "req_id": req["req_id"], "success": True,
                     "result": {"channel": channel, "symbol": symbol}}
            if method == "unsubscribe":
                if self.subscribed.get(key) != depth and channel == "book":
                    reply = self._refused(req, symbol, f"Subscription with depth {depth} not Found")
                else:
                    self.subscribed.pop(key, None)
            elif key in self.subscribed:
                reply = self._refused(req, symbol, "Already subscribed")
            else:
                self.subscribed[key] = depth
            self.push(reply)
            if method == "subscribe" and channel == "book" and reply["success"]:
                self.push(snapshot(symbol, self.bids))

    @staticmethod
    def _refused(req, symbol, error):
        return {"method": req["method"], "req_id": req["req_id"], "success": False, "symbol": symbol,
                "error": error}

    def push(self, frame):
        self._inbox.put_nowait(frame if isinstance(frame, str) else json.dumps(frame))

    def drop(self):
        self._inbox.put_nowait(self._CLOSED)

    async def close(self):
        self.closed = True
        self.drop()

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._inbox.get()
        if item is self._CLOSED:
            raise StopAsyncIteration
        return item

    def requests(self, method, channel):
        return [r["params"] for r in self.sent if r["method"] == method and r["params"]["channel"] == channel]


class FakeKraken:
    """``Connect`` as websockets behaves: one connection per (re)connect, in the
    order the test queues them. A queued exception ends the iteration, as an
    error websockets won't retry does."""

    def __init__(self):
        self.conns: list[FakeConn] = []
        self._queue: asyncio.Queue = asyncio.Queue()

    def __call__(self, url, note_error):
        return self._attempts()

    async def _attempts(self):
        while True:
            item = await self._queue.get()
            if isinstance(item, Exception):
                raise item
            self.conns.append(item)
            yield item

    def accept(self, **kw) -> FakeConn:
        conn = FakeConn(**kw)
        self._queue.put_nowait(conn)
        return conn

    def give_up(self, exc):
        self._queue.put_nowait(exc)


def snapshot(symbol, bids, asks=((101.0, 1.0),)):
    return {"channel": "book", "type": "snapshot", "data": [{
        "symbol": symbol, "timestamp": TS, "checksum": 1,
        "bids": [{"price": p, "qty": q} for p, q in bids],
        "asks": [{"price": p, "qty": q} for p, q in asks]}]}


def book_update(symbol, bids):
    return {"channel": "book", "type": "update", "data": [{
        "symbol": symbol, "timestamp": TS, "checksum": 2, "bids": [{"price": p, "qty": q} for p, q in bids],
        "asks": []}]}


async def until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("timed out waiting")
        await asyncio.sleep(0.01)


def rows(base, stream):
    path = os.path.join(base, stream)
    files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(".parquet")] if os.path.isdir(path) else []
    return ds.dataset(files, format="parquet").to_table().to_pylist() if files else []


@pytest.fixture(autouse=True)
def fast_loop(monkeypatch):
    monkeypatch.setattr(collector, "RECONNECT_PAUSE_S", 0)
    monkeypatch.setattr(collector, "MAINTAIN_INTERVAL_S", 0.02)


def make(tmp_path, kraken, *, snapshot_interval=0, heartbeat_timeout=0, retry=30, markets=MARKETS):
    return TidePoolCollector(markets, {"hot_dir": str(tmp_path), "roll_interval_seconds": 3600,
                                       "snapshot_interval_seconds": snapshot_interval},
                             {"heartbeat_timeout": heartbeat_timeout, "reconnect_interval": retry},
                             connect=kraken)


async def stop(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def subscribed(c, conn):
    return all((ch, s) in conn.subscribed for ch in ("book", "trade") for s in c.markets) and \
        all(s in c.orderbooks for s in c.markets)


# -- reconnecting ---------------------------------------------------------------

def test_a_dropped_connection_is_reconnected_and_subscribed_again(tmp_path):
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken)
        first = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: subscribed(c, first))

        second = kraken.accept()
        first.drop()
        await until(lambda: subscribed(c, second))
        assert not task.done()
        assert second.requests("subscribe", "book") == [{"channel": "book", "symbol": MARKETS, "depth": BOOK_DEPTH}]
        assert second.requests("subscribe", "trade") == [{"channel": "trade", "symbol": MARKETS}]
        await stop(task)

    asyncio.run(scenario())


def test_a_new_connection_starts_from_its_own_snapshot(tmp_path):
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken, markets=["BTC/USD"])
        first = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: subscribed(c, first))
        first.push(book_update("BTC/USD", [(99.0, 2.0)]))
        await until(lambda: 99.0 in c.orderbooks["BTC/USD"]["bids"])

        second = kraken.accept(bids=((98.0, 3.0),))
        first.drop()
        await until(lambda: subscribed(c, second) and 98.0 in c.orderbooks["BTC/USD"]["bids"])
        assert c.orderbooks["BTC/USD"]["bids"] == {98.0: 3.0}
        await stop(task)

    asyncio.run(scenario())


def test_a_connection_that_gives_up_is_tried_again_after_reconnect_interval(tmp_path):
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken, retry=0.01)
        kraken.give_up(OSError("HTTP 403"))
        conn = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: subscribed(c, conn))
        await stop(task)

    asyncio.run(scenario())


def test_a_silent_connection_is_closed_and_opened_again(tmp_path):
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken, heartbeat_timeout=0.2)
        first = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: subscribed(c, first))
        second = kraken.accept()
        await until(lambda: first.closed and subscribed(c, second))
        await stop(task)

    asyncio.run(scenario())
    (gap, *_) = [g for g in rows(tmp_path, "gap") if g["kind"] == "disconnect"]
    assert gap["detail"].startswith("no frame for")


def test_an_unreadable_frame_starts_a_new_connection(tmp_path):
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken)
        first = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: subscribed(c, first))
        second = kraken.accept()
        first.push("not json")
        await until(lambda: first.closed and subscribed(c, second))
        await stop(task)

    asyncio.run(scenario())
    assert any(g["detail"].startswith("unreadable frame") for g in rows(tmp_path, "gap") if g["kind"] == "disconnect")


# -- gaps and run id -------------------------------------------------------------

def test_a_disconnect_is_one_gap_per_symbol_and_stream(tmp_path):
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken)
        first = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: subscribed(c, first) and not c.gaps)
        second = kraken.accept()
        first.drop()
        await until(lambda: subscribed(c, second) and not c.gaps)
        await stop(task)
        return c.run_id

    run_id = asyncio.run(scenario())
    gaps = rows(tmp_path, "gap")
    by_kind = {}
    for g in gaps:
        by_kind.setdefault(g["kind"], set()).add((g["symbol"], g["stream"]))
    every_pair = {(s, st) for s in MARKETS for st in ("book", "trade")}
    assert by_kind == {"start": every_pair, "disconnect": every_pair}
    assert all(g["run_id"] == run_id for g in gaps)

    starts = [g for g in gaps if g["kind"] == "start"]
    assert all(g["start_ts"] is None and g["end_ts"] is not None for g in starts)

    # A book gap ends at the next stored snapshot: the second connection's
    snaps = {(s["symbol"], s["recv_ts"]) for s in rows(tmp_path, "book_snapshot")}
    for g in gaps:
        assert g["kind"] != "disconnect" or g["start_ts"] < g["end_ts"]
        if g["kind"] == "disconnect" and g["stream"] == "book":
            assert (g["symbol"], g["end_ts"]) in snaps


def test_every_row_carries_the_run_id_and_seq_keeps_counting_across_connections(tmp_path):
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken, markets=["BTC/USD"])
        first = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: subscribed(c, first))
        second = kraken.accept()
        first.drop()
        await until(lambda: subscribed(c, second))
        await stop(task)
        return c.run_id

    run_id = asyncio.run(scenario())
    snaps = rows(tmp_path, "book_snapshot")
    assert len(snaps) == 2 and {s["run_id"] for s in snaps} == {run_id}
    assert snaps[0]["seq"] != snaps[1]["seq"]


def test_stopping_is_not_recorded_as_a_disconnect(tmp_path):
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken)
        conn = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: subscribed(c, conn) and not c.gaps)
        await stop(task)

    asyncio.run(scenario())
    assert {g["kind"] for g in rows(tmp_path, "gap")} == {"start"}


def test_resync_unsubscribes_at_the_subscribed_depth_and_gets_a_fresh_snapshot(tmp_path):
    """Kraken finds a book subscription by its depth. An unsubscribe without it is
    refused, and then the subscribe gets "Already subscribed" and no snapshot."""
    kraken = FakeKraken()

    async def scenario():
        c = make(tmp_path, kraken, snapshot_interval=0.1, markets=["BTC/USD"])
        conn = kraken.accept()
        task = asyncio.create_task(c.run())
        await until(lambda: len(conn.requests("subscribe", "book")) >= 2 and not c.gaps)
        (unsub, *_) = conn.requests("unsubscribe", "book")
        assert unsub == {"channel": "book", "symbol": ["BTC/USD"], "depth": BOOK_DEPTH}
        await stop(task)

    asyncio.run(scenario())
    assert len(rows(tmp_path, "book_snapshot")) >= 2
    assert "resync" in {g["kind"] for g in rows(tmp_path, "gap")}


def test_dropped_rows_open_a_gap_and_resync_that_book(tmp_path):
    c = TidePoolCollector(["BTC/USD"], {"hot_dir": str(tmp_path), "queue_maxsize": 1,
                                        "snapshot_interval_seconds": 0}, {"heartbeat_timeout": 0})
    conn = FakeConn()

    async def scenario():
        await c.handle_message(snapshot("BTC/USD", [(100.0, 1.0)]))      # fills the queue
        await c.handle_message(book_update("BTC/USD", [(99.0, 2.0)]))    # dropped
        assert c.gaps[("BTC/USD", "book")][0] == "dropped_rows" and c.dropped_rows == 1

        c.queue.get_nowait()                                            # the writer catches up
        await c._maintain_once(conn)
        assert conn.requests("unsubscribe", "book") == conn.requests("subscribe", "book") == [
            {"channel": "book", "symbol": ["BTC/USD"], "depth": BOOK_DEPTH}]

        await c.handle_message(snapshot("BTC/USD", [(100.0, 1.0)]))      # the resync's snapshot is stored
        assert ("BTC/USD", "book") not in c.gaps

    asyncio.run(scenario())
    gaps = c.writer._buffers["gap"]
    book = [i for i, (s, k) in enumerate(zip(gaps["stream"], gaps["kind"])) if s == "book" and k == "dropped_rows"]
    assert len(book) == 1
    assert gaps["start_ts"][book[0]] < gaps["end_ts"][book[0]]


def test_a_dropped_trade_opens_a_trade_gap_until_the_next_stored_trade(tmp_path):
    c = TidePoolCollector(["BTC/USD"], {"hot_dir": str(tmp_path), "queue_maxsize": 1})

    def trade(i):
        return {"channel": "trade", "type": "update", "data": [{
            "symbol": "BTC/USD", "side": "buy", "price": 100.0, "qty": 1.0, "ord_type": "limit",
            "trade_id": i, "timestamp": TS}]}

    async def scenario():
        await c.handle_message(trade(1))                                # stored; closes the start gap
        await c.handle_message(trade(2))                                # dropped
        assert c.gaps[("BTC/USD", "trade")][0] == "dropped_rows"
        c.queue.get_nowait()
        await c.handle_message(trade(3))                                # stored again
        assert ("BTC/USD", "trade") not in c.gaps

    asyncio.run(scenario())
    assert c.writer._buffers["gap"]["kind"].count("dropped_rows") == 1
