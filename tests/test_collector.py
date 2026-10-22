import asyncio

from src.collector import TidePoolCollector
from src.storage.hot_writer import BOOK_DEPTH, parse_ts_ns

TS1 = "2025-09-01T20:11:35.911390Z"
TS2 = "2025-09-01T20:11:36.000001Z"


def _collector():
    return TidePoolCollector(["BTC/USD"], {"queue_maxsize": 10_000})


def _drain(c):
    rows = []
    while not c.queue.empty():
        rows.append(c.queue.get_nowait())
    return rows


def _book_msg(kind, bids, asks, ts=TS1, checksum=123):
    return {"channel": "book", "type": kind, "data": [{
        "symbol": "BTC/USD", "timestamp": ts, "checksum": checksum,
        "bids": [{"price": p, "qty": q} for p, q in bids],
        "asks": [{"price": p, "qty": q} for p, q in asks],
    }]}


def test_snapshot_then_updates_store_snapshot_and_deltas():
    c = _collector()

    async def scenario():
        await c.handle_message(_book_msg("snapshot", [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0)]))
        await c.handle_message(_book_msg("update", [(100.0, 0.0), (98.0, 5.0)], [], ts=TS2, checksum=456))

    asyncio.run(scenario())
    rows = _drain(c)

    (s_stream, snap), *deltas = rows
    assert s_stream == "book_snapshot"
    assert snap["ts"] == parse_ts_ns(TS1) and snap["seq"] == 1 and snap["checksum"] == 123
    assert (snap["bid_1_price"], snap["bid_2_price"], snap["ask_1_price"]) == (100.0, 99.0, 101.0)
    assert snap["recv_ts"] > 0

    assert [s for s, _ in deltas] == ["book_delta", "book_delta"]
    assert [(d["side"], d["price"], d["qty"]) for _, d in deltas] == [("bid", 100.0, 0.0), ("bid", 98.0, 5.0)]
    assert all(d["ts"] == parse_ts_ns(TS2) and d["seq"] == 2 and d["checksum"] == 456 for _, d in deltas)
    assert c.orderbooks["BTC/USD"]["bids"] == {99.0: 2.0, 98.0: 5.0}


def test_resync_snapshot_replaces_drifted_book():
    c = _collector()

    async def scenario():
        await c.handle_message(_book_msg("snapshot", [(100.0, 1.0)], [(101.0, 1.0)]))
        await c.handle_message(_book_msg("update", [(50.0, 1.0)], []))  # stale level we "missed" the removal of
        await c.handle_message(_book_msg("snapshot", [(100.0, 2.0)], [(101.0, 1.0)], ts=TS2))

    asyncio.run(scenario())
    assert c.orderbooks["BTC/USD"]["bids"] == {100.0: 2.0}
    assert [s for s, _ in _drain(c)] == ["book_snapshot", "book_delta", "book_snapshot"]


def test_book_truncated_to_depth():
    c = _collector()
    bids = [(float(1000 - i), 1.0) for i in range(BOOK_DEPTH)]

    async def scenario():
        await c.handle_message(_book_msg("snapshot", bids, []))
        # A better bid pushes the worst level out; Kraken sends no qty 0 for it
        await c.handle_message(_book_msg("update", [(2000.0, 1.0)], []))

    asyncio.run(scenario())
    book = c.orderbooks["BTC/USD"]["bids"]
    assert len(book) == BOOK_DEPTH
    assert 2000.0 in book and float(1000 - BOOK_DEPTH + 1) not in book


def test_missing_exchange_timestamp_falls_back_to_recv_ts():
    c = _collector()
    msg = _book_msg("update", [(100.0, 1.0)], [])
    del msg["data"][0]["timestamp"]
    asyncio.run(c.handle_message(msg))
    (_, row), = _drain(c)
    assert row["ts"] == row["recv_ts"]


def test_trade_uses_exchange_timestamp():
    c = _collector()
    msg = {"channel": "trade", "type": "update", "data": [{
        "symbol": "BTC/USD", "side": "buy", "price": 100.5, "qty": 0.25,
        "ord_type": "limit", "trade_id": 9, "timestamp": TS1}]}
    asyncio.run(c.handle_message(msg))
    (stream, row), = _drain(c)
    assert stream == "trade" and row["ts"] == parse_ts_ns(TS1) and row["trade_id"] == 9
