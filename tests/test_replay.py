"""Replay persisted books through resyncs, gaps, and checksum checks."""

import asyncio

import pytest

from src.replay import _checksum, replay_books
from src.storage.hot_writer import HotParquetWriter


def snapshot(ts, seq, bids, asks, run="r1", checksum=None):
    row = {"ts": ts, "recv_ts": ts, "run_id": run, "seq": seq,
           "symbol": "BTC/USD", "checksum": checksum}
    for side, levels in (("bid", bids), ("ask", asks)):
        for index, (price, qty) in enumerate(levels, 1):
            row[f"{side}_{index}_price"] = price
            row[f"{side}_{index}_qty"] = qty
    return row


def delta(ts, seq, side, price, qty, run="r1", checksum=None):
    return {"ts": ts, "recv_ts": ts, "run_id": run, "seq": seq,
            "symbol": "BTC/USD", "side": side, "price": price, "qty": qty,
            "checksum": checksum}


def write(tmp_path, events):
    async def save():
        writer = HotParquetWriter(asyncio.Queue(), base_dir=str(tmp_path))
        for stream, row in events:
            writer.append(stream, row)
        await writer.stop()
    asyncio.run(save())


def test_replays_whole_messages_and_replaces_book_at_next_snapshot(tmp_path):
    write(tmp_path, [
        ("book_snapshot", snapshot(10, 1, [(100.0, 1.0)], [(101.0, 2.0)])),
        ("book_delta", delta(20, 2, "bid", 100.0, 0.0)),
        ("book_delta", delta(20, 2, "bid", 99.0, 3.0)),
        ("book_snapshot", snapshot(30, 3, [(98.0, 4.0)], [(102.0, 1.0)])),
        ("book_delta", delta(40, 4, "ask", 102.0, 0.0)),
    ])
    events = list(replay_books(tmp_path, "BTC/USD", 15, 41))
    assert [(e.seq, e.source, e.bids, e.asks) for e in events] == [
        (2, "delta", ((99.0, 3.0),), ((101.0, 2.0),)),
        (3, "snapshot", ((98.0, 4.0),), ((102.0, 1.0),)),
        (4, "delta", ((98.0, 4.0),), ()),
    ]


def test_gap_and_new_run_require_new_snapshot(tmp_path):
    gap = {"ts": 40, "run_id": "r1", "symbol": "BTC/USD", "stream": "book",
           "kind": "disconnect", "start_ts": 20, "end_ts": 40, "detail": None}
    write(tmp_path, [
        ("book_snapshot", snapshot(10, 1, [(100.0, 1.0)], [])),
        ("book_delta", delta(20, 2, "bid", 99.0, 2.0)),
        ("book_delta", delta(30, 3, "bid", 98.0, 3.0)),
        ("gap", gap),
        ("book_snapshot", snapshot(40, 4, [(97.0, 4.0)], [])),
        ("book_delta", delta(50, 1, "bid", 96.0, 5.0, run="r2")),
        ("book_snapshot", snapshot(60, 2, [(95.0, 6.0)], [], run="r2")),
    ])
    events = list(replay_books(tmp_path, "BTC/USD", 0, 70))
    assert [(e.recv_ts, e.bids[0][0]) for e in events] == [(10, 100.0), (20, 100.0), (40, 97.0), (60, 95.0)]


def test_checksum_detects_bad_book(tmp_path):
    book = {"bids": {100.0: 1.25}, "asks": {101.0: 2.0}}
    check = _checksum(book, 1, 2)
    write(tmp_path, [
        ("book_snapshot", snapshot(10, 1, [(100.0, 1.25)], [(101.0, 2.0)], checksum=check)),
        ("book_delta", delta(20, 2, "bid", 100.0, 1.5, checksum=0)),
    ])
    with pytest.raises(ValueError, match="checksum mismatch"):
        list(replay_books(tmp_path, "BTC/USD", 0, 30, price_decimals=1, qty_decimals=2))


def test_depth_trim_and_open_gap(tmp_path):
    bids = [(float(price), 1.0) for price in range(100, 75, -1)]
    write(tmp_path, [
        ("book_snapshot", snapshot(10, 1, bids, [])),
        ("book_delta", delta(20, 2, "bid", 101.0, 2.0)),
        ("book_delta", delta(30, 3, "bid", 101.0, 0.0)),
        ("gap", {"ts": 40, "run_id": "r1", "symbol": "BTC/USD", "stream": "book",
                 "kind": "dropped_rows", "start_ts": 30, "end_ts": None, "detail": None}),
        ("book_delta", delta(40, 4, "bid", 99.0, 3.0)),
    ])
    events = list(replay_books(tmp_path, "BTC/USD", 0, 50))
    assert [event.seq for event in events] == [1, 2, 3]
    assert len(events[1].bids) == 25
    assert events[1].bids[-1][0] == 77.0
    assert events[2].bids[-1][0] == 77.0  # The trimmed level cannot reappear.
