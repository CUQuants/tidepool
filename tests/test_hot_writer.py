import asyncio
import os
import re

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from src.storage.hot_writer import BOOK_DELTA_SCHEMA, BOOK_SNAPSHOT_SCHEMA, TRADE_SCHEMA, HotParquetWriter, parse_ts_ns

FILE_RE = re.compile(r"^ts_(\d+)_(\d+)(_\d+)?\.parquet$")


def test_parse_ts_ns():
    assert parse_ts_ns("1970-01-01T00:00:01.000001Z") == 1_000_001_000
    assert parse_ts_ns("2025-09-01T20:11:35.911390Z") == 1756757495_911390_000


def _book_row(ts, symbol, seq=None):
    return {"ts": ts, "recv_ts": ts + 5, "seq": ts if seq is None else seq, "symbol": symbol, "checksum": 1,
            "bid_1_price": 100.0, "bid_1_qty": 1.0, "ask_1_price": 101.0, "ask_1_qty": 2.0}


def _delta_row(ts, seq, symbol, side="bid", price=100.0, qty=1.0):
    return {"ts": ts, "recv_ts": ts + 5, "seq": seq, "symbol": symbol,
            "side": side, "price": price, "qty": qty, "checksum": 7}


def _trade_row(ts, symbol, trade_id):
    return {"ts": ts, "recv_ts": ts + 5, "symbol": symbol, "side": "buy", "price": 100.5,
            "qty": 0.25, "ord_type": "limit", "trade_id": trade_id}


def _files(base, stream):
    return sorted(os.listdir(os.path.join(base, stream)))


def test_writer_rolls_and_flushes_on_stop(tmp_path):
    base = str(tmp_path / "hot")

    async def scenario():
        queue = asyncio.Queue()
        writer = HotParquetWriter(queue, base_dir=base, roll_interval_s=0.2, max_rows=1_000_000)
        writer.start()

        # First window: out of order across symbols to exercise sorting
        for ts, sym in [(30, "ETH/USD"), (10, "BTC/USD"), (20, "ETH/USD"), (40, "BTC/USD")]:
            queue.put_nowait(("book_snapshot", _book_row(ts, sym)))
        queue.put_nowait(("trade", _trade_row(15, "BTC/USD", 1)))
        await asyncio.sleep(0.5)  # roll happens
        assert len(_files(base, "book_snapshot")) == 1

        # Second window is left buffered and must be written by stop()
        for ts in (50, 60):
            queue.put_nowait(("book_snapshot", _book_row(ts, "BTC/USD")))
        await writer.stop()

    asyncio.run(scenario())

    book_files = _files(base, "book_snapshot")
    assert len(book_files) == 2
    assert not any(f.endswith(".tmp") for f in os.listdir(os.path.join(base, "book_snapshot")))

    for name in book_files:
        m = FILE_RE.match(name)
        assert m, name
        t = pq.read_table(os.path.join(base, "book_snapshot", name))
        assert t.schema.equals(BOOK_SNAPSHOT_SCHEMA)
        assert int(m.group(1)) == pc.min(t["ts"]).as_py()
        assert int(m.group(2)) == pc.max(t["ts"]).as_py()

    first = pq.read_table(os.path.join(base, "book_snapshot", book_files[0]))
    assert list(zip(first["symbol"].to_pylist(), first["ts"].to_pylist())) == [
        ("BTC/USD", 10), ("BTC/USD", 40), ("ETH/USD", 20), ("ETH/USD", 30)]
    assert first["bid_2_price"].null_count == first.num_rows

    book = ds.dataset(os.path.join(base, "book_snapshot"), format="parquet").to_table()
    assert book.num_rows == 6

    trades = ds.dataset(os.path.join(base, "trade"), format="parquet").to_table()
    assert trades.schema.equals(TRADE_SCHEMA)
    assert trades.to_pylist() == [_trade_row(15, "BTC/USD", 1)]


def test_max_rows_triggers_early_roll(tmp_path):
    base = str(tmp_path / "hot")

    async def scenario():
        queue = asyncio.Queue()
        writer = HotParquetWriter(queue, base_dir=base, roll_interval_s=3600, max_rows=3)
        writer.start()
        for i in range(7):
            queue.put_nowait(("trade", _trade_row(i, "BTC/USD", i)))
        await asyncio.sleep(0.3)
        assert len(_files(base, "trade")) == 2
        await writer.stop()

    asyncio.run(scenario())
    assert len(_files(base, "trade")) == 3
    trades = ds.dataset(os.path.join(base, "trade"), format="parquet").to_table()
    assert sorted(trades["trade_id"].to_pylist()) == list(range(7))


def test_deltas_sort_by_seq_not_ts(tmp_path):
    """Deltas with equal or out-of-order exchange timestamps must keep arrival (seq) order."""
    base = str(tmp_path / "hot")

    async def scenario():
        queue = asyncio.Queue()
        writer = HotParquetWriter(queue, base_dir=base, roll_interval_s=3600)
        writer.start()
        for ts, seq in [(100, 3), (100, 1), (90, 2)]:
            queue.put_nowait(("book_delta", _delta_row(ts, seq, "BTC/USD")))
        await writer.stop()

    asyncio.run(scenario())
    deltas = ds.dataset(os.path.join(base, "book_delta"), format="parquet").to_table()
    assert deltas.schema.equals(BOOK_DELTA_SCHEMA)
    assert deltas["seq"].to_pylist() == [1, 2, 3]
