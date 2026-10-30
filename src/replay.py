"""Rebuild recorded Kraken L2 books from Tidepool's hot Parquet files."""

import zlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow.dataset as ds

from src.storage.hot_writer import BOOK_DEPTH


@dataclass(frozen=True)
class BookEvent:
    ts: int
    recv_ts: int
    run_id: str
    seq: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]
    source: str


def _rows(root: Path, stream: str, symbol: str) -> list[dict]:
    directory = root / stream
    files = sorted(directory.glob("*.parquet")) if directory.exists() else []
    if not files:
        return []
    dataset = ds.dataset([str(path) for path in files], format="parquet")
    return dataset.to_table(filter=ds.field("symbol") == symbol).to_pylist()


def _levels(book: dict, side: str) -> tuple[tuple[float, float], ...]:
    return tuple(sorted(book[side].items(), reverse=side == "bids")[:BOOK_DEPTH])


def _checksum(book: dict, price_decimals: int, qty_decimals: int) -> int:
    def digits(value: float, decimals: int) -> str:
        return f"{value:.{decimals}f}".replace(".", "").lstrip("0") or "0"

    # Kraken hashes asks first, then bids, using the top ten levels of each.
    payload = "".join(
        digits(price, price_decimals) + digits(qty, qty_decimals)
        for side in ("asks", "bids") for price, qty in _levels(book, side)[:10]
    )
    return zlib.crc32(payload.encode()) & 0xFFFFFFFF


def replay_books(root: str | Path, symbol: str, start_ts: int, end_ts: int,
                 *, price_decimals: int | None = None, qty_decimals: int | None = None):
    """Yield books visible in [start_ts, end_ts), measured by collector receive time.

    Supply both decimal counts to verify Kraken checksums. A mismatch raises ValueError.
    These counts must match Kraken's original price and quantity formatting.
    """
    if end_ts <= start_ts:
        raise ValueError("end_ts must be greater than start_ts")
    if (price_decimals is None) != (qty_decimals is None):
        raise ValueError("supply both price_decimals and qty_decimals")
    if any(value is not None and value < 0 for value in (price_decimals, qty_decimals)):
        raise ValueError("decimal counts cannot be negative")

    root = Path(root)
    snapshots = _rows(root, "book_snapshot", symbol)
    deltas = _rows(root, "book_delta", symbol)
    gaps = [row for row in _rows(root, "gap", symbol) if row["stream"] == "book"]
    events = [(row["recv_ts"], row["run_id"], row["seq"], 0, row) for row in snapshots]
    events += [(row["recv_ts"], row["run_id"], row["seq"], 1, row) for row in deltas]
    events.sort(key=lambda event: event[:4])

    book = None
    run_id = None
    # Apply whole Kraken messages before emitting or checking their book.
    index = 0
    while index < len(events):
        recv_ts, event_run, seq, kind, row = events[index]
        index += 1
        group = [row]
        if kind == 1:
            while index < len(events) and events[index][:4] == (recv_ts, event_run, seq, 1):
                group.append(events[index][4])
                index += 1

        if kind == 0:
            # Every stored snapshot is a fresh starting point, including after resync.
            book = {side: {} for side in ("bids", "asks")}
            for side in ("bids", "asks"):
                for level in range(1, BOOK_DEPTH + 1):
                    price, qty = row[f"{side[:-1]}_{level}_price"], row[f"{side[:-1]}_{level}_qty"]
                    if price is not None and qty is not None:
                        book[side][price] = qty
            run_id = event_run
        elif book is not None and run_id == event_run:
            for change in group:
                side = "bids" if change["side"] == "bid" else "asks"
                if change["qty"] == 0:
                    book[side].pop(change["price"], None)
                else:
                    book[side][change["price"]] = change["qty"]
            # Kraken does not send removals for levels pushed past depth 25.
            for side in ("bids", "asks"):
                book[side] = dict(_levels(book, side))
        else:
            # A delta from another run cannot continue the previous book.
            book = None
            continue

        # Gap timestamps use receive time. A snapshot at the gap end restores trust.
        in_gap = any(g["run_id"] == event_run and
                     (g["start_ts"] is None or recv_ts > g["start_ts"]) and
                     (g["end_ts"] is None or recv_ts < g["end_ts"])
                     for g in gaps)
        if in_gap:
            book = None
            continue

        expected = row["checksum"]
        if price_decimals is not None and expected is not None:
            actual = _checksum(book, price_decimals, qty_decimals)
            if actual != expected:
                raise ValueError(f"checksum mismatch for {symbol} at run {event_run} seq {seq}: "
                                 f"expected {expected}, got {actual}")
        if start_ts <= recv_ts < end_ts:
            yield BookEvent(row["ts"], recv_ts, event_run, seq,
                            _levels(book, "bids"), _levels(book, "asks"),
                            "snapshot" if kind == 0 else "delta")
