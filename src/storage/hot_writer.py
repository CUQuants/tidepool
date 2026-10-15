"""
Hot-tier Parquet writer.
Drains (stream, row) tuples from an asyncio.Queue, buffers them in memory, and
rolls short immutable Parquet files per docs/parquet-storage-spec.md:

    <base_dir>/<stream>/ts_<start_ns>_<end_ns>.parquet
"""
import asyncio
import datetime
import logging
import os
import time
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

BOOK_DEPTH = 25

_ORDER_FIELDS = [
    pa.field("ts", pa.int64(), nullable=False),        # exchange timestamp
    pa.field("recv_ts", pa.int64(), nullable=False),   # our receive time
    pa.field("seq", pa.int64(), nullable=False),       # per-process message counter; restarts begin with a snapshot
    pa.field("symbol", pa.string(), nullable=False),
]

# One row per changed price level, exactly as Kraken sent it. qty == 0 removes the level.
BOOK_DELTA_SCHEMA = pa.schema(_ORDER_FIELDS + [
    pa.field("side", pa.string(), nullable=False),     # "bid" or "ask"
    pa.field("price", pa.float64(), nullable=False),
    pa.field("qty", pa.float64(), nullable=False),
    pa.field("checksum", pa.int64()),                  # Kraken CRC32 of the top 10 levels after this message
])

# Full top-BOOK_DEPTH book from each Kraken snapshot (startup and every periodic resync).
_snapshot_fields = _ORDER_FIELDS + [pa.field("checksum", pa.int64())]
for _i in range(1, BOOK_DEPTH + 1):
    _snapshot_fields += [pa.field(f"bid_{_i}_price", pa.float64()), pa.field(f"bid_{_i}_qty", pa.float64())]
for _i in range(1, BOOK_DEPTH + 1):
    _snapshot_fields += [pa.field(f"ask_{_i}_price", pa.float64()), pa.field(f"ask_{_i}_qty", pa.float64())]

BOOK_SNAPSHOT_SCHEMA = pa.schema(_snapshot_fields)

TRADE_SCHEMA = pa.schema([
    pa.field("ts", pa.int64(), nullable=False),
    pa.field("recv_ts", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("side", pa.string()),
    pa.field("price", pa.float64()),
    pa.field("qty", pa.float64()),
    pa.field("ord_type", pa.string()),
    pa.field("trade_id", pa.int64()),
])

SCHEMAS = {"book_delta": BOOK_DELTA_SCHEMA, "book_snapshot": BOOK_SNAPSHOT_SCHEMA, "trade": TRADE_SCHEMA}

# Book rows must replay in arrival order, so they sort by seq; equal exchange timestamps can't order them.
SORT_KEYS = {
    "book_delta": [("symbol", "ascending"), ("seq", "ascending")],
    "book_snapshot": [("symbol", "ascending"), ("seq", "ascending")],
    "trade": [("symbol", "ascending"), ("ts", "ascending")],
}

# Low-cardinality string columns get dictionary encoding; timestamps get delta encoding.
# The two sets must not overlap or pyarrow rejects the column_encoding.
_DICT_COLUMNS = {"symbol", "side", "ord_type"}
_DELTA_COLUMNS = {"ts": "DELTA_BINARY_PACKED", "recv_ts": "DELTA_BINARY_PACKED", "seq": "DELTA_BINARY_PACKED"}

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_ONE_US = datetime.timedelta(microseconds=1)


def parse_ts_ns(iso: str) -> int:
    """Parse a Kraken RFC3339 timestamp (e.g. 2025-09-01T20:11:35.911390Z) to int64 ns since epoch."""
    dt = datetime.datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return ((dt - _EPOCH) // _ONE_US) * 1000


class HotParquetWriter:
    """
    Background task that turns queued rows into hot-tier Parquet files.
    Files are written to a .tmp name and atomically renamed, so readers never see a partial file.
    """

    def __init__(
        self,
        queue: asyncio.Queue,
        base_dir: str = "data/hot",
        roll_interval_s: float = 60,
        max_rows: int = 500_000,
        compression: str = "lz4",
    ):
        self.queue = queue
        self.base_dir = base_dir
        self.roll_interval_s = roll_interval_s
        self.max_rows = max_rows
        self.compression = compression

        self._buffers: Dict[str, Dict[str, list]] = {}
        self._opened_at: Dict[str, float] = {}
        self._task: Optional[asyncio.Task] = None

        for stream in SCHEMAS:
            os.makedirs(os.path.join(base_dir, stream), exist_ok=True)
            self._reset(stream)

    def _reset(self, stream: str):
        self._buffers[stream] = {name: [] for name in SCHEMAS[stream].names}
        self._opened_at.pop(stream, None)

    def _rows(self, stream: str) -> int:
        return len(self._buffers[stream]["ts"])

    def _append(self, stream: str, row: dict):
        buf = self._buffers.get(stream)
        if buf is None:
            logger.warning(f"Dropping row for unknown stream '{stream}'")
            return
        if stream not in self._opened_at:
            self._opened_at[stream] = time.monotonic()
        for name, col in buf.items():
            col.append(row.get(name))

    def _next_timeout(self) -> Optional[float]:
        if not self._opened_at:
            return None
        oldest = min(self._opened_at.values())
        return max(0.0, oldest + self.roll_interval_s - time.monotonic())

    def _due_streams(self) -> List[str]:
        now = time.monotonic()
        return [s for s, t in self._opened_at.items() if now - t >= self.roll_interval_s]

    def _take(self, stream: str) -> Optional[Dict[str, list]]:
        if self._rows(stream) == 0:
            return None
        columns = self._buffers[stream]
        self._reset(stream)
        return columns

    def _drain_queue(self):
        while True:
            try:
                stream, row = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self._append(stream, row)

    async def _flush(self, stream: str):
        columns = self._take(stream)
        if columns is not None:
            await asyncio.to_thread(self._write_file, stream, columns)

    def _flush_all_sync(self):
        for stream in SCHEMAS:
            columns = self._take(stream)
            if columns is not None:
                self._write_file(stream, columns)

    def _write_file(self, stream: str, columns: Dict[str, list]):
        try:
            schema = SCHEMAS[stream]
            table = pa.Table.from_pydict(columns, schema=schema)
            table = table.sort_by(SORT_KEYS[stream])

            ts = columns["ts"]
            start_ns, end_ns = min(ts), max(ts)
            directory = os.path.join(self.base_dir, stream)
            final = os.path.join(directory, f"ts_{start_ns}_{end_ns}.parquet")
            # Two rolls covering the identical ns range would overwrite each other; keep both.
            n = 1
            while os.path.exists(final):
                final = os.path.join(directory, f"ts_{start_ns}_{end_ns}_{n}.parquet")
                n += 1
            tmp = final + ".tmp"

            pq.write_table(
                table,
                tmp,
                compression=self.compression,
                use_dictionary=[c for c in schema.names if c in _DICT_COLUMNS],
                column_encoding={c: e for c, e in _DELTA_COLUMNS.items() if c in schema.names},
                write_statistics=True,
            )
            os.replace(tmp, final)
            logger.info(f"Wrote {table.num_rows} {stream} rows to {final}")
        except Exception as e:
            logger.exception(f"Failed to write {stream} parquet file: {e}")

    def start(self) -> asyncio.Task:
        """Launch run() as a background task on the current loop."""
        self._task = asyncio.create_task(self.run())
        return self._task

    async def run(self):
        """Consume the queue until cancelled, rolling a file per stream every roll_interval_s."""
        try:
            while True:
                try:
                    stream, row = await asyncio.wait_for(self.queue.get(), self._next_timeout())
                    self._append(stream, row)
                    if stream in self._buffers and self._rows(stream) >= self.max_rows:
                        await self._flush(stream)
                except asyncio.TimeoutError:
                    pass
                for stream in self._due_streams():
                    await self._flush(stream)
        except asyncio.CancelledError:
            # Shutdown path (stop() or asyncio.run cancelling tasks): persist whatever is buffered.
            self._drain_queue()
            self._flush_all_sync()
            raise

    async def stop(self):
        """Stop the run task and flush everything still queued or buffered."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._drain_queue()
        self._flush_all_sync()
