# Data Collector: Parquet Storage Spec

## Goals
- Hold collected data long term, heavily compressed.
- Make the last 10 hours of data fast and easy to load on startup so trading models can train on it.

## Architecture: hot tier and cold tier

Optimize the hot tier for fast reads. Optimize the cold tier for size.

```
data/
  hot/    ts_<start_ns>_<end_ns>.parquet     # 1-5 min files, lz4 or snappy, last ~24h
  cold/   date=YYYY-MM-DD/part-0.parquet     # daily, sorted, zstd level 9+
```

## Hot tier

- Roll a new file every 1 to 5 minutes. A Parquet file is not readable until its footer is written, so the file currently being written cannot be read. Short rolls keep that unreadable window small.
- Put the start and end timestamps in the filename. Startup can then select the last 10 hours from a directory listing without opening any file.
- Use light compression (lz4 or snappy). About 24h of data is small, so write and read speed matter more than ratio.
- Write to a temp name, then atomically rename to the final name. The loader must never see a half-written file.
- Keep the hot window at 24h even though only 10h is needed at startup. This gives slack if compaction lags or the collector was down.

## Cold tier

- A compaction job merges hot files older than about 24h into one sorted daily file.
- Use zstd at a high level (9 to 15). Files are written once and read rarely.
- Delete hot files only after the cold file is written and its row count is verified against the source files.

## Compression rules

**Codec**
- zstd is the default. Level 3 is a balanced choice. Levels 9 to 15 give smaller files for write-once data. Decompression stays fast at any level.
- Skip snappy and gzip for archival data (snappy compresses worse, gzip is slower and usually larger than zstd).

**Things that shrink files more than the codec does**
- Sort before writing: by symbol, then timestamp (or by whatever the low-cardinality keys are). Sorted data gives longer runs and better dictionary, delta, and zstd results. Can cut size 30 to 50 percent.
- Use the narrowest types. Prices as scaled int64 (int32 where it fits) instead of float64. Timestamps as int64 with the correct unit. Symbols and enums as dictionary-encoded strings or small ints.
- Use `DELTA_BINARY_PACKED` for timestamps and monotonic sequence IDs. With pyarrow, set it via `column_encoding` and make sure `use_dictionary` is a list or False so the two do not conflict.
- If floats must stay floats, use `BYTE_STREAM_SPLIT` encoding. It helps zstd a lot.
- Keep the schema flat. Avoid nested or repeated columns unless needed.

**Layout**
- Row groups of roughly 128 MB uncompressed, or 1 to 10 million rows.
- Cold files of 256 MB to 1 GB. Tiny files hurt compression and query speed.
- Partition by date. Partition by symbol or exchange only if cardinality is low. Over-partitioning creates many small files.

**Realistic expectation:** 5 to 15x compression versus raw CSV or JSON for market data once types are tight and data is sorted. Random floats compress much worse.

## Cold file writer (pyarrow)

```python
import pyarrow as pa
import pyarrow.parquet as pq

table = table.sort_by([("symbol", "ascending"), ("ts", "ascending")])

pq.write_table(
    table,
    "data.parquet",
    compression="zstd",
    compression_level=9,
    use_dictionary=["symbol", "side"],
    column_encoding={"ts": "DELTA_BINARY_PACKED", "seq": "DELTA_BINARY_PACKED"},
    row_group_size=2_000_000,
    write_statistics=True,
)
```

## Ingestion pattern

- Parquet files are immutable. Never append to them.
- Buffer in memory or write to a staging format (WAL, Arrow IPC, or small Parquet files), then roll up on a schedule.
- If crash safety is needed, the staging layer provides it.

## Startup loader (last 10 hours)

```python
import time
import pyarrow.dataset as ds

cutoff = time.time_ns() - 10 * 3600 * 1_000_000_000

dset = ds.dataset("data/hot", format="parquet")
table = dset.to_table(filter=ds.field("ts") >= cutoff)
df = table.to_pandas()  # or keep as Arrow / polars
```

Row group statistics on `ts` let pyarrow skip files and row groups outside the window. If filenames follow the pattern above, the file list can also be filtered by name before building the dataset.

## Edge cases to handle

- **Collector was down:** after loading, check the max time gap between consecutive timestamps per symbol. Log or fail if the window is not contiguous. Training on a window with a hole can quietly hurt the model.
- **Window crosses into cold:** if the hot tier is missing data (for example after a long outage plus compaction), fall back to reading `cold/date=...` for the last two days with the same timestamp filter.
- **Live tail:** the last few minutes will not be in a closed file yet. If the models need them, keep a small in-memory ring buffer in the collector, or expose it over Redis or a socket, and merge it after loading the files.
- **Multiple consumers:** if several processes need the same 10h at startup, have the collector publish it (Redis or a socket) instead of each process reading disk.

Ten hours of market data is small enough to hold fully in memory, so the loader should be fast.

## Open questions (fill in before finalizing)

- Rows per hour being collected?
- One exchange or several?
- Schema (columns and types)?
- Query pattern for cold data: full scans, or by symbol and time range?

These decide the roll interval for hot files (5 min vs shorter), partitioning, encodings, and the compaction schedule.
