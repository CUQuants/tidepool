# Strategy Testing Readiness

What the collector gives you for replay, backtesting and model training, what is missing, and what to fix first. Storage layout and file format are in `parquet-storage-spec.md`.

## What is collected today

Three hot-tier streams under `data/hot/`, all from the Kraken v2 public WebSocket:

| Stream | One row per | Key columns |
|---|---|---|
| `book_delta` | changed price level | `ts`, `recv_ts`, `seq`, `symbol`, `side`, `price`, `qty` (0 = removed), `checksum` |
| `book_snapshot` | Kraken snapshot (startup + every `snapshot_interval_seconds`) | `ts`, `recv_ts`, `seq`, `symbol`, `checksum`, `bid_1..25_price/qty`, `ask_1..25_price/qty` |
| `trade` | trade | `ts`, `recv_ts`, `symbol`, `side` (taker), `price`, `qty`, `ord_type`, `trade_id` |

- `ts` is Kraken's timestamp and `recv_ts` is the collector's clock, both in ns since epoch. `recv_ts - ts` is the observed feed latency.
- `seq` orders book rows in arrival order within one collector run.
- Book depth is 25 levels per side. Markets are whatever `config.yaml` lists (currently FX and stablecoin pairs).

### What this is enough for
- Rebuilding the L2 book (top 25) at any point in time.
- Features such as mid, microprice, spread, depth imbalance, order flow imbalance, trade flow and realized volatility.
- Backtesting strategies that take liquidity (market orders, or crossing limits): the book shows the price you would have paid and how deep you would have walked.
- Modelling latency using the `recv_ts - ts` distribution.

### What it is not enough for (yet)
- Market-making strategies that depend on exact queue position (see "Data to add").
- Training sets that span months (no cold tier yet; see "System improvements").
- Strategies on pairs not in `config.yaml`.

## Replaying the book

1. Pick a symbol and a start time `T`.
2. Load the latest `book_snapshot` row for that symbol with `ts <= T`. That row is the starting book.
3. Load `book_delta` rows for that symbol with `seq` greater than the snapshot's `seq`, sorted by `seq`.
4. For each group of rows sharing a `seq` (one Kraken message), apply every level (`qty == 0` removes it), then trim each side to 25 levels. Kraken does not send removals for levels pushed past the subscribed depth, so the trim is required.
5. Optional: after each message, compute the CRC32 of the top 10 levels and compare with `checksum`. A mismatch means the rebuilt book is wrong from that point until the next snapshot.
6. When you reach the next `book_snapshot`, replace the book with it.

Caveats:
- `seq` restarts at 0 when the collector restarts. Never apply deltas across a restart; start again from the first snapshot of the new run. Until gap markers exist (below), detect restarts by `seq` going down.
- Prices are stored as float64. Exact checksum checking needs the price and quantity text exactly as Kraken sent it, rebuilt using each pair's price and lot decimals.

No replay loader exists in the repo yet. Writing one is the first piece of work under "Tooling".

## Building a backtest on this data

Things a realistic simulator needs, and where each comes from:

| Need | Source | Status |
|---|---|---|
| Book at decision time | replay of `book_snapshot` + `book_delta` | data collected, loader not written |
| Trades, to fill resting orders | `trade` stream (`side` is the taker) | collected |
| Latency (feed, decision, order round trip) | `recv_ts - ts` for the feed; order latency must be measured from live trading through the gateway | feed: collected; order: not measured |
| Fees | Kraken fee tier for the account | not collected; hardcode per tier for now |
| Tick size, lot size, min order, price/lot decimals | Kraken `instrument` channel or REST `AssetPairs` | not collected |
| Exact queue position for resting orders | Kraken `level3` channel (individual orders) | not collected |
| Exchange status, halts, maintenance | Kraken `status` channel | not collected |

Guidelines:
- **Decide on what you would have seen.** Index decisions by `recv_ts`, not `ts`. The strategy can only act once data reaches it.
- **Add order latency.** An order sent at `recv_ts = t` reaches the matching engine at about `t + order latency`, and fills against the book at that time, not at `t`.
- **Fill resting orders conservatively with L2 data.** Treat your order as last in the queue at its price. Count it filled only when trades at that price exceed the quantity ahead of you, or when the price trades through. This underestimates fills, which is the safe direction.
- **Walk the book for aggressive orders.** Take quantity level by level, not all at the top price.
- **Model market impact for large orders.** The replayed book does not react to your orders. For sizes that are large relative to top-of-book depth, results will be too optimistic.
- **Skip data gaps.** Do not trade or compute features across a restart or dropped-row gap. Mark those windows and exclude them.
- **Separate training and testing by time,** with a buffer between them, so features built on overlapping windows don't leak future data.

## Data to add

In rough order of value:

1. **Instrument metadata** (`instrument` channel or `AssetPairs`): tick size, lot size, minimum order size, price and lot decimals. Store one snapshot per day plus any changes. Needed for valid order prices and sizes in simulation, and for exact checksum checking.
2. **Gap markers:** a row on every disconnect, restart, resync and dropped queue row, with start and end times. Without these, replays and training sets contain silent holes.
3. **Run id:** a column (or file-level metadata) identifying each collector run, so `seq` resets are explicit.
4. **More markets** if strategies need them (BTC, ETH and the other majors). Check queue and disk headroom before adding high-volume pairs.
5. **Level3 data** (`level3` channel, authenticated), only if a market-making strategy needs real queue position. It is much larger than L2.
6. **Fee schedule** for the trading account, stored with an effective date.
7. **Our own orders and fills** from the trading gateway's audit log, joined to market data, to compare simulated against real fills (slippage and fill-rate calibration).
8. **Exchange status** (`status` channel), to exclude maintenance and cancel-only periods.

## System improvements

### Reliability (fix before trusting any dataset)
- **Disconnects end the process.** `kraken_api/kraken_ws.py` calls `sys.exit` when the connection closes and in `close()`, and `src/collector.py` calls `sys.exit(1)` on errors. `SystemExit` is not an `Exception`, so the restart loop in `main.py` never runs. Reconnect inside the collector (or raise a normal exception), and run under a supervisor (systemd or Docker `restart: always`) as a backstop.
- **Dropped rows are only logged.** When the write queue is full, rows are dropped silently in the data. Write a gap marker, and force a resync so the book is correct again.
- **No live checksum checking.** A wrong in-memory book goes unnoticed until the next snapshot (up to `snapshot_interval_seconds`). Check the checksum on every message and resubscribe on mismatch. This needs exact decimal parsing (`json.loads(..., parse_float=Decimal)`) in `kraken_ws.py`, and is the point to switch the in-memory book to a sorted structure such as `sortedcontainers.SortedDict`.
- **Clock sync.** `recv_ts` and the latency numbers are only meaningful if the host runs NTP or chrony. Log clock offset at startup.
- **Monitoring.** Track rows per minute per stream, queue depth, dropped rows, resync count and the latest `recv_ts - ts`. Alert when a stream goes quiet.

### Storage
- **No cold tier.** The compaction job in `parquet-storage-spec.md` (hot files to daily zstd files, verified row counts, then delete hot files) does not exist, and nothing removes hot files. Needed before collecting for weeks.
- **Backups:** copy cold files off the collector host.
- **Schema versioning:** record a schema version in Parquet metadata so loaders can handle future column changes.

### Tooling
- **Replay loader:** reads a symbol and time range, rebuilds the book step by step following "Replaying the book", and verifies checksums. For speed, use two fixed-size sorted arrays per book (numpy or numba) rather than a dict or tree.
- **Data quality report:** per symbol per day, coverage (time with valid data), gaps, checksum mismatch rate, latency percentiles.
- **Backtest harness:** event loop over replayed book and trade events in `recv_ts` order, with the fill and latency rules above, fees and instrument rules.
- **Dependencies:** `requirements.txt` lists `asyncio` and `logging`, which are part of Python. Remove them so `pip install -r requirements.txt` works.

## Suggested order

1. Reconnect handling, gap markers and run id.
2. Replay loader with checksum verification. This also tests that the collected data is correct.
3. Instrument metadata and live checksum checking.
4. Cold tier compaction and monitoring.
5. Backtest harness and data quality report.
6. More markets, level3 and own-fill data as specific strategies need them.
