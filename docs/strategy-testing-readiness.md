# Strategy Testing Readiness

What the collector gives you for replay, backtesting and model training, what is missing, and what to fix first. Storage layout and file format are in `parquet-storage-spec.md`.

## What is collected today

Five hot-tier streams under `data/hot/`, from the Kraken v2 public WebSocket and collector connection events:

| Stream | One row per | Key columns |
|---|---|---|
| `book_delta` | changed price level | `ts`, `recv_ts`, `seq`, `symbol`, `side`, `price`, `qty` (0 = removed), `checksum` |
| `book_snapshot` | Kraken snapshot (startup + every `snapshot_interval_seconds`) | `ts`, `recv_ts`, `seq`, `symbol`, `checksum`, `bid_1..25_price/qty`, `ask_1..25_price/qty` |
| `trade` | trade | `ts`, `recv_ts`, `symbol`, `side` (taker), `price`, `qty`, `ord_type`, `trade_id` |
| `gap` | span with missing or untrusted data, per symbol and stream | `symbol`, `stream` (`book` or `trade`), `kind`, `start_ts`, `end_ts`, `detail` |
| `status` | exchange status update or connection boundary | `ts` (receive time), `run_id`, `seq`, `system`, `source`, `details` |

- `ts` is Kraken's timestamp and `recv_ts` is the collector's clock, both in ns since epoch. `recv_ts - ts` is the observed feed latency.
- `seq` orders book rows in arrival order within one run. `run_id` names the run and is on every row.
- A `gap` row has `kind` `start`, `disconnect`, `resync` or `dropped_rows`. `start_ts` is the last good data (null for `start`). `end_ts` is when the data is good again: the next stored snapshot for `book`, the confirmed subscription or next stored trade for `trade`. It is null if the gap was still open at shutdown.
- Book depth is 25 levels per side. Markets are whatever `config.yaml` lists (currently FX and stablecoin pairs).
- `status` applies to the whole exchange. Exclude unknown periods, including after a disconnect or collector crash; don't carry a status across runs. Kraken's status channel has no exchange timestamp, so the transition is known only when received.

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

1. Pick a symbol and a start time `T` measured by collector receive time (`recv_ts`).
2. Load the latest usable `book_snapshot` row for that symbol with `recv_ts <= T`. That row is the starting book.
3. Load `book_delta` rows for that symbol with the snapshot's `run_id` and a `seq` greater than the snapshot's `seq`, sorted by `seq`.
4. For each group of rows sharing a `seq` (one Kraken message), apply every level (`qty == 0` removes it), then trim each side to 25 levels. Kraken does not send removals for levels pushed past the subscribed depth, so the trim is required.
5. Optional: after each message, compute the CRC32 of the top 10 levels and compare with `checksum`. A mismatch means the rebuilt book is wrong from that point until the next snapshot.
6. When you reach the next `book_snapshot`, replace the book with it.

Caveats:
- Never apply deltas across a `book` gap or across runs. After a gap, start again from the snapshot at its `end_ts`.
- Prices are stored as float64. Exact checksum checking needs the price and quantity text exactly as Kraken sent it, rebuilt using each pair's price and lot decimals.

`src/replay.py` provides `replay_books(hot_dir, symbol, start_ts, end_ts)`. Times are collector receive times in nanoseconds, with an inclusive start and exclusive end. It yields one `BookEvent` per snapshot or complete delta message, skips book gaps, and starts fresh after each snapshot or collector run. Supply `price_decimals` and `qty_decimals` to check Kraken CRC32; these must match the exchange's original decimal formatting. Existing float64 rows cannot guarantee exact checksums for every pair, so a mismatch may also indicate lost precision. The loader reads closed hot Parquet files; live buffered rows are not included.

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
| Exchange status, halts, maintenance | Kraken `status` channel | collected; unknown during connection gaps |

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
2. **More markets** if strategies need them (BTC, ETH and the other majors). Check queue and disk headroom before adding high-volume pairs.
3. **Level3 data** (`level3` channel, authenticated), only if a market-making strategy needs real queue position. It is much larger than L2.
4. **Fee schedule** for the trading account, stored with an effective date.
5. **Our own orders and fills** from the trading gateway's audit log, joined to market data, to compare simulated against real fills (slippage and fill-rate calibration).

## System improvements

### Reliability (fix before trusting any dataset)
- **Supervisor.** The collector reconnects on its own. Run it under systemd or Docker (`restart: always`) so that a crash or a reboot also restarts it.
- **No live checksum checking.** A wrong in-memory book goes unnoticed until the next snapshot (up to `snapshot_interval_seconds`). Check the checksum on every message and resubscribe on mismatch. This needs exact decimal parsing (`json.loads(..., parse_float=Decimal)`) in `kraken_ws.py`, and is the point to switch the in-memory book to a sorted structure such as `sortedcontainers.SortedDict`.
- **Clock sync.** `recv_ts` and the latency numbers are only meaningful if the host runs NTP or chrony. Log clock offset at startup.
- **Monitoring.** Track rows per minute per stream, queue depth, dropped rows, resync count and the latest `recv_ts - ts`. Alert when a stream goes quiet.

### Storage
- **No cold tier.** The compaction job in `parquet-storage-spec.md` (hot files to daily zstd files, verified row counts, then delete hot files) does not exist, and nothing removes hot files. Needed before collecting for weeks.
- **Backups:** copy cold files off the collector host.
- **Schema versioning:** record a schema version in Parquet metadata so loaders can handle future column changes.

### Tooling
- **Replay loader:** `src/replay.py` rebuilds the book for a symbol and receive-time range, checks gaps, and optionally verifies checksums when the pair's decimal formatting is supplied. For large datasets, replace its in-memory row scan with file pruning and fixed-size arrays.
- **Data quality report:** per symbol per day, coverage (time with valid data), gaps, checksum mismatch rate, latency percentiles.
- **Backtest harness:** event loop over replayed book and trade events in `recv_ts` order, with the fill and latency rules above, fees and instrument rules.

## Suggested order

1. Replay loader (implemented); capture exact decimals and instrument metadata for reliable checksum verification.
2. Instrument metadata and live checksum checking.
3. Cold tier compaction and monitoring.
4. Backtest harness and data quality report.
5. More markets, level3 and own-fill data as specific strategies need them.
