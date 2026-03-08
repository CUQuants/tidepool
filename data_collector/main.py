"""
Kraken US - Historical trade fetcher
Fetches all raw trades from the Trades endpoint and saves to:
  ./data/{PAIR}/{PAIR}_part001.csv, part002.csv, ...
  (new file every 100MB)

Usage:
    python kraken_fetch.py

Requirements:
    pip install requests
"""

import requests
import time
import os
import csv
from datetime import datetime, timezone, timedelta

# ── Configuration ─────────────────────────────────────────────────────────────

PAIRS = [
    "MOODENGUSD",
    "AVAXUSD",
    "DOTUSD",
    "ARBUSD",
    "OPUSD",
    "APTUSD",
    "SUIUSD"
]

OUTPUT_DIR     = "./data"
DAYS_BACK      = 30
REQUEST_DELAY  = 1.2
MAX_FILE_BYTES = 40 * 1024 * 1024  # 40 MB

# ── Kraken API ────────────────────────────────────────────────────────────────

TRADES_URL = "https://api.kraken.com/0/public/Trades"

def fetch_trades(pair: str, since_ns: int) -> tuple[list, int]:
    """
    Fetch up to 1000 trades starting from `since_ns` (Unix nanoseconds).
    Returns (trades, last_ns) for pagination.
    Each trade: [price, volume, time, side, type, misc]
    """
    params = {"pair": pair, "since": since_ns, "count": 1000}
    resp = requests.get(TRADES_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise ValueError(f"Kraken API error for {pair}: {data['error']}")
    result   = data["result"]
    pair_key = [k for k in result.keys() if k != "last"][0]
    return result[pair_key], int(result["last"])

# ── File writer ───────────────────────────────────────────────────────────────

HEADERS = ["timestamp", "price", "volume", "side", "type"]

class RotatingCsvWriter:
    """Writes trades to CSV files, rotating to a new file every MAX_FILE_BYTES."""

    def __init__(self, pair: str):
        self.pair      = pair
        self.pair_dir  = os.path.join(OUTPUT_DIR, pair)
        os.makedirs(self.pair_dir, exist_ok=True)
        self.part      = 0
        self.file      = None
        self.writer    = None
        self.filepath  = None
        self._open_next()

    def _open_next(self):
        if self.file:
            self.file.close()
        self.part += 1
        self.filepath = os.path.join(self.pair_dir, f"{self.pair}_part{self.part:03d}.csv")
        self.file   = open(self.filepath, "w", newline="")
        self.writer = csv.DictWriter(self.file, fieldnames=HEADERS)
        self.writer.writeheader()

    def write(self, trades: list):
        for t in trades:
            ts = datetime.fromtimestamp(float(t[2]), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            self.writer.writerow({
                "timestamp": ts,
                "price":     t[0],
                "volume":    t[1],
                "side":      t[3],  # b=buy, s=sell
                "type":      t[4],  # m=market, l=limit
            })
            # Rotate if file has hit the size limit
            if self.file.tell() >= MAX_FILE_BYTES:
                self._open_next()

    def close(self):
        if self.file:
            self.file.close()

    @property
    def parts(self):
        return self.part

# ── Main ──────────────────────────────────────────────────────────────────────

def fetch_pair(pair: str) -> int:
    print(f"\n{'='*60}")
    print(f"  Fetching: {pair}")
    print(f"{'='*60}")

    start_ns = int((datetime.now(tz=timezone.utc) - timedelta(days=DAYS_BACK)).timestamp() * 1e9)
    end_ns   = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
    since_ns = start_ns

    writer        = RotatingCsvWriter(pair)
    total_trades  = 0
    request_count = 0
    retries       = 0

    while since_ns < end_ns:
        request_count += 1
        since_dt = datetime.fromtimestamp(since_ns / 1e9, tz=timezone.utc)
        print(f"  req {request_count:>6} | {since_dt.strftime('%Y-%m-%d %H:%M')} | "
              f"trades: {total_trades:>10,} | part: {writer.parts:>3}", end="\r", flush=True)

        try:
            trades, last_ns = fetch_trades(pair, since_ns)
            retries = 0
        except Exception as e:
            retries += 1
            wait = min(60, 5 * retries)
            print(f"\n  Warning: {e} — retry {retries} in {wait}s")
            time.sleep(wait)
            continue

        if not trades or last_ns <= since_ns:
            break

        writer.write(trades)
        total_trades += len(trades)
        since_ns = last_ns
        time.sleep(REQUEST_DELAY)

    writer.close()
    print(f"\n  Done: {total_trades:,} trades across {writer.parts} file(s)")
    return total_trades


def main():
    start_day = (datetime.now(tz=timezone.utc) - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    today     = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    print(f"Kraken Trade Fetcher")
    print(f"Pairs:      {len(PAIRS)}")
    print(f"Range:      {start_day} to {today}")
    print(f"Max CSV:    {MAX_FILE_BYTES // 1024 // 1024}MB per file")
    print(f"Output:     {os.path.abspath(OUTPUT_DIR)}\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    grand_total = 0

    for pair in PAIRS:
        grand_total += fetch_pair(pair)

    print(f"\n{'='*60}")
    print(f"  All done! Grand total: {grand_total:,} trades")
    print(f"  Saved to: {os.path.abspath(OUTPUT_DIR)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()