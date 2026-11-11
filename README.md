# TidePool
## OKX market data via the trading gateway

The existing Kraken collector runs by default. To also collect OKX spot books and trades, run a second process using `config.okx.example.yaml` and an authorized running trading gateway. Install the proxy SDK in the same environment:

```bash
pip install 'cuq-proxy-client[websocket] @ git+https://github.com/CUQuants/proxy-client.git@aaa19e99138d6d2a7ec70bc052ec632d5e12cbb9'
python main.py --config config.okx.example.yaml
```

Before starting, set `CUQ_PROXY_URL`, `CUQ_PROXY_API_KEY`, `CUQ_PROXY_SECRET`, `CUQ_PROXY_OPERATOR_ID`, and `CUQ_PROXY_OPERATOR_NAME` in the environment. Do not put credentials in the config file. Configure markets as `BASE/QUOTE` spot pairs supported by OKX.

OKX writes to `data/hot/okx/{book_snapshot,book_delta,trade,gap}`; Kraken stays at `data/hot/{book_snapshot,book_delta,trade,gap}`. Both use the same Parquet columns, symbol notation, receive-time and exchange-time units (nanoseconds), and buy/sell trade sides. OKX trade IDs must fit the existing signed 64-bit field; an unsupported ID causes a reconnect and a recorded gap. OKX has no order type in its public trade feed, so `ord_type` is null.

OKX's `books` channel maintains 400 levels per side in memory. Updates store top-25 deltas, including levels promoted from deeper in the book. A snapshot is stored on connection and at `storage.snapshot_interval_seconds` (default 300; set to 0 to disable periodic checkpoints). Periodic snapshots are taken from the trusted local book, not freshly requested from OKX. OKX sequence gaps or gateway disconnects mark data untrusted until a new exchange snapshot arrives. Checksum is null because OKX no longer supports that check. A gateway already sharing the same subscription may not send a snapshot to a late subscriber; Tidepool retries and records a gap instead of trusting partial updates. Resolving that case requires snapshot support in the gateway.

ByteBit is a robust Python application that streams real-time orderbook and trade data from Kraken's WebSocket API. Built by CU Quants for researchers, traders, and analysts who need reliable, continuous market data collection.

# Data Structure
## Example:
```bash
data/
├── BTC-USD/
│   ├── orderbook/
│   │   └── orderbook_data_2024-06-17.csv
│   └── trades/
│       └── trade_data_2024-06-17.csv
└── ETH-USD/
    ├── orderbook/
    └── trades/
```

## Data Format
```bash
Orderbook CSV: timestamp, 25th_bid_qty, 25th_bid_price, ..., mid_price, best_ask_qty, best_ask_price, ...
```

# System Architecture

## High level
```bash
┌─────────────────────┐
│   Supervisor        │
│   (main.py)         │
│                     │
│ • Manages config    │
│ • Spawns subprocess │
│ • Handles restarts  │
│ • Logs events       │
└──────────┬──────────┘
           │
           │ subprocess
           │
┌──────────▼──────────┐
│   Data Collector    │
│   (collector.py)    │
│                     │
│ • WebSocket client  │
│ • Real-time data    │
│ • CSV file writing  │
│ • Handles sys.exit  │
└─────────────────────┘
```

## Files
```bash
bytebit/
├── README.md
├── requirements.txt
├── config.yaml.example
├── src/
│   ├── main.py              
│   ├── collector.py         
│   ├── config_manager.py    
│   ├── file_manager.py      
│   ├── kraken_client.py     
│   └── utils.py            
├── data/                   
│   ├── BTC-USD/
│   │   ├── orderbook/
│   │   │   └── orderbook_data_2024-06-17.csv
│   │   └── trades/
│   │       └── trade_data_2024-06-17.csv
│   └── ETH-USD/
│       ├── orderbook/
│       └── trades/
└── logs/
    └── bytebit.log
```

