"""
TidePool Data Collector
Real-time orderbook and trade data collector for the TidePool dataset.
Maintains full orderbook state and applies incremental updates.
"""
from kraken_api.kraken_ws import KrakenWebSocket
from src.storage.hot_writer import BOOK_DEPTH, HotParquetWriter, parse_ts_ns
import asyncio
import datetime
import json
import logging
import sys
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

class TidePoolCollector:
    """
    TidePool Data Collector
    Real-time orderbook and trade data collector for the TidePool dataset.
    Maintains full orderbook state in memory.
    """
    def __init__(self, markets: list[str], storage_cfg: Optional[Dict[str, Any]] = None):
        """
        Initialize the TidePool Collector
        """
        storage_cfg = storage_cfg or {}
        self.markets = markets
        self.client = KrakenWebSocket()
        
        # Dictionary to store full orderbook state for each symbol
        # Format: {symbol: {'bids': {price: qty}, 'asks': {price: qty}}}
        self.orderbooks: Dict[str, Dict[str, Dict[float, float]]] = {}
        
        # Rows go websocket handler -> queue -> hot-tier Parquet writer task
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=storage_cfg.get('queue_maxsize', 200_000))
        self.writer = HotParquetWriter(
            self.queue,
            base_dir=storage_cfg.get('hot_dir', 'data/hot'),
            roll_interval_s=storage_cfg.get('roll_interval_seconds', 60),
            max_rows=storage_cfg.get('max_rows_per_file', 500_000),
            compression=storage_cfg.get('compression', 'lz4'),
        )
        self.dropped_rows = 0
        self._last_drop_log = 0.0
        
        # Set up the message handler
        self.client.on_message = self.handle_message
    
    async def handle_message(self, message):
        """
        Handle incoming WebSocket messages
        """
        try:
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            # Handle the new message format
            if isinstance(data, dict) and 'channel' in data and 'type' in data and 'data' in data:
                if data['type'] == 'snapshot':
                    # Initial full orderbook snapshot
                    await self.process_snapshot_message(data)
                elif data['type'] == 'update':
                    await self.process_update_message(data)
            elif isinstance(data, dict) and 'event' in data:
                # Handle other message types (heartbeat, status, etc.)
                print(f"Event: {data['event']}")
                    
        except Exception as e:
            print(f"Error handling message: {e}")
    
    async def process_snapshot_message(self, message: dict):
        """
        Process initial snapshot messages to build full orderbook state
        """
        try:
            channel = message['channel']
            message_data = message['data']
            
            if channel == 'book':
                for item in message_data:
                    symbol = item.get('symbol', '')
                    timestamp = item.get('timestamp', datetime.datetime.utcnow().isoformat() + 'Z')
                    
                    # Initialize orderbook for this symbol
                    self.orderbooks[symbol] = {'bids': {}, 'asks': {}}
                    
                    # Process bids
                    for bid in item.get('bids', []):
                        price = float(bid['price'])
                        qty = float(bid['qty'])
                        self.orderbooks[symbol]['bids'][price] = qty
                    
                    # Process asks
                    for ask in item.get('asks', []):
                        price = float(ask['price'])
                        qty = float(ask['qty'])
                        self.orderbooks[symbol]['asks'][price] = qty
                    
                    print(f"Initialized orderbook for {symbol} with {len(self.orderbooks[symbol]['bids'])} bids and {len(self.orderbooks[symbol]['asks'])} asks")
                    
                    # Save the full orderbook snapshot
                    await self.enqueue_orderbook_snapshot(symbol, timestamp)
                    
        except Exception as e:
            print(f"Error processing snapshot message: {e}")
    
    async def process_update_message(self, message: dict):
        """
        Process update messages and apply changes to orderbook state
        """
        try:
            channel = message['channel']
            message_data = message['data']
            
            # Process each item in the data array
            for item in message_data:
                symbol = item.get('symbol', '')
                timestamp = item.get('timestamp', datetime.datetime.utcnow().isoformat() + 'Z')
                
                if channel == 'book':
                    await self.update_orderbook(symbol, item, timestamp)
                elif channel == 'trade':
                    await self.enqueue_trade(symbol, item, timestamp)
                    
        except Exception as e:
            print(f"Error processing update message: {e}")
    
    async def update_orderbook(self, symbol: str, orderbook_item: dict, timestamp: str):
        """
        Update orderbook state with incremental changes and save current state
        """
        try:
            # Ensure we have an orderbook for this symbol
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = {'bids': {}, 'asks': {}}
                print(f"Warning: No snapshot received for {symbol}, initializing empty orderbook")
            
            # Process bid updates
            for bid in orderbook_item.get('bids', []):
                price = float(bid['price'])
                qty = float(bid['qty'])
                
                if qty == 0:
                    # Remove this price level
                    self.orderbooks[symbol]['bids'].pop(price, None)
                else:
                    # Update/add this price level
                    self.orderbooks[symbol]['bids'][price] = qty
            
            # Process ask updates
            for ask in orderbook_item.get('asks', []):
                price = float(ask['price'])
                qty = float(ask['qty'])
                
                if qty == 0:
                    # Remove this price level
                    self.orderbooks[symbol]['asks'].pop(price, None)
                else:
                    # Update/add this price level
                    self.orderbooks[symbol]['asks'][price] = qty
            
            # Save the updated orderbook state
            await self.enqueue_orderbook_snapshot(symbol, timestamp)
            
        except Exception as e:
            print(f"Error updating orderbook for {symbol}: {e}")
    
    def _enqueue(self, stream: str, row: dict):
        """
        Hand a row to the Parquet writer without blocking the websocket loop.
        Drops the row if the queue is full (writer can't keep up).
        """
        try:
            self.queue.put_nowait((stream, row))
        except asyncio.QueueFull:
            self.dropped_rows += 1
            now = time.monotonic()
            if now - self._last_drop_log >= 10:
                logger.warning(f"Write queue full, dropped {self.dropped_rows} rows so far")
                self._last_drop_log = now

    def _row_times(self, timestamp: str) -> tuple:
        """Return (ts, recv_ts) in ns, falling back to receive time if the exchange timestamp is bad."""
        recv_ts = time.time_ns()
        try:
            return parse_ts_ns(timestamp), recv_ts
        except (TypeError, ValueError):
            return recv_ts, recv_ts

    async def enqueue_orderbook_snapshot(self, symbol: str, timestamp: str):
        """
        Enqueue current top 25 bids and asks as a single row
        """
        try:
            if symbol not in self.orderbooks:
                return

            orderbook = self.orderbooks[symbol]

            # Get top 25 bids (highest prices first) and top 25 asks (lowest prices first)
            top_bids = sorted(orderbook['bids'].keys(), reverse=True)[:BOOK_DEPTH]
            top_asks = sorted(orderbook['asks'].keys())[:BOOK_DEPTH]

            ts, recv_ts = self._row_times(timestamp)
            row = {'ts': ts, 'recv_ts': recv_ts, 'symbol': symbol}

            # Level 1 is the best price; missing levels stay null
            for i, price in enumerate(top_bids, start=1):
                row[f'bid_{i}_price'] = price
                row[f'bid_{i}_qty'] = orderbook['bids'][price]
            for i, price in enumerate(top_asks, start=1):
                row[f'ask_{i}_price'] = price
                row[f'ask_{i}_qty'] = orderbook['asks'][price]

            self._enqueue('book', row)

        except Exception as e:
            logger.error(f"Error enqueuing orderbook snapshot for {symbol}: {e}")

    async def enqueue_trade(self, symbol: str, trade_item: dict, timestamp: str):
        """
        Enqueue a trade row
        """
        try:
            ts, recv_ts = self._row_times(timestamp)
            price = trade_item.get('price')
            qty = trade_item.get('qty')
            trade_id = trade_item.get('trade_id')

            self._enqueue('trade', {
                'ts': ts,
                'recv_ts': recv_ts,
                'symbol': symbol,
                'side': trade_item.get('side'),
                'price': float(price) if price is not None else None,
                'qty': float(qty) if qty is not None else None,
                'ord_type': trade_item.get('ord_type'),
                'trade_id': int(trade_id) if trade_id is not None else None,
            })

        except Exception as e:
            logger.error(f"Error enqueuing trade for {symbol}: {e}")

    async def run(self) -> None:
        """
        Run the TidePool Collector
        """
        self.writer.start()
        try:
            # Connect to WebSocket
            await self.client.connect()
           
            await self.client.subscribe_book(self.markets, depth=25, handler=self.handle_message)
            await self.client.subscribe_trades(self.markets, handler=self.handle_message)
            await self.client.run()
            await self.client.close()
           
        except Exception as e:
            print(f"Error in run method: {e}")
            sys.exit(1)
        finally:
            # Flush buffered rows on every exit path, including the sys.exit calls in KrakenWebSocket
            await self.writer.stop()
