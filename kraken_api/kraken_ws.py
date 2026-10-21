import asyncio
import json
import websockets
import logging
from typing import Dict, List, Callable, Optional, Any
from pathlib import Path
import yaml
import hashlib
import hmac
import base64
import time
import urllib.parse
import sys

from kraken_api.account import KrakenAccount
from kraken_api.markets import KrakenMarkets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KrakenWebSocketError(Exception):
    """Custom exception for KrakenWebSocket errors"""
    pass

class KrakenWebSocket:
    """
    Kraken WebSocket client for real-time data streaming and trading.
    Updated to use v2 API endpoints consistently.
    """
    
    # Updated to v2 endpoints
    WS_PUBLIC_URL = "wss://ws.kraken.com/v2"
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, auto_reconnect: bool = False):
        self.public_ws = None
        self.is_connected = False
        self.auto_reconnect = auto_reconnect  # Add flag to control reconnection behavior
        
        self.handlers: Dict[str, List[Callable]] = {}
        self.subscriptions: Dict[str, Dict] = {}
        
        # Initialize components - disable auto-reconnect if we want simple error handling
        self.account = KrakenAccount(api_key, api_secret)
        # You may need to disable auto-reconnect on the account object too
        # if hasattr(self.account, 'auto_reconnect'):
        #     self.account.auto_reconnect = auto_reconnect
        
        self.markets = KrakenMarkets()
        
        self._running = False

    def check_connection(self) -> bool:
        """
        Check if the WebSocket connections are active.
        
        Returns:
            bool: True if public connection is active, False otherwise.
                  Also considers account connection status if account has credentials.
        """
        # Check public connection
        public_connected = (
            self.public_ws is not None and 
            self.is_connected
        )
        
        # If no API credentials, only check public connection
        if not (hasattr(self.account, 'api_key') and self.account.api_key):
            return public_connected
        
        # If API credentials exist, check both public and private connections
        account_connected = self.account.connected() if hasattr(self.account, 'connected') else False
        
        return public_connected and account_connected
        
    async def connect(self, private: bool = False):
        """
        Connects to the necessary WebSocket endpoints.
        - Public connection for market data (v2).
        - Private connection is handled by the account object (v2).
        """
        if not self.public_ws:
            try:
                self.public_ws = await websockets.connect(self.WS_PUBLIC_URL)
                self.is_connected = True
                logger.info("Connected to Kraken public WebSocket v2 for market data.")
            except Exception as e:
                raise KrakenWebSocketError(f"Failed to connect to public WebSocket: {e}")

        if private:
            try:
                # The account object manages its own v2 connection
                await self.account.connect_v2()
            except Exception as e:
                raise KrakenWebSocketError(f"Failed to connect to private WebSocket: {e}")
    
    def add_handler(self, event_type: str, handler: Callable):
        """Add a message handler for a specific data type (e.g., 'book', 'trade')."""
        if event_type not in self.handlers:
            self.handlers[event_type] = []
        self.handlers[event_type].append(handler)
        logger.info(f"Added handler for {event_type}")

    # --- Public Market Data Subscriptions (v2 format) ---

    async def subscribe_book(self, symbols: List[str], depth: int = 10, handler: Optional[Callable] = None):
        """Subscribe to order book data using v2 format."""
        if handler:
            self.add_handler('book', handler)
        
        # v2 subscription format
        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": symbols,
                "depth": depth
            }
        }
        
        await self._send_public_subscription(subscription)
        self.subscriptions['book'] = subscription
        logger.info(f"Subscribed to order book for symbols: {symbols}, depth: {depth}")

    # --- Public Market Data Unsubscription Methods (v2 format) ---

    async def unsubscribe_book(self, symbols: List[str], depth: int = 10):
        """Unsubscribe from order book data using v2 format.

        Kraken finds the subscription by its depth, so ``depth`` must equal the
        depth that was subscribed. A different depth is refused with
        "Subscription with depth N not Found".
        """
        subscription = {
            "method": "unsubscribe",
            "params": {
                "channel": "book",
                "symbol": symbols,
                "depth": depth
            }
        }
        
        await self._send_public_subscription(subscription)
        
        # Remove from stored subscriptions
        if 'book' in self.subscriptions:
            del self.subscriptions['book']
        
        logger.info(f"Unsubscribed from order book for symbols: {symbols}")

    async def unsubscribe_trades(self, symbols: List[str]):
        """Unsubscribe from trade data using v2 format."""
        subscription = {
            "method": "unsubscribe",
            "params": {
                "channel": "trade",
                "symbol": symbols
            }
        }
        
        await self._send_public_subscription(subscription)
        
        # Remove from stored subscriptions
        if 'trade' in self.subscriptions:
            del self.subscriptions['trade']
        
        logger.info(f"Unsubscribed from trades for symbols: {symbols}")

    async def unsubscribe_ticker(self, symbols: List[str]):
        """Unsubscribe from ticker data using v2 format."""
        subscription = {
            "method": "unsubscribe",
            "params": {
                "channel": "ticker",
                "symbol": symbols
            }
        }
        
        await self._send_public_subscription(subscription)
        
        # Remove from stored subscriptions
        if 'ticker' in self.subscriptions:
            del self.subscriptions['ticker']
        
        logger.info(f"Unsubscribed from ticker for symbols: {symbols}")

    async def unsubscribe_ohlc(self, symbols: List[str], interval: int = 1):
        """Unsubscribe from OHLC data using v2 format."""
        subscription = {
            "method": "unsubscribe",
            "params": {
                "channel": "ohlc",
                "symbol": symbols,
                "interval": interval
            }
        }
        
        await self._send_public_subscription(subscription)
        
        # Remove from stored subscriptions
        if 'ohlc' in self.subscriptions:
            del self.subscriptions['ohlc']
        
        logger.info(f"Unsubscribed from OHLC for symbols: {symbols}, interval: {interval}")

    async def unsubscribe_all_public(self):
        """Unsubscribe from all public market data subscriptions."""
        channels_to_unsubscribe = list(self.subscriptions.keys())
        
        for channel in channels_to_unsubscribe:
            if channel in ['book', 'trade', 'ticker', 'ohlc']:
                subscription_info = self.subscriptions[channel]
                symbols = subscription_info['params']['symbol']
                
                if channel == 'book':
                    await self.unsubscribe_book(symbols)
                elif channel == 'trade':
                    await self.unsubscribe_trades(symbols)
                elif channel == 'ticker':
                    await self.unsubscribe_ticker(symbols)
                elif channel == 'ohlc':
                    interval = subscription_info['params'].get('interval', 1)
                    await self.unsubscribe_ohlc(symbols, interval)
        
        logger.info("Unsubscribed from all public market data subscriptions")

    def remove_handler(self, event_type: str, handler: Optional[Callable] = None):
        """
        Remove a message handler for a specific data type.
        If handler is None, removes all handlers for that event type.
        """
        if event_type in self.handlers:
            if handler is None:
                # Remove all handlers for this event type
                del self.handlers[event_type]
                logger.info(f"Removed all handlers for {event_type}")
            else:
                # Remove specific handler
                if handler in self.handlers[event_type]:
                    self.handlers[event_type].remove(handler)
                    logger.info(f"Removed specific handler for {event_type}")
                    
                    # If no handlers left, remove the event type
                    if not self.handlers[event_type]:
                        del self.handlers[event_type]

    async def subscribe_trades(self, symbols: List[str], handler: Optional[Callable] = None):
        """Subscribe to trade data using v2 format."""
        if handler:
            self.add_handler('trade', handler)
        
        # v2 subscription format
        subscription = {
            "method": "subscribe", 
            "params": {
                "channel": "trade",
                "symbol": symbols
            }
        }
        
        await self._send_public_subscription(subscription)
        self.subscriptions['trade'] = subscription
        logger.info(f"Subscribed to trades for symbols: {symbols}")

    async def subscribe_ticker(self, symbols: List[str], handler: Optional[Callable] = None):
        """Subscribe to ticker data using v2 format."""
        if handler:
            self.add_handler('ticker', handler)
        
        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "ticker", 
                "symbol": symbols
            }
        }
        
        await self._send_public_subscription(subscription)
        self.subscriptions['ticker'] = subscription
        logger.info(f"Subscribed to ticker for symbols: {symbols}")

    async def subscribe_ohlc(self, symbols: List[str], interval: int = 1, handler: Optional[Callable] = None):
        """Subscribe to OHLC data using v2 format."""
        if handler:
            self.add_handler('ohlc', handler)
        
        subscription = {
            "method": "subscribe",
            "params": {
                "channel": "ohlc",
                "symbol": symbols,
                "interval": interval
            }
        }
        
        await self._send_public_subscription(subscription)
        self.subscriptions['ohlc'] = subscription
        logger.info(f"Subscribed to OHLC for symbols: {symbols}, interval: {interval}")

    # --- Private Data Subscriptions (delegated to account v2) ---

    async def subscribe_own_trades(self, 
                                   snapshot: bool = True, 
                                   consolidate_taker: bool = True,
                                   handler: Optional[Callable] = None):
        """
        Subscribe to own trades data stream using v2 format.
        """
        # Ensure the account is connected to v2
        if not self.account.connected():
            await self.account.connect_v2()
        
        await self.account.subscribe_own_trades(
            snap_trades=snapshot,
            consolidate_taker=consolidate_taker,
            handler=handler
        )

    async def subscribe_open_orders(self, handler: Optional[Callable] = None):
        """Subscribe to open orders data stream using v2 format."""
        if not self.account.connected():
            await self.account.connect_v2()
        
        await self.account.subscribe_open_orders(handler=handler)

    async def unsubscribe_own_trades(self):
        """Unsubscribe from own trades data stream."""
        await self.account.unsubscribe_own_trades()

    async def unsubscribe_open_orders(self):
        """Unsubscribe from open orders data stream."""
        await self.account.unsubscribe_open_orders()

    # --- Cancel All Orders After (Dead Man's Switch) Methods ---

    async def set_cancel_all_orders_after(self, timeout: int) -> Dict:
        """
        Set up a "Dead Man's Switch" that will cancel all orders after the specified timeout.
        This must be called periodically to reset the timer and prevent cancellation.
        
        Args:
            timeout: Duration in seconds to set/extend the timer (max 86400 seconds).
                    Set to 0 to disable the mechanism.
        
        Returns:
            Response from the server with currentTime and triggerTime.
        """
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.set_cancel_all_orders_after(timeout)

    async def enable_cancel_on_disconnect(self, timeout: int = 60) -> Dict:
        """
        Enable the "Dead Man's Switch" mechanism with the specified timeout.
        
        Args:
            timeout: Duration in seconds (recommended: 60). Must be > 0.
        
        Returns:
            Response from the server.
        """
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.enable_cancel_on_disconnect(timeout)

    async def disable_cancel_on_disconnect(self) -> Dict:
        """
        Disable the "Dead Man's Switch" mechanism.
        
        Returns:
            Response from the server.
        """
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.disable_cancel_on_disconnect()

    async def reset_cancel_timer(self, timeout: int = 60) -> Dict:
        """
        Reset the cancel timer to prevent order cancellation.
        Should be called periodically (every 15-30 seconds recommended).
        
        Args:
            timeout: Duration in seconds to reset the timer to.
        
        Returns:
            Response from the server.
        """
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.reset_cancel_timer(timeout)

    async def get_cancel_on_disconnect_status(self) -> Dict:
        """
        Get the current cancel on disconnect status.
        
        Returns:
            Dict containing enabled status and timeout (if applicable).
        """
        return await self.account.get_cancel_on_disconnect_status()

    async def set_cancel_on_disconnect(self, enabled: bool, timeout: int = 60) -> Dict:
        """
        Convenience method to enable or disable the cancel mechanism.
        
        Args:
            enabled: Whether to enable or disable the mechanism
            timeout: Duration in seconds when enabling (ignored when disabling)
        
        Returns:
            Response from the server.
        """
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.set_cancel_on_disconnect(enabled, timeout)

    async def start_cancel_timer_task(self, timeout: int = 60, reset_interval: int = 30) -> asyncio.Task:
        """
        Start a background task that automatically resets the cancel timer.
        
        Args:
            timeout: Duration in seconds for the cancel timer
            reset_interval: How often to reset the timer (in seconds)
        
        Returns:
            The asyncio Task that can be cancelled to stop the auto-reset
        """
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.start_cancel_timer_task(timeout, reset_interval)

    # --- Enhanced Trading Methods (v2 API) ---

    async def add_order_v2(self, symbol: str, side: str, order_type: str, 
                          order_qty: str, limit_price: Optional[str] = None,
                          display_qty: Optional[str] = None, validate: bool = False, 
                          **kwargs) -> Dict:
        """
        Places orders using the v2 API format.
        Supports all order types including iceberg orders.
        """
        if not self.account.connected():
            await self.account.connect_v2()
            
        return await self.account.add_order_v2(
            symbol=symbol,
            side=side,
            order_type=order_type,
            order_qty=order_qty,
            limit_price=limit_price,
            display_qty=display_qty,
            validate=validate,
            **kwargs
        )

    async def amend_order_v2(self, order_id: Optional[str] = None, 
                            cl_ord_id: Optional[str] = None,
                            order_qty: Optional[str] = None,
                            limit_price: Optional[str] = None,
                            display_qty: Optional[str] = None,
                            **kwargs) -> Dict:
        """
        Amends an order using v2 API format.
        """
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.amend_order_v2(
            order_id=order_id,
            cl_ord_id=cl_ord_id,
            order_qty=order_qty,
            limit_price=limit_price,
            display_qty=display_qty,
            **kwargs
        )

    async def cancel_order_v2(self, order_id: Optional[str] = None, 
                             cl_ord_id: Optional[str] = None) -> Dict:
        """Cancel an order using v2 API format."""
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.cancel_order_v2(order_id=order_id, cl_ord_id=cl_ord_id)

    async def cancel_all_orders_v2(self) -> Dict:
        """Cancel all orders using v2 API format."""
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.cancel_all_orders_v2()

    # --- Legacy v1 methods for backward compatibility ---

    async def add_order(self, pair: str, type: str, ordertype: str, volume: str,
                       price: Optional[str] = None, validate: bool = False, **kwargs) -> Dict:
        """
        Legacy method for backward compatibility.
        Internally converts to v2 format.
        """
        # Convert v1 parameters to v2 format
        symbol = pair  # May need symbol mapping
        side = type  # "buy" or "sell"
        order_type = ordertype  # "limit", "market", etc.
        order_qty = volume
        limit_price = price
        
        return await self.add_order_v2(
            symbol=symbol,
            side=side,
            order_type=order_type,
            order_qty=order_qty,
            limit_price=limit_price,
            validate=validate,
            **kwargs
        )

    async def cancel_order(self, txid):
        """Cancel an order (legacy v1 method)."""
        return await self.cancel_order_v2(order_id=txid)

    async def cancel_all_orders(self):
        """Cancel all orders (legacy v1 method)."""
        return await self.cancel_all_orders_v2()

    # --- Account Information Methods ---

    async def get_balance(self) -> Dict[str, str]:
        """Get account balance via the account object."""
        if not self.account.connected():
            await self.account.connect_v2()
        
        return await self.account.get_balance()

    async def _send_public_subscription(self, subscription: Dict):
        """Sends a subscription message to the public WebSocket v2."""
        if not self.is_connected or not self.public_ws:
            await self.connect()
        
        # Add req_id for v2 format
        req_id = int(time.time() * 1000)
        subscription["req_id"] = req_id
        
        try:
            await self.public_ws.send(json.dumps(subscription))
        except websockets.exceptions.ConnectionClosed as e:
            raise KrakenWebSocketError(f"Failed to send subscription - connection closed: {e}")
        except Exception as e:
            raise KrakenWebSocketError(f"Failed to send subscription: {e}")

    async def _handle_public_message(self, message: str):
        """Handles incoming public market data messages in v2 format."""
        try:
            data = json.loads(message)
            
            # Handle subscription confirmations and errors
            if isinstance(data, dict) and 'method' in data:
                if data['method'] == 'subscribe':
                    if data.get('success'):
                        logger.info(f"Successfully subscribed: {data}")
                    else:
                        logger.error(f"Subscription failed: {data.get('error', 'Unknown error')}")
                elif data['method'] == 'ping':
                    # Respond to ping
                    pong = {"method": "pong", "req_id": data.get("req_id")}
                    try:
                        await self.public_ws.send(json.dumps(pong))
                    except websockets.exceptions.ConnectionClosed as e:
                        raise KrakenWebSocketError(f"Failed to send pong - connection closed: {e}")
                return

            # Handle data messages - v2 format is different
            if isinstance(data, dict) and 'channel' in data and 'data' in data:
                channel_name = data['channel']
                
                if channel_name in self.handlers:
                    for handler in self.handlers[channel_name]:
                        try:
                            asyncio.create_task(handler(data))
                        except Exception as e:
                            logger.error(f"Error in '{channel_name}' handler: {e}")
            else:
                logger.debug(f"Received message with unexpected format: {data}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Error handling public message: {e}")

    async def run(self):
        """Starts the client and keeps it running."""
        if not self.public_ws:
            raise KrakenWebSocketError("Not connected. Call connect() before running.")
            
        self._running = True
        logger.info("KrakenWebSocket v2 client is running...")
        
        try:
            while self._running:
                try:
                    message = await self.public_ws.recv()
                    await self._handle_public_message(message)
                except websockets.exceptions.ConnectionClosed as e:
                    logger.error(f"WebSocket connection closed: {e}")
                    sys.exit(0)
                    self._running = False
                    if not self.auto_reconnect:
                        raise KrakenWebSocketError(f"WebSocket connection lost: {e}")
                    else:
                        logger.info("Auto-reconnect enabled, attempting to reconnect...")
                        # Add reconnection logic here if needed
                        break
                except Exception as e:
                    logger.error(f"An error occurred in the run loop: {e}")
                    self._running = False
                    raise KrakenWebSocketError(f"Run loop error: {e}")
        finally:
            self._running = False
            logger.info("WebSocket run loop ended")

    async def close(self):
        """Closes all connections."""
        self._running = False
        
        # Close public connection
        if self.public_ws:# and not self.public_ws.closed:
            try:
                await self.public_ws.close()
                logger.info("Public WebSocket connection closed.")
                sys.exit(1)
            except Exception as e:
                logger.warning(f"Error closing public WebSocket: {e}")
            
        # Close private connection via the account object
        try:
            await self.account.close()
        except Exception as e:
            logger.warning(f"Error closing account connection: {e}")