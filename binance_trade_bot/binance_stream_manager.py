import sys
import threading
import time
from contextlib import contextmanager
from typing import Dict, Set, Tuple

import binance.client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

from .config import Config
from .logger import Logger


class BinanceOrder:  # pylint: disable=too-few-public-methods
    def __init__(self, report):
        self.event = report
        self.symbol = report["symbol"]
        self.side = report["side"]
        self.order_type = report["order_type"]
        self.id = report["order_id"]
        self.cumulative_quote_qty = float(report["cumulative_quote_asset_transacted_quantity"])
        self.status = report["current_order_status"]
        self.price = float(report["order_price"])
        self.time = report["transaction_time"]
        self.cumulative_filled_quantity = float(report["cumulative_filled_quantity"])

    def __repr__(self):
        return f"<BinanceOrder {self.event}>"


class BinanceCache:  # pylint: disable=too-few-public-methods
    ticker_values: Dict[str, float] = {}
    _balances: Dict[str, float] = {}
    _balances_mutex: threading.Lock = threading.Lock()
    non_existent_tickers: Set[str] = set()
    orders: Dict[str, BinanceOrder] = {}

    @contextmanager
    def open_balances(self):
        with self._balances_mutex:
            yield self._balances


class OrderGuard:
    def __init__(self, pending_orders: Set[Tuple[str, int]], mutex: threading.Lock):
        self.pending_orders = pending_orders
        self.mutex = mutex
        # lock immediately because OrderGuard
        # should be entered and put tag that shouldn't be missed
        self.mutex.acquire()
        self.tag = None

    def set_order(self, origin_symbol: str, target_symbol: str, order_id: int):
        self.tag = (origin_symbol + target_symbol, order_id)

    def __enter__(self):
        try:
            if self.tag is None:
                raise Exception("OrderGuard wasn't properly set")
            self.pending_orders.add(self.tag)
        finally:
            self.mutex.release()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pending_orders.remove(self.tag)


class BinanceStreamManager:
    def __init__(self, cache: BinanceCache, config: Config, binance_client: binance.client.Client, logger: Logger):
        self.cache = cache
        self.logger = logger
        self.bw_api_manager = BinanceWebSocketApiManager(
            output_default="UnicornFy", enable_stream_signal_buffer=True, exchange=f"binance.{config.BINANCE_TLD}"
        )
        self.bw_api_manager.create_stream(
            ["arr"], ["!miniTicker"], api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_API_SECRET_KEY
        )
        self.bw_api_manager.create_stream(
            ["arr"], ["!userData"], api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_API_SECRET_KEY
        )
        self.binance_client = binance_client
        self.pending_orders: Set[Tuple[str, int]] = set()
        self.pending_orders_mutex: threading.Lock = threading.Lock()
        self._processorThread = threading.Thread(target=self._stream_processor)
        self._processorThread.start()

    def acquire_order_guard(self):
        return OrderGuard(self.pending_orders, self.pending_orders_mutex)

    def _fetch_pending_orders(self):
        pending_orders: Set[Tuple[str, int]]
        with self.pending_orders_mutex:
            pending_orders = self.pending_orders.copy()
        for (symbol, order_id) in pending_orders:
            order = None
            while True:
                try:
                    order = self.binance_client.get_order(symbol=symbol, orderId=order_id)
                except (BinanceRequestException, BinanceAPIException) as e:
                    self.logger.error(f"Got exception during fetching pending order: {e}")
                if order is not None:
                    break
                time.sleep(1)
            fake_report = {
                "symbol": order["symbol"],
                "side": order["side"],
                "order_type": order["type"],
                "order_id": order["orderId"],
                "cumulative_quote_asset_transacted_quantity": float(order["cummulativeQuoteQty"]),
                "cumulative_filled_quantity": float(order["executedQty"]),
                "current_order_status": order["status"],
                "order_price": float(order["price"]),
                "transaction_time": order["time"],
            }
            self.logger.info(f"Pending order {order_id} for symbol {symbol} fetched:\n{fake_report}", False)
            self.cache.orders[fake_report["order_id"]] = BinanceOrder(fake_report)

    def _invalidate_balances(self):
        with self.cache.open_balances() as balances:
            balances.clear()

    def _stream_processor(self):
        while True:
            if self.bw_api_manager.is_manager_stopping():
                sys.exit()

            stream_signal = self.bw_api_manager.pop_stream_signal_from_stream_signal_buffer()
            stream_data = self.bw_api_manager.pop_stream_data_from_stream_buffer()

            if stream_signal is not False:
                signal_type = stream_signal["type"]
                stream_id = stream_signal["stream_id"]
                if signal_type == "CONNECT":
                    stream_info = self.bw_api_manager.get_stream_info(stream_id)
                    if "!userData" in stream_info["markets"]:
                        self.logger.debug("Connect for userdata arrived", False)
                        self._fetch_pending_orders()
                        self._invalidate_balances()
            if stream_data is not False:
                self._process_stream_data(stream_data)
            if stream_data is False and stream_signal is False:
                time.sleep(0.01)

    def _process_stream_data(self, stream_data):
        event_type = stream_data["event_type"]
        if event_type == "executionReport":  # !userData
            self.logger.debug(f"execution report: {stream_data}")
            order = BinanceOrder(stream_data)
            self.cache.orders[order.id] = order
        elif event_type == "balanceUpdate":  # !userData
            self.logger.debug(f"Balance update: {stream_data}")
            with self.cache.open_balances() as balances:
                asset = stream_data["asset"]
                if asset in balances:
                    del balances[stream_data["asset"]]
        elif event_type in {"outboundAccountPosition", "outboundAccountInfo"}:  # !userData
            self.logger.debug(f"{event_type}: {stream_data}")
            with self.cache.open_balances() as balances:
                for bal in stream_data["balances"]:
                    balances[bal["asset"]] = float(bal["free"])
        elif event_type == "24hrMiniTicker":
            for event in stream_data["data"]:
                self.cache.ticker_values[event["symbol"]] = float(event["close_price"])
        else:
            self.logger.error(f"Unknown event type found: {event_type}\n{stream_data}")

    def close(self):
        self.bw_api_manager.stop_manager_with_all_streams()
