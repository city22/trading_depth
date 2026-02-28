import asyncio
import json
import logging
import time
from collections import defaultdict, deque

import ccxt.pro as ccxtpro
from fastapi import WebSocket

logger = logging.getLogger(__name__)

SUPPORTED_EXCHANGES = [
    # Tier-1 volume
    "binance", "okx", "bybit",
    # High-volume additions
    "gate", "htx", "kucoin", "mexc", "bitget",
    "phemex", "bingx", "bitfinex", "upbit", "cryptocom",
    # Lower USDT volume but kept for reference
    "kraken", "coinbase",
]
BROADCAST_INTERVAL = 0.2  # seconds


def _price_key(price: float, precision: float) -> str:
    """Format a price as a string key matching the given precision."""
    if precision >= 1:
        decimals = 0
    else:
        import math
        decimals = max(0, -int(math.floor(math.log10(precision))))
    return f"{price:.{decimals}f}"


def _round_to_precision(price: float, precision: float) -> float:
    return round(price / precision) * precision


class ExchangeManager:
    def __init__(self):
        self.supported_exchanges: list[str] = []
        self._exchanges: dict[str, object] = {}

        # Per-(exchange, symbol) orderbook: {bids: {price: size}, asks: {price: size}}
        self._orderbooks: dict[tuple, dict] = {}
        # Per-(exchange, symbol) raw trades: deque of (timestamp_ms, price, amount, side)
        self._raw_trades: dict[tuple, deque] = defaultdict(lambda: deque(maxlen=10000))
        # Per-(exchange, symbol) last price
        self._last_prices: dict[tuple, float] = {}

        # Registered WebSocket clients with their config
        self._clients: dict[WebSocket, dict] = {}

        # Running watch tasks keyed by (ex_id, symbol, type)
        self._watch_tasks: dict[tuple, asyncio.Task] = {}

        self._broadcast_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Initialise supported exchanges."""
        ex_config: dict = {
            "enableRateLimit": True,
        }

        for ex_id in SUPPORTED_EXCHANGES:
            try:
                exchange_class = getattr(ccxtpro, ex_id)
                exchange = exchange_class(ex_config)
                self._exchanges[ex_id] = exchange
                self.supported_exchanges.append(ex_id)
                logger.info("Loaded exchange: %s", ex_id)
            except AttributeError:
                logger.warning("ccxt.pro does not support %s — skipping", ex_id)

        self._broadcast_task = asyncio.create_task(self._broadcast_loop())

    async def stop(self):
        if self._broadcast_task:
            self._broadcast_task.cancel()

        for task in self._watch_tasks.values():
            task.cancel()

        for exchange in self._exchanges.values():
            try:
                await exchange.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Client registration
    # ------------------------------------------------------------------

    def register_client(self, ws: WebSocket):
        self._clients[ws] = {}

    def unregister_client(self, ws: WebSocket):
        self._clients.pop(ws, None)

    async def subscribe(self, ws: WebSocket, msg: dict):
        config = {
            "symbol": msg.get("symbol", "BTC/USDT"),
            "exchanges": msg.get("exchanges", list(self.supported_exchanges)),
            "time_window": float(msg.get("time_window", 5)),
            "precision": float(msg.get("precision", 1)),
        }
        self._clients[ws] = config
        await self._ensure_watches(config["symbol"], config["exchanges"])

    # ------------------------------------------------------------------
    # Watch loops
    # ------------------------------------------------------------------

    async def _ensure_watches(self, symbol: str, exchange_ids: list[str]):
        for ex_id in exchange_ids:
            if ex_id not in self._exchanges:
                continue
            ob_key = (ex_id, symbol, "ob")
            tr_key = (ex_id, symbol, "tr")
            if ob_key not in self._watch_tasks or self._watch_tasks[ob_key].done():
                self._watch_tasks[ob_key] = asyncio.create_task(
                    self._watch_orderbook_loop(ex_id, symbol)
                )
            if tr_key not in self._watch_tasks or self._watch_tasks[tr_key].done():
                self._watch_tasks[tr_key] = asyncio.create_task(
                    self._watch_trades_loop(ex_id, symbol)
                )

    async def _watch_orderbook_loop(self, ex_id: str, symbol: str):
        exchange = self._exchanges[ex_id]
        key = (ex_id, symbol)
        retry_delay = 1.0
        while True:
            try:
                ob = await exchange.watch_order_book(symbol)
                bids: dict[float, float] = {}
                asks: dict[float, float] = {}
                for row in ob.get("bids", []):
                    price, size = row[0], row[1]
                    if size > 0:
                        bids[float(price)] = float(size)
                for row in ob.get("asks", []):
                    price, size = row[0], row[1]
                    if size > 0:
                        asks[float(price)] = float(size)
                self._orderbooks[key] = {"bids": bids, "asks": asks}
                retry_delay = 1.0
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("Orderbook watch error %s %s: %s", ex_id, symbol, e)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    async def _watch_trades_loop(self, ex_id: str, symbol: str):
        exchange = self._exchanges[ex_id]
        key = (ex_id, symbol)
        retry_delay = 1.0
        while True:
            try:
                trades = await exchange.watch_trades(symbol)
                for trade in trades:
                    ts = trade.get("timestamp") or int(time.time() * 1000)
                    price = float(trade.get("price", 0))
                    amount = float(trade.get("amount", 0))
                    side = trade.get("side", "buy")
                    self._raw_trades[key].append((ts, price, amount, side))
                    if price > 0:
                        self._last_prices[key] = price
                retry_delay = 1.0
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("Trades watch error %s %s: %s", ex_id, symbol, e)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def _aggregate_depth(
        self, ex_id: str, symbol: str, precision: float
    ) -> tuple[dict[str, float], dict[str, float]]:
        key = (ex_id, symbol)
        ob = self._orderbooks.get(key, {})
        bids: dict[str, float] = {}
        asks: dict[str, float] = {}
        for price, size in ob.get("bids", {}).items():
            bucket = _round_to_precision(price, precision)
            k = _price_key(bucket, precision)
            bids[k] = bids.get(k, 0.0) + size
        for price, size in ob.get("asks", {}).items():
            bucket = _round_to_precision(price, precision)
            k = _price_key(bucket, precision)
            asks[k] = asks.get(k, 0.0) + size
        return bids, asks

    def _aggregate_trades(
        self, ex_id: str, symbol: str, time_window: float, precision: float
    ) -> tuple[dict[str, float], dict[str, float]]:
        key = (ex_id, symbol)
        trades = self._raw_trades.get(key, deque())
        cutoff_ms = (time.time() - time_window) * 1000
        buys: dict[str, float] = {}
        sells: dict[str, float] = {}
        for ts, price, amount, side in trades:
            if ts < cutoff_ms:
                continue
            bucket = _round_to_precision(price, precision)
            k = _price_key(bucket, precision)
            if side == "buy":
                buys[k] = buys.get(k, 0.0) + amount
            else:
                sells[k] = sells.get(k, 0.0) + amount
        return buys, sells

    # ------------------------------------------------------------------
    # Broadcast loop
    # ------------------------------------------------------------------

    async def _broadcast_loop(self):
        while True:
            try:
                await asyncio.sleep(BROADCAST_INTERVAL)
                if not self._clients:
                    continue

                # Group clients by their subscription config
                # (symbol, exchanges tuple, time_window, precision)
                groups: dict[tuple, list[WebSocket]] = defaultdict(list)
                for ws, cfg in list(self._clients.items()):
                    if not cfg:
                        continue
                    key = (
                        cfg["symbol"],
                        tuple(sorted(cfg["exchanges"])),
                        cfg["time_window"],
                        cfg["precision"],
                    )
                    groups[key].append(ws)

                for (symbol, exchanges, time_window, precision), clients in groups.items():
                    payload = self._build_payload(
                        symbol, list(exchanges), time_window, precision
                    )
                    raw = json.dumps(payload)
                    dead = []
                    for ws in clients:
                        try:
                            await ws.send_text(raw)
                        except Exception:
                            dead.append(ws)
                    for ws in dead:
                        self._clients.pop(ws, None)

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("Broadcast loop error: %s", e)

    def _build_payload(
        self,
        symbol: str,
        exchange_ids: list[str],
        time_window: float,
        precision: float,
    ) -> dict:
        exchanges_data: dict[str, dict] = {}
        last_prices: list[float] = []

        for ex_id in exchange_ids:
            if ex_id not in self._exchanges:
                continue
            key = (ex_id, symbol)
            bids, asks = self._aggregate_depth(ex_id, symbol, precision)
            trade_buys, trade_sells = self._aggregate_trades(
                ex_id, symbol, time_window, precision
            )
            lp = self._last_prices.get(key)
            if lp:
                last_prices.append(lp)
            ob = self._orderbooks.get(key, {})
            raw_bids = ob.get("bids", {})
            raw_asks = ob.get("asks", {})
            best_bid = max(raw_bids.keys()) if raw_bids else None
            best_ask = min(raw_asks.keys()) if raw_asks else None
            exchanges_data[ex_id] = {
                "bids": bids,
                "asks": asks,
                "trade_buys": trade_buys,
                "trade_sells": trade_sells,
                "last_price": lp,
                "best_bid": best_bid,
                "best_ask": best_ask,
            }

        overall_last = (
            sum(last_prices) / len(last_prices) if last_prices else None
        )

        return {
            "type": "update",
            "symbol": symbol,
            "timestamp": time.time(),
            "last_price": overall_last,
            "exchanges": exchanges_data,
        }
