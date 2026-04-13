"""
Binance broker client — implements BrokerClient Protocol.

Supports Binance Spot and Futures (USDM) in both testnet and mainnet modes.

Configuration (via environment variables):
    CRYPTO_BINANCE_API_KEY          — Spot API key
    CRYPTO_BINANCE_API_SECRET       — Spot API secret
    CRYPTO_BINANCE_TESTNET          — "true" to use Spot testnet (default), "false" for mainnet
    CRYPTO_BINANCE_FUTURES          — "true" to use Futures API instead of Spot
    CRYPTO_BINANCE_FUTURES_API_KEY  — Futures API key (demo-fapi.binance.com for testnet)
    CRYPTO_BINANCE_FUTURES_API_SECRET — Futures API secret
"""
import hashlib
import hmac
import time
from typing import Any, Union
from urllib.parse import urlencode

import requests

from app.core.settings import (
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    BINANCE_TESTNET,
    BINANCE_FUTURES,
    BINANCE_FUTURES_API_KEY,
    BINANCE_FUTURES_API_SECRET,
)

# Spot
TESTNET_BASE_URL = "https://testnet.binance.vision"
MAINNET_BASE_URL = "https://api.binance.com"
_ORDER_ENDPOINT = "/api/v3/order"
_ACCOUNT_ENDPOINT = "/api/v3/account"
_ORDER_TEST_ENDPOINT = "/api/v3/order/test"
_TRADES_ENDPOINT = "/api/v3/myTrades"
_TIME_ENDPOINT = "/api/v3/time"

# Futures (USDM)
FUTURES_TESTNET_BASE_URL = "https://demo-fapi.binance.com"
FUTURES_MAINNET_BASE_URL = "https://fapi.binance.com"
_FUTURES_ORDER_ENDPOINT = "/fapi/v1/order"
_FUTURES_ACCOUNT_ENDPOINT = "/fapi/v2/account"
_FUTURES_TRADES_ENDPOINT = "/fapi/v1/userTrades"
_FUTURES_TIME_ENDPOINT = "/fapi/v1/time"
_DEFAULT_RECV_WINDOW_MS = 10000

class BinanceAPIError(RuntimeError):
    """Structured Binance API failure with response details for logging."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str = "",
        response_text: str = "",
        response_json: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.response_text = response_text
        self.response_json = response_json or None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status_code": self.status_code,
            "url": self.url,
            "response_text": self.response_text,
            "response_json": self.response_json,
        }
        if isinstance(self.response_json, dict):
            payload["binance_code"] = self.response_json.get("code")
            payload["binance_msg"] = self.response_json.get("msg")
        return payload

def _response_details(response: requests.Response) -> tuple[str, dict[str, Any] | None]:
    try:
        response_json = response.json()
    except ValueError:
        response_json = None
    return response.text, response_json

def _sign(query_string: str, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

def _weighted_avg_fill_price(fills: list) -> float:
    """Compute weighted-average fill price from Binance fills array."""
    total_qty = sum(float(f["qty"]) for f in fills)
    if total_qty == 0:
        return 0.0
    return sum(float(f["price"]) * float(f["qty"]) for f in fills) / total_qty

def _total_commission(fills: list) -> tuple[float, str]:
    """Sum commission across all partial fills. Returns (amount, asset)."""
    if not fills:
        return 0.0, ""
    asset = fills[0].get("commissionAsset", "")
    total = sum(float(f.get("commission", 0)) for f in fills)
    return total, asset

def _aggregate_trades_for_order(
    trades: list[dict[str, Any]],
    *,
    order_id: str,
    requested_qty: float,
) -> dict[str, Union[str, float, int, None]]:
    """Aggregate Binance userTrades rows for a single order.

    Futures testnet can acknowledge a market order as NEW with executedQty=0,
    while the matching userTrades are already available. Use those trades as the
    source of truth when present; otherwise keep the order unfilled locally.
    """
    matching_trades = [trade for trade in trades if str(trade.get("orderId") or "") == str(order_id)]
    if not matching_trades:
        return {
            "status": "NEW",
            "fill_price": 0.0,
            "fill_qty": 0.0,
            "commission": 0.0,
            "commission_asset": "",
            "quote_qty": None,
            "transact_time": None,
        }

    fill_qty = sum(float(trade.get("qty") or 0) for trade in matching_trades)
    quote_qty_value = sum(float(trade.get("quoteQty") or 0) for trade in matching_trades)
    fill_price = (quote_qty_value / fill_qty) if fill_qty > 0 and quote_qty_value > 0 else 0.0
    commission = sum(float(trade.get("commission") or 0) for trade in matching_trades)
    commission_asset = str(matching_trades[0].get("commissionAsset") or "")
    transact_time = max(int(trade.get("time") or 0) for trade in matching_trades) or None
    requested = float(requested_qty)
    if requested <= 0:
        status = "FILLED" if fill_qty > 0 else "NEW"
    else:
        status = "FILLED" if fill_qty >= requested else "PARTIALLY_FILLED"
    return {
        "status": status,
        "fill_price": fill_price,
        "fill_qty": fill_qty,
        "commission": commission,
        "commission_asset": commission_asset,
        "quote_qty": quote_qty_value or None,
        "transact_time": transact_time,
    }

class BinanceBrokerClient:
    """Live broker client targeting Binance Spot or Futures (USDM) API.

    Defaults to testnet for safety.  Switch to mainnet only after completing
    the full soak validation cycle.

    When futures=True the client uses the /fapi endpoints and handles the
    Futures response format (avgPrice instead of fills array).
    """

    broker_name = "binance"

    def __init__(
        self,
        api_key: str = BINANCE_API_KEY,
        api_secret: str = BINANCE_API_SECRET,
        testnet: bool = BINANCE_TESTNET,
        timeout_seconds: int = 10,
        futures: bool = BINANCE_FUTURES,
        futures_api_key: str = BINANCE_FUTURES_API_KEY,
        futures_api_secret: str = BINANCE_FUTURES_API_SECRET,
    ) -> None:
        self._futures = futures
        if futures:
            self._api_key = futures_api_key or api_key
            self._api_secret = futures_api_secret or api_secret
            self._base_url = FUTURES_TESTNET_BASE_URL if testnet else FUTURES_MAINNET_BASE_URL
            self._order_endpoint = _FUTURES_ORDER_ENDPOINT
            self._account_endpoint = _FUTURES_ACCOUNT_ENDPOINT
        else:
            self._api_key = api_key
            self._api_secret = api_secret
            self._base_url = TESTNET_BASE_URL if testnet else MAINNET_BASE_URL
            self._order_endpoint = _ORDER_ENDPOINT
            self._account_endpoint = _ACCOUNT_ENDPOINT
        self._timeout = timeout_seconds
        self._time_offset_ms = 0

    def _public_request(self, method: str, endpoint: str, params: dict[str, Union[str, int, float]] | None = None) -> Any:
        url = f"{self._base_url}{endpoint}"
        response = requests.request(
            method.upper(),
            url,
            params=params,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response.json()

    def _sync_server_time_offset(self) -> int:
        endpoint = _FUTURES_TIME_ENDPOINT if self._futures else _TIME_ENDPOINT
        data = self._public_request("GET", endpoint)
        server_time = int((data or {}).get("serverTime") or 0)
        if server_time > 0:
            self._time_offset_ms = server_time - int(time.time() * 1000)
        return self._time_offset_ms

    def _current_timestamp_ms(self) -> int:
        return int(time.time() * 1000) + int(self._time_offset_ms)

    def _signed_request(self, method: str, endpoint: str, params: dict[str, Union[str, int, float]]) -> dict:
        if not self._api_key or not self._api_secret:
            raise ValueError(
                "Binance API key and secret must be set via "
                "CRYPTO_BINANCE_API_KEY and CRYPTO_BINANCE_API_SECRET."
            )

        def _perform_request(request_params: dict[str, Union[str, int, float]]) -> dict:
            query_string = urlencode(request_params)
            signature = _sign(query_string, self._api_secret)
            signed_query = f"{query_string}&signature={signature}"
            url = f"{self._base_url}{endpoint}?{signed_query}"
            response = requests.request(
                method.upper(),
                url,
                headers={"X-MBX-APIKEY": self._api_key},
                timeout=self._timeout,
            )
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                response_text, response_json = _response_details(response)
                raise BinanceAPIError(
                    "Binance API request failed",
                    status_code=response.status_code,
                    url=url,
                    response_text=response_text,
                    response_json=response_json,
                ) from exc
            return response.json()

        request_params = dict(params)
        if "timestamp" in request_params:
            request_params["timestamp"] = self._current_timestamp_ms()
        request_params.setdefault("recvWindow", _DEFAULT_RECV_WINDOW_MS)
        try:
            return _perform_request(request_params)
        except BinanceAPIError as exc:
            binance_code = exc.response_json.get("code") if isinstance(exc.response_json, dict) else None
            if binance_code == -1021:
                self._sync_server_time_offset()
                request_params["timestamp"] = self._current_timestamp_ms()
                try:
                    return _perform_request(request_params)
                except BinanceAPIError as retry_exc:
                    exc = retry_exc
            detail = ""
            if isinstance(exc.response_json, dict):
                if exc.response_json.get("code") is not None:
                    detail = f" code={exc.response_json.get('code')}"
                if exc.response_json.get("msg"):
                    detail += f" msg={exc.response_json.get('msg')}"
            raise BinanceAPIError(
                f"Binance API request failed with status {exc.status_code}.{detail}".strip(),
                status_code=exc.status_code,
                url=exc.url,
                response_text=exc.response_text,
                response_json=exc.response_json,
            ) from exc

    def check_account_connectivity(self) -> dict[str, Union[str, bool, int]]:
        timestamp = self._current_timestamp_ms()
        data = self._signed_request("GET", self._account_endpoint, {"timestamp": timestamp})
        if self._futures:
            # Futures account response uses "assets" and "positions" instead of "balances"
            assets = data.get("assets") or []
            return {
                "status": "ok",
                "broker": self.broker_name,
                "account_type": "FUTURES",
                "can_trade": bool(data.get("canTrade", False)),
                "can_deposit": bool(data.get("canDeposit", False)),
                "can_withdraw": bool(data.get("canWithdraw", False)),
                "balance_count": len(assets),
            }
        balances = data.get("balances") or []
        return {
            "status": "ok",
            "broker": self.broker_name,
            "account_type": str(data.get("accountType") or "SPOT"),
            "can_trade": bool(data.get("canTrade", False)),
            "can_deposit": bool(data.get("canDeposit", False)),
            "can_withdraw": bool(data.get("canWithdraw", False)),
            "balance_count": len(balances),
        }

    def check_order_request(
        self,
        symbol: str,
        side: str,
        qty: float,
    ) -> dict[str, Union[str, float, bool]]:
        timestamp = self._current_timestamp_ms()
        params: dict[str, Union[str, int, float]] = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": qty,
            "timestamp": timestamp,
        }
        if self._futures:
            # Futures test endpoint: /fapi/v1/order/test
            self._signed_request("POST", _FUTURES_ORDER_ENDPOINT + "/test", params)
        else:
            self._signed_request("POST", _ORDER_TEST_ENDPOINT, params)
        return {
            "status": "ok",
            "broker": self.broker_name,
            "symbol": symbol,
            "side": side.upper(),
            "qty": float(qty),
            "validated": True,
        }

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        ref_price: float,
    ) -> dict[str, Union[str, float]]:
        """Place a MARKET order on Binance and return fill details.

        Args:
            symbol:    e.g. "BTCUSDT"
            side:      "BUY" or "SELL"
            qty:       order quantity in base asset
            ref_price: reference price (unused for MARKET orders, kept for
                       interface compatibility and logging)

        Returns:
            dict with "status", "fill_price", "fill_qty", and "order_id".

        Raises:
            requests.HTTPError: if Binance returns a non-2xx status.
            ValueError: if required credentials are not configured.
        """
        timestamp = self._current_timestamp_ms()
        params: dict[str, Union[str, int, float]] = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": qty,
            "timestamp": timestamp,
        }
        if self._futures:
            params["newOrderRespType"] = "RESULT"
        else:
            params["newOrderRespType"] = "FULL"
        data = self._signed_request("POST", self._order_endpoint, params)

        if self._futures:
            # Futures API: no fills array; use avgPrice / updateTime
            avg_price = float(data.get("avgPrice") or 0)
            _executed = float(data.get("executedQty") or 0)
            status = str(data.get("status", "UNKNOWN"))
            if _executed > 0:
                quote_qty = float(data.get("cumQuote") or 0) or None
                transact_time = int(data["updateTime"]) if data.get("updateTime") else None
                return {
                    "status": status,
                    "fill_price": avg_price if avg_price > 0 else ref_price,
                    "fill_qty": _executed,
                    "order_id": str(data.get("orderId", "")),
                    "commission": 0.0,
                    "commission_asset": "",
                    "quote_qty": quote_qty,
                    "transact_time": transact_time,
                }

            order_id = str(data.get("orderId", ""))
            trades = self.get_user_trades(
                symbol,
                start_time=timestamp - 300000,
                end_time=self._current_timestamp_ms(),
                limit=1000,
            )
            aggregated = _aggregate_trades_for_order(trades, order_id=order_id, requested_qty=float(qty))
            return {
                "status": str(aggregated["status"] or status),
                "fill_price": float(aggregated["fill_price"] or 0.0),
                "fill_qty": float(aggregated["fill_qty"] or 0.0),
                "order_id": order_id,
                "commission": float(aggregated["commission"] or 0.0),
                "commission_asset": str(aggregated["commission_asset"] or ""),
                "quote_qty": aggregated["quote_qty"],
                "transact_time": aggregated["transact_time"],
            }

        fills = data.get("fills") or []
        fill_price = _weighted_avg_fill_price(fills) if fills else ref_price
        _executed = float(data.get("executedQty") or 0)
        fill_qty = _executed if _executed > 0 else float(qty)
        quote_qty = float(data.get("cummulativeQuoteQty") or 0) or None
        transact_time = int(data["transactTime"]) if data.get("transactTime") else None
        status = str(data.get("status", "UNKNOWN"))
        commission, commission_asset = _total_commission(fills)

        return {
            "status": status,
            "fill_price": fill_price,
            "fill_qty": fill_qty,
            "order_id": str(data.get("orderId", "")),
            "commission": commission,
            "commission_asset": commission_asset,
            "quote_qty": quote_qty,
            "transact_time": transact_time,
        }

    def query_order(self, symbol: str, broker_order_id: str) -> dict[str, Union[str, float]]:
        """Query order status from Binance. Returns fill details if FILLED."""
        timestamp = self._current_timestamp_ms()
        endpoint = _FUTURES_ORDER_ENDPOINT if self._futures else _ORDER_ENDPOINT
        data = self._signed_request("GET", endpoint, {
            "symbol": symbol,
            "orderId": int(broker_order_id),
            "timestamp": timestamp,
        })
        status = str(data.get("status", "UNKNOWN"))
        if self._futures:
            avg_price = float(data.get("avgPrice") or 0)
            _executed = float(data.get("executedQty") or 0)
            if _executed > 0:
                fill_price = avg_price if avg_price > 0 else 0.0
                fill_qty = _executed
                quote_qty = float(data.get("cumQuote") or 0) or None
                transact_time = int(data["updateTime"]) if data.get("updateTime") else None
                return {
                    "status": status,
                    "fill_price": fill_price,
                    "fill_qty": fill_qty,
                    "order_id": broker_order_id,
                    "commission": 0.0,
                    "commission_asset": "",
                    "quote_qty": quote_qty,
                    "transact_time": transact_time,
                }
            trades = self.get_user_trades(
                symbol,
                start_time=timestamp - 86400000,
                end_time=timestamp,
                limit=1000,
            )
            aggregated = _aggregate_trades_for_order(trades, order_id=broker_order_id, requested_qty=0.0)
            return {
                "status": str(aggregated["status"] or status),
                "fill_price": float(aggregated["fill_price"] or 0.0),
                "fill_qty": float(aggregated["fill_qty"] or 0.0),
                "order_id": broker_order_id,
                "commission": float(aggregated["commission"] or 0.0),
                "commission_asset": str(aggregated["commission_asset"] or ""),
                "quote_qty": aggregated["quote_qty"],
                "transact_time": aggregated["transact_time"],
            }
        fills = data.get("fills") or []
        _executed = float(data.get("executedQty") or 0)
        fill_price = _weighted_avg_fill_price(fills) if fills else 0.0
        fill_qty = _executed
        commission, commission_asset = _total_commission(fills)
        return {
            "status": status,
            "fill_price": fill_price,
            "fill_qty": fill_qty,
            "order_id": broker_order_id,
            "commission": commission,
            "commission_asset": commission_asset,
            "quote_qty": float(data.get("cummulativeQuoteQty") or 0) or None,
            "transact_time": int(data["transactTime"]) if data.get("transactTime") else None,
        }

    def get_user_trades(
        self,
        symbol: str,
        *,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Return recent account trades for a symbol."""
        timestamp = self._current_timestamp_ms()
        params: dict[str, Union[str, int, float]] = {
            "symbol": symbol,
            "timestamp": timestamp,
            "limit": max(1, min(int(limit), 1000)),
        }
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)
        endpoint = _FUTURES_TRADES_ENDPOINT if self._futures else _TRADES_ENDPOINT
        data = self._signed_request("GET", endpoint, params)
        if isinstance(data, list):
            return data
        return []

    def get_positions(
        self,
        *,
        symbol: str | None = None,
        include_flat: bool = False,
    ) -> list[dict[str, Any]]:
        """Return normalized futures positions from Binance.

        Uses /fapi/v2/positionRisk (weight=5) rather than the heavier
        /fapi/v2/account (weight=5 but returns full account data) so that
        frequent per-execution calls are less likely to hit rate limits.
        When a symbol filter is provided the endpoint accepts it directly,
        reducing the payload further.
        """
        if not self._futures:
            return []
        timestamp = self._current_timestamp_ms()
        params: dict[str, Union[str, int, float]] = {"timestamp": timestamp}
        if symbol:
            params["symbol"] = symbol.upper()
        rows = self._signed_request("GET", "/fapi/v2/positionRisk", params)
        if not isinstance(rows, list):
            rows = []
        normalized: list[dict[str, Any]] = []
        target_symbol = symbol.upper() if symbol else None
        for row in rows:
            current_symbol = str(row.get("symbol") or "").upper()
            if target_symbol and current_symbol != target_symbol:
                continue
            qty = float(row.get("positionAmt") or 0.0)
            if not include_flat and abs(qty) <= 0:
                continue
            updated_at = int(row.get("updateTime") or 0) or None
            normalized.append(
                {
                    "symbol": current_symbol,
                    "qty": qty,
                    "avg_price": float(row.get("entryPrice") or 0.0),
                    "unrealized_pnl": float(row.get("unRealizedProfit") or 0.0),
                    "notional": float(row.get("notional") or 0.0),
                    "position_side": str(row.get("positionSide") or "BOTH"),
                    "updated_at": updated_at,
                    "created_at": (
                        time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(updated_at / 1000))
                        if updated_at
                        else None
                    ),
                    "source": "binance_futures",
                }
            )
        return normalized
