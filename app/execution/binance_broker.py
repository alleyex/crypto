"""
Binance broker client — implements BrokerClient Protocol.

Supports both Binance Spot testnet and mainnet via the CRYPTO_BINANCE_TESTNET
setting.  Always places MARKET orders so fills happen immediately at the best
available price; the weighted-average fill price is returned.

Configuration (via environment variables):
    CRYPTO_BINANCE_API_KEY     — Binance API key
    CRYPTO_BINANCE_API_SECRET  — Binance API secret
    CRYPTO_BINANCE_TESTNET     — "true" to use testnet (default), "false" for mainnet
"""
import hashlib
import hmac
import time
from typing import Any, Dict, Optional, Union
from urllib.parse import urlencode

import requests

from app.core.settings import BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_TESTNET


TESTNET_BASE_URL = "https://testnet.binance.vision"
MAINNET_BASE_URL = "https://api.binance.com"

_ORDER_ENDPOINT = "/api/v3/order"
_ACCOUNT_ENDPOINT = "/api/v3/account"
_ORDER_TEST_ENDPOINT = "/api/v3/order/test"


class BinanceAPIError(RuntimeError):
    """Structured Binance API failure with response details for logging."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        url: str = "",
        response_text: str = "",
        response_json: Optional[dict[str, Any]] = None,
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


def _response_details(response: requests.Response) -> tuple[str, Optional[dict[str, Any]]]:
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


class BinanceBrokerClient:
    """Live broker client targeting Binance Spot API.

    Defaults to testnet for safety.  Set CRYPTO_BINANCE_TESTNET=false to use
    mainnet — only do this after completing the full soak validation cycle.
    """

    broker_name = "binance"

    def __init__(
        self,
        api_key: str = BINANCE_API_KEY,
        api_secret: str = BINANCE_API_SECRET,
        testnet: bool = BINANCE_TESTNET,
        timeout_seconds: int = 10,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = TESTNET_BASE_URL if testnet else MAINNET_BASE_URL
        self._timeout = timeout_seconds

    def _signed_request(self, method: str, endpoint: str, params: Dict[str, Union[str, int, float]]) -> dict:
        if not self._api_key or not self._api_secret:
            raise ValueError(
                "Binance API key and secret must be set via "
                "CRYPTO_BINANCE_API_KEY and CRYPTO_BINANCE_API_SECRET."
            )

        query_string = urlencode(params)
        signature = _sign(query_string, self._api_secret)
        query_string += f"&signature={signature}"
        url = f"{self._base_url}{endpoint}?{query_string}"
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
            detail = ""
            if isinstance(response_json, dict):
                if response_json.get("code") is not None:
                    detail = f" code={response_json.get('code')}"
                if response_json.get("msg"):
                    detail += f" msg={response_json.get('msg')}"
            raise BinanceAPIError(
                f"Binance API request failed with status {response.status_code}.{detail}".strip(),
                status_code=response.status_code,
                url=url,
                response_text=response_text,
                response_json=response_json,
            ) from exc
        return response.json()

    def check_account_connectivity(self) -> Dict[str, Union[str, bool, int]]:
        timestamp = int(time.time() * 1000)
        data = self._signed_request("GET", _ACCOUNT_ENDPOINT, {"timestamp": timestamp})
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
    ) -> Dict[str, Union[str, float, bool]]:
        timestamp = int(time.time() * 1000)
        self._signed_request(
            "POST",
            _ORDER_TEST_ENDPOINT,
            {
                "symbol": symbol,
                "side": side.upper(),
                "type": "MARKET",
                "quantity": qty,
                "timestamp": timestamp,
            },
        )
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
    ) -> Dict[str, Union[str, float]]:
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
        timestamp = int(time.time() * 1000)
        params: Dict[str, Union[str, int, float]] = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": qty,
            "timestamp": timestamp,
        }
        data = self._signed_request("POST", _ORDER_ENDPOINT, params)

        fills = data.get("fills") or []
        fill_price = _weighted_avg_fill_price(fills) if fills else ref_price
        fill_qty = float(data.get("executedQty") or qty)
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
