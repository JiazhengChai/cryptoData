"""
Unified exchange data download module using ccxt.

Supports 90+ centralized exchanges via ccxt and DEX data via DeFi Llama API.
"""

import os
import csv
import time
import json
import requests
import datetime as dt

import ccxt


# Mapping of exchange aliases to ccxt exchange IDs
EXCHANGE_ALIASES = {
    "binance_coin_margined_future": "binancecoinm",
    "binance_usd_margined_future": "binanceusdm",
}

# DEX identifiers (not ccxt exchanges)
DEX_EXCHANGES = {"uniswap_v3", "uniswap_v2", "sushiswap"}

# DeFi Llama base URL for coin price charts
DEFI_LLAMA_CHART_URL = "https://coins.llama.fi/chart"
DEFI_LLAMA_PRICES_URL = "https://coins.llama.fi/prices/current"

# Period mapping for DeFi Llama API
DEFI_LLAMA_PERIOD_MAP = {
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "1d": "1d",
    "1w": "1w",
}


def get_ccxt_exchange_id(exchange_name):
    """Resolve an exchange name to its ccxt exchange ID."""
    name = exchange_name.lower()
    if name in EXCHANGE_ALIASES:
        return EXCHANGE_ALIASES[name]
    if name in ccxt.exchanges:
        return name
    raise ValueError(
        f"Exchange '{exchange_name}' is not supported. "
        f"Use --list_exchanges to see available exchanges."
    )


def list_supported_exchanges():
    """Return list of all supported exchanges (CEX + DEX)."""
    cex_list = sorted(ccxt.exchanges)
    dex_list = sorted(DEX_EXCHANGES)
    alias_list = sorted(EXCHANGE_ALIASES.keys())
    return cex_list, dex_list, alias_list


def get_ccxt_timeframes(exchange_name):
    """Return available timeframes for a ccxt exchange."""
    exchange_id = get_ccxt_exchange_id(exchange_name)
    exchange = getattr(ccxt, exchange_id)()
    return list(exchange.timeframes.keys()) if exchange.timeframes else []


def _build_symbol(base, quote, exchange_id):
    """Build the trading pair symbol for ccxt."""
    base = base.upper()
    quote = quote.upper()
    return f"{base}/{quote}"


def _interval_to_seconds(interval):
    """Convert interval string to seconds."""
    if not interval:
        return None
    units = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
    unit = interval[-1]
    if unit == "M":
        return 30 * 86400  # approximate
    if unit in units:
        try:
            return int(interval[:-1]) * units[unit]
        except ValueError:
            pass
    return None


def _parse_date(date_str, int_to_month_dict):
    """Parse DD/MM/YYYY date string to a human-readable format and timestamp."""
    parts = date_str.split("/")
    day, month, year = parts[0], int(parts[1]), parts[2]
    month_str = int_to_month_dict[month]
    readable = f"{month_str} {day}, {year}"

    import dateparser
    import pytz

    d = dateparser.parse(readable)
    if d is None:
        raise ValueError(f"Could not parse date: {date_str}")
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)
    epoch = dt.datetime(1970, 1, 1, tzinfo=pytz.utc)
    return int((d - epoch).total_seconds() * 1000)


def _create_exchange(exchange_id):
    """Create a ccxt exchange instance."""
    return getattr(ccxt, exchange_id)({"enableRateLimit": True})


def ccxt_download_ohlcv(csv_path, exchange_name, base, quote, timeframe,
                        start_ts_ms, end_ts_ms):
    """
    Download OHLCV data from a CEX exchange using ccxt and save to CSV.

    :param csv_path: Path to save the CSV file
    :param exchange_name: Exchange name (ccxt ID or alias)
    :param base: Base currency (e.g., 'BTC')
    :param quote: Quote currency (e.g., 'USDT')
    :param timeframe: Candle timeframe (e.g., '1h', '4h', '1d')
    :param start_ts_ms: Start timestamp in milliseconds
    :param end_ts_ms: End timestamp in milliseconds
    """
    exchange_id = get_ccxt_exchange_id(exchange_name)
    exchange = _create_exchange(exchange_id)

    symbol = _build_symbol(base, quote, exchange_id)

    # Validate timeframe
    if exchange.timeframes and timeframe not in exchange.timeframes:
        available = list(exchange.timeframes.keys())
        raise ValueError(
            f"Timeframe '{timeframe}' not supported by {exchange_name}. "
            f"Available: {available}"
        )

    tf_ms = _interval_to_seconds(timeframe)
    if tf_ms is None:
        raise ValueError(f"Invalid timeframe format: {timeframe}")
    tf_ms *= 1000  # convert to milliseconds

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Date", "Open", "High", "Low", "Close", "Volume"])

        since = start_ts_ms
        total_candles = 0

        while since < end_ts_ms:
            try:
                ohlcv = exchange.fetch_ohlcv(
                    symbol, timeframe, since=since, limit=1000
                )
            except ccxt.BadSymbol:
                print(f"Symbol {symbol} not found on {exchange_name}.")
                print("Please check the base/quote currency pair.")
                return
            except ccxt.NetworkError as e:
                print(f"Network error: {e}. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            except ccxt.ExchangeError as e:
                print(f"Exchange error: {e}")
                return

            if not ohlcv:
                break

            for candle in ohlcv:
                ts, op, hi, lo, cl, vol = candle
                if ts > end_ts_ms:
                    break
                date = dt.datetime.fromtimestamp(ts / 1000.0, tz=dt.timezone.utc)
                writer.writerow([date, op, hi, lo, cl, vol])
                total_candles += 1

            last_ts = ohlcv[-1][0]
            if last_ts >= end_ts_ms:
                break

            # Move past the last candle
            since = last_ts + tf_ms

            if last_ts == since - tf_ms and len(ohlcv) < 2:
                # No new data available
                break

            print(f"  Downloaded {total_candles} candles so far... "
                  f"(latest: {dt.datetime.fromtimestamp(last_ts / 1000.0, tz=dt.timezone.utc)})")

    print(f"Done. Total {total_candles} candles saved to {csv_path}")


def dex_download_ohlcv(csv_path, dex_name, base, quote, timeframe,
                       start_ts_ms, end_ts_ms):
    """
    Download DEX token price data from DeFi Llama and save to CSV.

    DeFi Llama provides price chart data indexed by coingecko token IDs.
    For DEX data, we use the coin identifier format: 'coingecko:{token_id}'.

    :param csv_path: Path to save the CSV file
    :param dex_name: DEX name (e.g., 'uniswap_v3')
    :param base: Base token coingecko ID (e.g., 'ethereum', 'bitcoin')
    :param quote: Quote currency for reference (e.g., 'USD')
    :param timeframe: Candle period (e.g., '1h', '4h', '1d')
    :param start_ts_ms: Start timestamp in milliseconds
    :param end_ts_ms: End timestamp in milliseconds
    """
    period = DEFI_LLAMA_PERIOD_MAP.get(timeframe)
    if period is None:
        raise ValueError(
            f"Timeframe '{timeframe}' not supported for DEX data. "
            f"Available: {list(DEFI_LLAMA_PERIOD_MAP.keys())}"
        )

    # DeFi Llama uses coingecko IDs - common mappings
    token_id_map = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "USDT": "tether",
        "USDC": "usd-coin",
        "BNB": "binancecoin",
        "SOL": "solana",
        "XRP": "ripple",
        "ADA": "cardano",
        "AVAX": "avalanche-2",
        "DOT": "polkadot",
        "DOGE": "dogecoin",
        "MATIC": "matic-network",
        "LINK": "chainlink",
        "UNI": "uniswap",
        "AAVE": "aave",
        "SUSHI": "sushi",
        "CRV": "curve-dao-token",
        "COMP": "compound-governance-token",
        "MKR": "maker",
        "SNX": "havven",
        "YFI": "yearn-finance",
        "LTC": "litecoin",
        "BCH": "bitcoin-cash",
        "ETC": "ethereum-classic",
        "ATOM": "cosmos",
        "FTM": "fantom",
        "ALGO": "algorand",
        "NEAR": "near",
        "APE": "apecoin",
        "ARB": "arbitrum",
        "OP": "optimism",
    }

    base_upper = base.upper()
    token_id = token_id_map.get(base_upper, base.lower())
    coin_key = f"coingecko:{token_id}"

    start_ts_s = int(start_ts_ms / 1000)
    end_ts_s = int(end_ts_ms / 1000)
    tf_seconds = _interval_to_seconds(timeframe)

    # Calculate span (number of data points per request)
    total_points = (end_ts_s - start_ts_s) // tf_seconds
    max_per_request = 1000

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Date", "Open", "High", "Low", "Close", "Volume"])

        current_start = start_ts_s
        total_candles = 0

        while current_start < end_ts_s:
            remaining = (end_ts_s - current_start) // tf_seconds
            span = min(remaining, max_per_request)
            if span <= 0:
                break

            url = (
                f"{DEFI_LLAMA_CHART_URL}/{coin_key}"
                f"?start={current_start}&span={span}&period={period}"
            )

            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            except json.JSONDecodeError:
                print(f"Invalid response from DeFi Llama. Retrying...")
                time.sleep(5)
                continue

            coins_data = data.get("coins", {}).get(coin_key, {})
            prices = coins_data.get("prices", [])

            if not prices:
                print(f"No data available for {base_upper} from DeFi Llama.")
                break

            for point in prices:
                ts = point.get("timestamp", 0)
                price = point.get("price", 0)

                if ts > end_ts_s:
                    break

                date = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
                # DeFi Llama only provides price, not full OHLCV
                # We use price as Open/High/Low/Close, volume as 0
                writer.writerow([date, price, price, price, price, 0])
                total_candles += 1

            if prices:
                last_ts = prices[-1].get("timestamp", 0)
                current_start = last_ts + tf_seconds
                print(f"  Downloaded {total_candles} data points so far... "
                      f"(latest: {dt.datetime.fromtimestamp(last_ts, tz=dt.timezone.utc)})")
            else:
                break

    print(f"Done. Total {total_candles} data points saved to {csv_path}")
    if total_candles > 0:
        print("Note: DEX data from DeFi Llama provides price snapshots. "
              "OHLC values are the same (snapshot price). Volume is not available.")
