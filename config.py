import argparse

import ccxt
from exchanges import EXCHANGE_ALIASES, DEX_EXCHANGES

parser = argparse.ArgumentParser(
    description="Download historical crypto market data from CEX and DEX exchanges."
)

parser.add_argument(
    "--exchange",
    default="binance",
    type=str,
    help=(
        "Exchange to download data from. Supports 90+ CEX exchanges via ccxt "
        "(e.g., binance, bybit, okx, kraken, coinbase, kucoin, bitfinex, bitstamp, etc.) "
        "and DEX data via DeFi Llama (uniswap_v3, uniswap_v2, sushiswap). "
        "Use --list_exchanges to see all available exchanges."
    ),
)

parser.add_argument(
    "--base",
    default="BTC",
    type=str,
    help="Base currency (e.g., BTC, ETH, SOL). For DEX, use coingecko token ID or ticker.",
)

parser.add_argument(
    "--quote",
    default="USDT",
    type=str,
    help="Quote currency (e.g., USDT, USD, BTC, EUR).",
)

parser.add_argument(
    "--timeframe",
    default="1h",
    type=str,
    help=(
        "Timeframe for candlestick data. Common options: "
        "1m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 1w, 1M. "
        "Available timeframes depend on the exchange."
    ),
)

parser.add_argument(
    "--start_date",
    default="1/1/2024",
    type=str,
    help="Start date in DD/MM/YYYY format.",
)

parser.add_argument(
    "--end_date",
    default="31/1/2024",
    type=str,
    help="End date in DD/MM/YYYY format.",
)

parser.add_argument(
    "--list_exchanges",
    action="store_true",
    help="List all supported exchanges and exit.",
)

parser.add_argument(
    "--list_timeframes",
    action="store_true",
    help="List available timeframes for the specified exchange and exit.",
)

args = parser.parse_args()
