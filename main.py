import os
import sys

from config import args
from exchanges import (
    ccxt_download_ohlcv,
    dex_download_ohlcv,
    list_supported_exchanges,
    get_ccxt_timeframes,
    DEX_EXCHANGES,
    DEFI_LLAMA_PERIOD_MAP,
    _parse_date,
)

int_to_month_dict = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# Handle --list_exchanges
if args.list_exchanges:
    cex_list, dex_list, alias_list = list_supported_exchanges()
    print("=== Centralized Exchanges (CEX) ===")
    for e in cex_list:
        print(f"  {e}")
    print(f"\n=== DEX (via DeFi Llama) ===")
    for e in dex_list:
        print(f"  {e}")
    print(f"\n=== Exchange Aliases ===")
    for e in alias_list:
        print(f"  {e}")
    print(f"\nTotal: {len(cex_list)} CEX + {len(dex_list)} DEX exchanges")
    sys.exit(0)

# Handle --list_timeframes
if args.list_timeframes:
    exchange = args.exchange.lower()
    if exchange in DEX_EXCHANGES:
        print(f"Available timeframes for {exchange} (DEX):")
        for tf in DEFI_LLAMA_PERIOD_MAP.keys():
            print(f"  {tf}")
    else:
        timeframes = get_ccxt_timeframes(exchange)
        print(f"Available timeframes for {exchange}:")
        for tf in timeframes:
            print(f"  {tf}")
    sys.exit(0)

# Parse arguments
exchange = args.exchange.lower()
base = args.base.upper()
quote = args.quote.upper()
tf = args.timeframe
start_date = args.start_date
end_date = args.end_date

# Parse dates
start_ts_ms = _parse_date(start_date, int_to_month_dict)
end_ts_ms = _parse_date(end_date, int_to_month_dict)

# Build CSV filename
start_day, start_month, start_year = start_date.split("/")
end_day, end_month, end_year = end_date.split("/")
csv_name = "_".join([
    base, quote, exchange, tf,
    start_day, int_to_month_dict[int(start_month)], start_year,
    end_day, int_to_month_dict[int(end_month)], end_year,
])

# Create data folder
cur_path = os.path.abspath(os.getcwd())
data_folder = os.path.join(cur_path, "data")
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

path_to_csv = os.path.join(data_folder, csv_name + ".csv")
print(f"Exchange: {exchange}")
print(f"Pair: {base}/{quote}")
print(f"Timeframe: {tf}")
print(f"Period: {start_date} to {end_date}")
print(f"Saving CSV data to: {path_to_csv}")

# Download data
if exchange in DEX_EXCHANGES:
    dex_download_ohlcv(path_to_csv, exchange, base, quote, tf,
                       start_ts_ms, end_ts_ms)
else:
    ccxt_download_ohlcv(path_to_csv, exchange, base, quote, tf,
                        start_ts_ms, end_ts_ms)

