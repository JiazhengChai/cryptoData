
# cryptoData

Download historical cryptocurrency market data (OHLCV) from **90+ centralized exchanges** and **DEX price data** via a single command-line tool.

Built on [ccxt](https://github.com/ccxt/ccxt) for CEX data and [DeFi Llama](https://defillama.com/) for DEX data.

## Features

- **90+ CEX exchanges** supported via ccxt (Binance, Bybit, OKX, Kraken, Coinbase, KuCoin, Bitfinex, Bitstamp, and many more)
- **DEX price data** via DeFi Llama (Uniswap V2/V3, SushiSwap)
- **Binance futures** support (coin-margined and USD-margined)
- Unified CSV output format: Date, Open, High, Low, Close, Volume
- Automatic rate limiting and retry on network errors
- List available exchanges and timeframes

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```bash
# Download BTC/USDT 4-hour candles from Binance
python main.py --exchange binance --base BTC --quote USDT --timeframe 4h --start_date 1/1/2024 --end_date 31/1/2024

# Download ETH/USD daily candles from Kraken
python main.py --exchange kraken --base ETH --quote USD --timeframe 1d --start_date 1/1/2024 --end_date 31/1/2024

# Download SOL/USDT 1-hour candles from Bybit
python main.py --exchange bybit --base SOL --quote USDT --timeframe 1h --start_date 1/6/2024 --end_date 30/6/2024

# Download DEX price data for ETH via DeFi Llama
python main.py --exchange uniswap_v3 --base ETH --quote USD --timeframe 1h --start_date 1/1/2024 --end_date 31/1/2024

# Download Binance coin-margined futures data
python main.py --exchange binance_coin_margined_future --base BTC --quote USD --timeframe 1h --start_date 1/1/2024 --end_date 31/1/2024

# List all supported exchanges
python main.py --list_exchanges

# List available timeframes for an exchange
python main.py --list_timeframes --exchange binance

# Show help
python main.py -h
```

## Parameters

| Argument | Description | Default | 
| -------- | ----------- | ------- |
| `--exchange` | Exchange to download from. Use `--list_exchanges` to see all options. | `binance` |
| `--base` | Base currency (e.g., BTC, ETH, SOL). For DEX, use ticker symbol. | `BTC` |
| `--quote` | Quote currency (e.g., USDT, USD, BTC, EUR). | `USDT` |
| `--timeframe` | Candle timeframe. Use `--list_timeframes` to check availability. | `1h` |
| `--start_date` | Start date in DD/MM/YYYY format. | `1/1/2024` |
| `--end_date` | End date in DD/MM/YYYY format. | `31/1/2024` |
| `--list_exchanges` | List all supported exchanges and exit. | - |
| `--list_timeframes` | List available timeframes for the specified exchange. | - |

## Supported Exchanges

### Centralized Exchanges (CEX)

All exchanges supported by [ccxt](https://github.com/ccxt/ccxt), including:
Binance, Bybit, OKX, Kraken, Coinbase, KuCoin, Gate.io, Bitfinex, Bitstamp, Poloniex, Bitbank, HTX (Huobi), Bitget, MEXC, and 80+ more.

### Decentralized Exchanges (DEX)

DEX token price data via DeFi Llama:
- `uniswap_v3` - Uniswap V3
- `uniswap_v2` - Uniswap V2
- `sushiswap` - SushiSwap

> **Note**: DEX data provides price snapshots (OHLC values are the same). Volume data is not available through this source.

### Exchange Aliases

- `binance_coin_margined_future` → Binance Coin-Margined Futures
- `binance_usd_margined_future` → Binance USD-Margined Futures

## Output Format

Data is saved as CSV in the `data/` directory with columns:

```
Date, Open, High, Low, Close, Volume
```

## Running Tests

```bash
pytest
```

Or for verbose output:

```bash
pytest -v
```

## Contributing

Pull requests are welcome.

## License

[MIT](https://choosealicense.com/licenses/mit/)
