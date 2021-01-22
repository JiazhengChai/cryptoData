import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--exchange', default="binance", type=str, choices=["binance","poloniex","bitfinex","bitstamp",
                                                                        "bybit","bitbank","binance_coin_margined_future"])

parser.add_argument('--base', default="BTC", type=str,help="base currency (BTC, etc.)")

parser.add_argument('--quote', default="USD", type=str,help="quote currency (USDT, etc.)")

parser.add_argument('--timeframe', default="1h", type=str,choices=["1m","5m","15m","30m","1h",
                                                                   "2h","4h","6h","8h","12h",
                                                                   "1d","1w","1M"])

parser.add_argument('--start_date', default="1/1/2020", type=str,help="day/month/year")

parser.add_argument('--end_date', default="31/1/2020", type=str,help="day/month/year")

args=parser.parse_args()
