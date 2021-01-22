
# cryptoData

cryptoData acts as a tool to download historical data from major cryptocurrency exchanges via API.
The data is in the format of date, open, high, low, close, volume.
Currently, Binance, Poloniex, Bitfinex, Bitstamp, Bybit, and Bitbank are supported and tested under windows. It should work in Linux as well, with little or no adaptation.
The codes may contain some bugs in some exceptional cases but it works in most normal usages.

## Installation
Install all the basic requirements listed in the requirements.txt.

```bash
pip install -r requirements.txt
```

## Example Usage

```
python main.py --exchange binance --base ETH --timeframe 4h --start_date 1/1/2021 --end_date 7/1/2021
python main.py --exchange bitfinex --base XRP --timeframe 5m --start_date 1/10/2020 --end_date 1/12/2020
python main.py -h #for help in argument choices
```
## Available parameters


| Argument | Description | Default&nbsp;value | Type |
| -------- | ----------- | ------------------ | ---- |
| exchange | Specify the exchange to download candlesticks data from. Available exchanges are binance, poloniex, bitfinex, bitstamp, bybit, and bitbank. | binance | str |
| base | The base currecny. For example, BTC, ETH, XLM, etc.  | BTC | str |
| quote | The quote currency. For example, USD, USDT, BTC, etc. The code will adapt accordingly when there are only USD pairs/USDT pairs in an exchange.| USD | str |
| timeframe | The timeframe of data to download. Available tiemframes are 1m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,1w,1M | 1h | str |
| start_date | The starting date of the data to be downloaded. It must be written in the format of DD/MM/YYYY. | 1/1/2020 | str |
| end_date |  The end date of the data to be downloaded. It must be written in the format of DD/MM/YYYY. | 31/1/2020 | str |

## Contributing
Pull requests are welcome.

## License
[MIT](https://choosealicense.com/licenses/mit/)
