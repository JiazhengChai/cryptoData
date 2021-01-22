import platform
from utils import *
from config import args

exchange=args.exchange
base=args.base
quote= args.quote
tf=args.timeframe
start_date=args.start_date
end_date=args.end_date

start_day,start_month,start_year=start_date.split("/")

end_day,end_month,end_year=end_date.split("/")

csv_name="_".join([base,quote,exchange,tf,start_day,int_to_month_dict[int(start_month)],start_year,end_day,int_to_month_dict[int(end_month)],end_year])

start_date=int_to_month_dict[int(start_month)]+" "+start_day+", "+start_year
end_date=int_to_month_dict[int(end_month)]+" "+end_day+", "+end_year

exchange_function = {
    "binance": binance_get_historical_klines_to_csv,
    "poloniex":poloniex_get_historical_klines_to_csv,
    "bitfinex":bitfinex_get_historical_klines_to_csv,
    "bitstamp":bitstamp_get_historical_klines_to_csv,
    "bybit":bybit_get_historical_klines_to_csv,
    "bitbank":bitbank_get_historical_klines_to_csv,
    "binance_coin_margined_future":binance_delivery_get_historical_klines_to_csv
}

path_to_csv=os.path.join(data_folder,csv_name+".csv")
print("Saving csv data in ",path_to_csv)
exchange_function[exchange](path_to_csv,base,quote,tf,start_date,end_date)

