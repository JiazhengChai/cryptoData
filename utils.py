import os
import csv
import json
import time
import pytz
import requests
import dateparser
import datetime as dt
from binance.client import Client
from datetime import datetime, timedelta

cur_path=os.path.abspath(os.getcwd())

data_folder = os.path.join(cur_path, "data")

if not os.path.exists(data_folder):
    os.makedirs(data_folder)

int_to_month_dict = {
    1: "Jan", 2: "Feb", 3: "March", 4: "Apr", 5: "May", 6: "June", 7: "July", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov",
    12: "Dec"
}

bybit_root_url = 'https://api.bybit.com/'
bitbank_root_url = 'https://public.bitbank.cc'
bitstamp_root_url = 'https://www.bitstamp.net/api/v2/ohlc'
binance_spot_url = 'https://api.binance.com/api/v3/klines'
binance_delivery_url='https://dapi.binance.com/dapi/v1/klines'
bitfinex_root_url = 'https://api-pub.bitfinex.com/v2/candles/trade:'
poloniex_root_url = "https://poloniex.com/public?command=returnChartData&"

def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)

def date_to_utc(date_str):
    """Convert date to UTC
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch))

def interval_to_milliseconds(interval):
    """Convert a interval string to milliseconds
    :param interval: interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms

def binance_get_historical_klines_to_csv(csv_name,base,quote, interval, start_str, end_str=None):
    base=base.upper()
    quote=quote.upper()
    if quote=="USD":
        quote=quote+"T"

    symbol=base+quote
    my_csv = open(csv_name, 'w')
    writer = csv.writer(my_csv, lineterminator='\n')
    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    client = Client("", "")

    limit = 500

    timeframe = interval_to_milliseconds(interval)

    start_ts = date_to_milliseconds(start_str)

    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)


    idx = 0
    symbol_existed = False
    while True:
        temp_data = client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            for i in range(len(temp_data)):
                date = dt.datetime.fromtimestamp(temp_data[i][0] / 1000.0)
                op = temp_data[i][1]
                hi = temp_data[i][2]
                lo = temp_data[i][3]
                cl = temp_data[i][4]
                v = temp_data[i][5]

                writer.writerow( [date, op,hi , lo,cl, v])

            print(date)
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe

        else:
            print("Symbol not yet available. Increasing query start time...")
            start_ts += timeframe

        idx += 1
        if len(temp_data) < limit:
            break

        if idx % 3 == 0:
            time.sleep(1)

def binance_delivery_get_historical_klines_to_csv(csv_name,base,quote, interval, start_str, end_str=None):
    base=base.upper()
    quote=quote.upper()

    symbol=base+"USD_PERP"

    my_csv = open(csv_name, 'w')
    writer = csv.writer(my_csv, lineterminator='\n')
    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    client = Client("", "")

    limit = 500

    timeframe = interval_to_milliseconds(interval)

    start_ts = date_to_milliseconds(start_str)

    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)


    idx = 0
    symbol_existed = False
    while True:
        temp_data=client._request('get', 'https://dapi.binance.com/dapi/v1/klines', False, True,
                                  params={
                                      'symbol':symbol,
                                      'interval':interval,
                                       'limit':limit,
                                      'startTime':start_ts,
                                      'endTime':start_ts+(limit-1)*timeframe
                                  })

        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            for i in range(len(temp_data)):
                date = dt.datetime.fromtimestamp(temp_data[i][0] / 1000.0)
                op = temp_data[i][1]
                hi = temp_data[i][2]
                lo = temp_data[i][3]
                cl = temp_data[i][4]
                v = temp_data[i][5]

                writer.writerow( [date, op,hi , lo,cl, v])

            print(date)
            start_ts = start_ts + (limit) * timeframe

            if(end_ts <= start_ts):
                break

            if(end_ts < start_ts+(limit)*timeframe):
                limit=1+(end_ts-start_ts)//timeframe


        else:
            print("Symbol not yet available for the requested dates.")
            start_ts += timeframe

        idx += 1

        if len(temp_data) < limit:
            break

        if idx % 3 == 0:
            time.sleep(1)

def bitstamp_get_historical_klines_to_csv(csv_name,base,quote, interval, start_str, end_str=None):
    # bitstamp
    available_pairs=["btcusd", "btceur", "btcgbp", "btcpax", "gbpusd",
                    "gbpeur", "eurusd", "xrpusd", "xrpeur", "xrpbtc",
                    "xrpgbp", "xrppax", "ltcusd", "ltceur", "ltcbtc",
                    "ltcgbp", "ethusd", "etheur", "ethbtc", "ethgbp",
                    "ethpax", "bchusd", "bcheur", "bchbtc", "bchgbp", "paxusd",
                    "paxeur", "paxgbp", "xlmbtc", "xlmusd", "xlmeur", "xlmgbp"]
    base=base.lower()
    quote=quote.lower()
    symbol=base+quote
    if symbol not in available_pairs:
        print("Requested symbol not available in Bitstamp.")
        print("Please select from: ")
        print(available_pairs)
        quit()

    def bitstamp_get_bars(symbol, interval=86400, start=None, end=None, limit=500):
        url = bitstamp_root_url + '/' + symbol + '?step=' + str(interval) + '&limit=' + str(limit)
        if start:
            url = url + '&start=' + str(start)
        if end:
            url = url + '&end=' + str(end)

        data = json.loads(requests.get(url).text)

        return data

    my_csv = open(csv_name, 'w')
    writer = csv.writer(my_csv, lineterminator='\n')
    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    limit = 500

    timeframe = interval_to_milliseconds(interval)
    timeframe=int(timeframe/1000)

    start_ts = date_to_milliseconds(start_str)
    start_ts=int(start_ts/1000)-timeframe
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)
        end_ts = int(end_ts / 1000)
    else:
        end_ts=start_ts+500*timeframe#60

    cur_start=start_ts
    cur_end=end_ts
    if (end_ts-start_ts)/timeframe >500:
        cur_end=cur_start+500*timeframe

    idx = 0
    symbol_existed = False

    while True:
        temp_data=bitstamp_get_bars(symbol, interval = timeframe,start=cur_start ,end=cur_end,limit=int((cur_end-cur_start)/timeframe) )
        temp_data=temp_data["data"]["ohlc"]
        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            for i in range(len(temp_data)):

                date = dt.datetime.fromtimestamp(int(temp_data[i]["timestamp"]))
                op = temp_data[i]["open"]
                hi = temp_data[i]["high"]
                lo = temp_data[i]["low"]
                cl = temp_data[i]["close"]
                v = temp_data[i]["volume"]

                writer.writerow( [date, op,hi , lo,cl, v])

            if(cur_end==end_ts):
                break
            cur_start = int(temp_data[len(temp_data) - 1]["timestamp"]) + timeframe
            cur_end=cur_start+500*timeframe
            if(cur_end>end_ts):
                cur_end=end_ts
        else:
            print("Symbol not yet available. Increasing query start time...")
            start_ts += timeframe

        idx += 1
        if len(temp_data) < limit:
            break

        if idx % 3 == 0:
            time.sleep(1)

def poloniex_get_historical_klines_to_csv(csv_name,base,quote, interval, start_str, end_str=None):
    try:
        assert interval in ["5m","15m","30m","2h","4h","1d"]
    except:
        print("For poloniex, only the following timeframes are available.")
        print(["5m","15m","30m","2h","4h","1d"])
        quit()

    base=base.upper()
    quote=quote.upper()
    if quote=="USD":
        quote=quote+"T"

    symbol=quote+"_"+base

    def poloniex_get_bars(symbol, interval=5, start=None, end=None):
        url = poloniex_root_url + "currencyPair=" + symbol + '&start=' + str(start) + '&end=' + str(
            end) + "&period=" + str(interval) + "&resolution=auto"

        data = json.loads(requests.get(url).text)

        return data

    my_csv = open(csv_name, 'w')
    writer = csv.writer(my_csv, lineterminator='\n')
    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    limit = 10000

    timeframe = interval_to_milliseconds(interval)
    timeframe=int(timeframe/1000)

    start_ts = date_to_milliseconds(start_str)
    start_ts=int(start_ts/1000)

    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)
        end_ts = int(end_ts / 1000)
    else:
        end_ts=start_ts+limit*timeframe

    cur_start=start_ts
    cur_end=end_ts
    if (end_ts-start_ts)/timeframe >limit:
        cur_end=cur_start+limit*timeframe

    idx = 0
    symbol_existed = False

    prev_time=0
    while True:
        temp_data=poloniex_get_bars(symbol, interval = timeframe,start=cur_start ,end=cur_end)
        #print(len(temp_data))

        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            for i in range(len(temp_data)):

                date = dt.datetime.fromtimestamp(int(temp_data[i]["date"]))
                if i==0 :
                    print(date)
                    if prev_time!=0 :
                        if((int(temp_data[i]["date"]-prev_time)!=300)):
                          print("TIME DECALAGE.")

                if  i==len(temp_data)-1:
                    print(date)
                    prev_time=int(temp_data[i]["date"])

                op = temp_data[i]["open"]
                hi = temp_data[i]["high"]
                lo = temp_data[i]["low"]
                cl = temp_data[i]["close"]
                v = temp_data[i]["volume"]

                writer.writerow( [date, op,hi , lo,cl, v])

            if(cur_end==end_ts):
                break
            cur_start = int(temp_data[len(temp_data) - 1]["date"]) + timeframe
            cur_end=cur_start+limit*timeframe

            if(cur_end>end_ts):
                cur_end=end_ts
        else:
            print("Symbol not yet available. Increasing query start time...")
            start_ts += timeframe

        idx += 1

        if idx % 3 == 0:
            time.sleep(1)

def bitfinex_get_historical_klines_to_csv(csv_name,base,quote, interval, start_str, end_str=None):
    try:
        assert interval in ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1d', '7d', '14d']
    except:
        print("For poloniex, only the following timeframes are available.")
        print(['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1d', '7d', '14d'])
        quit()

    base = base.upper()
    quote = quote.upper()
    if quote == "USDT":
        quote = "USD"

    symbol = base + quote

    def bitfinex_get_bars(symbol, tf, start=None, end=None, limit=10000):
        tf_min=tf // 60000
        time_symbol="m"

        if tf_min >= 1440:
            tf_min=tf_min//1440
            time_symbol = "D"
        elif tf_min>=60:
            tf_min=tf_min//60
            time_symbol = "h"

        url = bitfinex_root_url + str(tf_min) + time_symbol+":t" + symbol + "/hist?" + '&start=' + str(
            start) + '&end=' + str(end) + \
              "&limit=" + str(limit) + "&sort=1"

        data = json.loads(requests.get(url).text)

        return data

    my_csv = open(csv_name, 'w')
    writer = csv.writer(my_csv, lineterminator='\n')
    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    limit = 10000

    timeframe = interval_to_milliseconds(interval)
    timeframe=int(timeframe)

    start_ts = date_to_milliseconds(start_str)
    start_ts=int(start_ts)

    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)
        end_ts =int(end_ts)
    else:
        end_ts=start_ts+limit*timeframe

    cur_start=start_ts
    cur_end=end_ts
    if (end_ts-start_ts)/timeframe >limit:
        cur_end=cur_start+limit*timeframe

    idx = 0
    symbol_existed = False
    start=time.time()

    prev_time=0
    while True:
        temp_data=bitfinex_get_bars(symbol, tf = timeframe,start=cur_start ,end=cur_end,limit=limit)
        #print(len(temp_data))

        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            for i in range(len(temp_data)):

                date = dt.datetime.fromtimestamp(int(temp_data[i][0]/1000))
                if i == 0:
                    print(date)
                    if prev_time != 0:
                        if ((int(temp_data[i][0] - prev_time) != 300000)):
                            print("TIME DECALAGE.")

                if i == len(temp_data) - 1:
                    print(date)
                    prev_time = int(temp_data[i][0])

                op = temp_data[i][1]
                hi = temp_data[i][3]
                lo = temp_data[i][4]
                cl = temp_data[i][2]
                v = temp_data[i][5]

                writer.writerow( [date, op,hi , lo,cl, v])

            if(cur_end==end_ts):
                break
            try:
                cur_start = int(temp_data[len(temp_data) - 1][0]) + timeframe
            except:
                print("wait")
            cur_end=cur_start+limit*timeframe
            if(cur_end>end_ts):
                cur_end=end_ts
        else:
            print("Symbol not yet available. Increasing query start time...")
            start_ts += timeframe

        idx += 1

        if idx % 3 == 0:
            time.sleep(1)

def bybit_get_historical_klines_to_csv(csv_name,base,quote, interval, start_str, end_str=None):
    base = base.upper()
    quote = quote.upper()
    symbol = base + quote

    try:
        if quote=="USD":
            assert base in ["BTC","ETH","XRP","EOS"]
        elif quote=="USDT":
            assert base in ["BTC","ETH","LTC","BCH","LINK","XTZ"]
    except:
        print(symbol+" does not exist in Bybit.")
        print("Try changing the quote currency to 'USD' or 'USDT'")
        quit()

    def bybit_get_bars(symbol, interval=5, start=None, limit=200):

        if "USDT" in symbol:
            url = bybit_root_url + "public/linear/kline?symbol=" + symbol + "&interval=" + str(
                interval) + "&limit=" + str(limit) + "&from=" + str(start)
        else:
            url = bybit_root_url + "v2/public/kline/list?symbol=" + symbol + "&interval=" + str(
                interval) + "&limit=" + str(limit) + "&from=" + str(start)

        data = json.loads(requests.get(url).text)

        return data

    my_csv = open(csv_name, 'w')
    writer = csv.writer(my_csv, lineterminator='\n')
    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    limit = 200

    if "m" in interval:
        interval_num=int(interval.replace("m",""))
    elif "h" in interval:
        hour=int(interval.replace("h",""))
        interval_num=hour*60
    else:
        interval_num = interval[-1].upper()

    timeframe = interval_to_milliseconds(interval)
    timeframe=int(timeframe/1000)

    start_ts = date_to_milliseconds(start_str)
    start_ts=int(start_ts/1000)
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)
        end_ts = int(end_ts / 1000)
        if interval!="5m":
            end_ts=end_ts+timeframe
    else:
        end_ts=start_ts+limit*timeframe

    cur_start=start_ts
    cur_end=end_ts
    if (end_ts-start_ts)/timeframe >limit:
        cur_end=cur_start+limit*timeframe

    idx = 0
    symbol_existed = False

    while True:
        temp_data=bybit_get_bars(symbol, interval = interval_num,start=cur_start,limit=int((cur_end-cur_start)/timeframe))
        temp_data=temp_data["result"]

        if not temp_data:
            print("Cannot query data for the current input parameters.")
            exit()

        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            for i in range(len(temp_data)):
                date = dt.datetime.fromtimestamp(int(temp_data[i]["open_time"]))
                op = temp_data[i]["open"]
                hi = temp_data[i]["high"]
                lo = temp_data[i]["low"]
                cl = temp_data[i]["close"]
                v = temp_data[i]["volume"]

                writer.writerow( [date, op,hi , lo,cl, v])
            print(date)
            if (cur_end >= end_ts):
                break
            cur_start = int(temp_data[len(temp_data) - 1]["open_time"]) + timeframe
            cur_end=cur_start+limit*timeframe

            if(cur_end>end_ts):
                cur_end=end_ts+timeframe
        else:
            print("Symbol not yet available. Increasing query start time...")
            start_ts += timeframe

        idx += 1

        if len(temp_data) < limit:
            break

        if idx % 3 == 0:
            time.sleep(1)

def bitbank_get_historical_klines_to_csv(csv_name,base,quote, interval, start_str, end_str=None):
    base = base.lower()
    quote = quote.lower()
    if quote in ["usd","usdt"]:
        symbol = base + "_jpy"
    else:
        symbol = base + quote

    if "min" not in interval and "m" in interval:
        interval=interval+"in"

    def bitbank_get_bars(symbol, ymd, interval="5min"):
        url = bitbank_root_url + "/" + symbol + "/candlestick/" + interval + "/" + ymd

        data = json.loads(requests.get(url).text)

        return data

    my_csv = open(csv_name, 'w')
    writer = csv.writer(my_csv, lineterminator='\n')
    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    if "min" not in interval:
        print("Only minutes interval supported now.")
        print("Exiting.")
        exit()

    interval_unit=int(interval.replace("min",""))
    idx = 0


    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)
    try:
        sd = datetime.strptime(start_str, '%B %d, %Y')
        ed = datetime.strptime(end_str, '%B %d, %Y')
    except:
        sd = datetime.strptime(start_str, '%b %d, %Y')
        ed = datetime.strptime(end_str, '%b %d, %Y')

    for single_date in daterange(sd, ed):
        ymd=single_date.strftime("%Y%m%d")
        temp_data=bitbank_get_bars(symbol, ymd=ymd,interval =interval)
        data=temp_data["data"]["candlestick"][0]["ohlcv"]
        #print(len(data))

        start_h=9
        start_m=0
        today_date=single_date
        for i in range(len(data)):
            if(start_m==0):
                date = today_date.strftime('%Y-%m-%d') + " " + str(start_h) + ":00:00"
            else:
                date = today_date.strftime('%Y-%m-%d') + " " + str(start_h) + ":" + str(start_m)+ ":00"
            if i==0:
                print(date)

            op = data[i][0]
            hi = data[i][1]
            lo = data[i][2]
            cl = data[i][3]
            v = data[i][4]

            writer.writerow( [date, op,hi , lo,cl, v])

            start_m=start_m+interval_unit
            if(start_m==60):
                start_m=0
                start_h=(start_h+1)
                if(start_h==24):
                    start_h=0
                    today_date= today_date+ timedelta(1)
        idx += 1

        if idx % 100 == 0:
            time.sleep(1)


