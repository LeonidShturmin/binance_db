from datetime import datetime, timezone
from typing import Optional

import time
import ccxt
import pymysql


from config import db_name, host, password, user


def timeframe_1m(ticker: str) -> dict:
    """
    receives a trading pair as input, for example BTC/USD 
    and returns 1 candle (row) of the minute timeframe
    """
    exchange_data = ccxt.binance({ 'enableRateLimit': True })
    candles = exchange_data.fetch_ohlcv(ticker, '1m', limit=2)[0]
    """
    variable get a 1 candle (row) data about ticker
    """
    keys = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    dict_candles = {x: y for x, y in zip(keys, candles)}
    dict_candles['timestamp'] = datetime.fromtimestamp(dict_candles['timestamp']/1000, tz=timezone.utc)

    return dict_candles

def connect_test():
    """
    database connection check
    """
    try:
        connection = pymysql.connect(
            host=host,
            port=3306,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as ex:
        return print(f'(Response from connection test...{ex}')

def insert_candle_db(candle: dict) -> Optional[str]:
    """
    adds a new row to the database
    """
    connection = connect_test()
    try:

        with connection.cursor() as cursor:

            insert_query = "INSERT INTO candles (timestamp, open, high, low, close, volume) \
                            VALUES (%s, %s, %s, %s, %s, %s)"
            val = (candle['timestamp'], \
                    candle['open'], \
                    candle['high'], \
                    candle['low'], \
                    candle['close'], \
                    candle['volume'])
            cursor.execute(insert_query, val )
            connection.commit()

    except Exception as ex:
        return print(f'(Response from inserting func...{ex}')

    finally:
        connection.close()

if __name__ == '__main__':
    while True:
        candle_1m = timeframe_1m('BTC/USDT')
        insert_candle_db(candle_1m)

        time.sleep(60)
