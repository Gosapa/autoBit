from pybitget import Client
from pybitget.enums import *
from pybitget import utils
from pybitget import exceptions
from pybitget import logger
import numpy as np
import datetime
import time
import pandas as pd
import requests
import json
api_key =  ""
api_secret = ""
api_passphrase = ""
myToken = ''
global start
global end
global gran

client = Client(api_key, api_secret, passphrase=api_passphrase)

def get_ohlcv(sym):
    contents = client.mix_get_candles(symbol=sym,granularity=gran,startTime= start, endTime= end)
    dt_list = []
    for x in contents:
        dt = datetime.datetime.fromtimestamp(int(x[0]) / 1000.0).strftime("%Y-%m-%d %H:%M:%S.%f")
        dt_list.append(dt)

    df = pd.DataFrame(contents,columns=[
        'timestamp',
        'opening_price',
        'high_price',
        'low_price',
        'trade_price',
        'candle_acc_trade_volume',
        'candle_acc_trade_price'], 
        index=dt_list)
    df = df.sort_index()
    df = df.drop('timestamp',axis=1)
    df = df.rename(columns={"opening_price": "open",
                                    "high_price": "high",
                                    "low_price": "low",
                                    "trade_price": "close",
                                    "candle_acc_trade_volume": "volume",
                                    "candle_acc_trade_price": "value"})

    for col in df.columns:
        df[col] = df[col].astype(float)
    return df


def post_message(token, channel, text):
    """슬랙 메시지 전송"""
    response = requests.post("https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer "+token},
        data={"channel": channel,"text": text}
    )

def get_target_price(ticker, k):
    df = get_ohlcv(ticker)
    target_price = df.iloc[0]['close'] + (df.iloc[0]['high'] - df.iloc[0]['low']) * k
    return target_price

def get_start_time(ticker):
    """시작 시간 조회"""
    df = get_ohlcv(ticker)
    start_time = df.index[1]
    return start_time

def get_balance(ticker):
    """잔고 조회"""
    balances = client.mix_get_account(symbol=ticker, marginCoin='USDT')
    return float(balances['data']['usdtEquity'])

def get_current_price(ticker):
    """현재가 조회"""
    return client.mix_get_market_price(symbol=ticker)['data']['markPrice']
def close_position():
    params = {}
    params['productType'] = 'umcbl' 
    return client._request_with_params(POST, MIX_ORDER_V1_URL+ '/close-all-positions', params)

print('autotrade start')
post_message(myToken, '#bitget', "autotrade start")

# 자동매매 시작
while True:
    try:
        now = datetime.datetime.now()
        start = round(time.time()*1000) - 16*60*1000
        end = round(time.time()*1000)
        gran = '15m'
        start_time = datetime.datetime.strptime(get_start_time("BTCUSDT_UMCBL"), "%Y-%m-%d %H:%M:%S.%f")
        end_time = start_time + datetime.timedelta(minutes=15)

        if start_time < now < end_time - datetime.timedelta(seconds=10):
            target_price = get_target_price("BTCUSDT_UMCBL", 0.5)
            current_price = float(get_current_price("BTCUSDT_UMCBL"))
            if target_price < current_price:
                dataCont = client.mix_get_account(symbol="BTCUSDT_UMCBL", marginCoin='USDT')
                calcSize = (dataCont['data']['crossMarginLeverage'] * dataCont['data']['btcEquity']) * (1 - (dataCont['data']['crossMarginLeverage'] * 0.0004))
                client.mix_place_order(symbol='BTCUSDT_UMCBL', marginCoin='USDT',size=calcSize, side='open_long', orderType='market')
                post_message(myToken,'#bitget', "BTC Buy : " + str(get_current_price('BTCUSDT_UMCBL')))
        else:
            try:
                close_position()
                post_message(myToken,'#bitget', "Closed Position\nCurrent USDT : " + str(get_balance()))
            except Exception as e:
                print(e)
                time.sleep(1)
        time.sleep(1)
    except Exception as e:
        print(e)
        time.sleep(1)