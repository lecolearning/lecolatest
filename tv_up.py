# -*- coding: utf-8 -*-

import json
import random
import re
import string
import asyncio
import requests
#from websocket import create_connection
import websockets
import ccxt.pro
from asyncio import run, gather, sleep
import datetime
from datetime import timezone
#import streamlit as st
import pandas as pd
import re
from pathlib import Path
from PIL import Image
import time

import nest_asyncio
nest_asyncio.apply()

#CCXT ORDER BOOK
orderbooks = {}

class RestartException(Exception):
    def __init__(self, msg='Trigger Restart Exception', *args, **kwargs):
        super().__init__(msg, *args, **kwargs)
        
def search(query, type):
    # type = 'stock' | 'futures' | 'forex' | 'cfd' | 'crypto' | 'index' | 'economic'
    # query = what you want to search!
    # it returns first matching item
    res = requests.get(
        f"https://symbol-search.tradingview.com/symbol_search/?text={query}&type={type}"
    )
    if res.status_code == 200:
        res = res.json()
        assert len(res) != 0, "Nothing Found."
        return res[0]
    else:
        print("Network Error!")
        exit(1)


def generateSession():
    stringLength = 12
    letters = string.ascii_lowercase
    random_string = "".join(random.choice(letters) for i in range(stringLength))
    return "qs_" + random_string


def prependHeader(st):
    return "~m~" + str(len(st)) + "~m~" + st


def constructMessage(func, paramList):
    return json.dumps({"m": func, "p": paramList}, separators=(",", ":"))


def createMessage(func, paramList):
    return prependHeader(constructMessage(func, paramList))


def sendMessage(ws, func, args):
    ws.send(createMessage(func, args))

async def sendMessage_async(ws, func, args):
    await ws.send(createMessage(func, args))

def sendPingPacket(ws, result):
    pingStr = re.findall(".......(.*)", result)
    if len(pingStr) != 0:
        pingStr = pingStr[0]
        ws.send("~m~" + str(len(pingStr)) + "~m~" + pingStr)
        
async def sendPingPacket_async(ws, result):
    pingStr = re.findall(".......(.*)", result)
    if len(pingStr) != 0:
        pingStr = pingStr[0]
        await ws.send("~m~" + str(len(pingStr)) + "~m~" + pingStr)


def socketJob(ws,priceType):
    while True:
        try:
            result = ws.recv()
            #print(result)
            if "quote_completed" in result or "session_id" in result:
                continue
            Res = re.findall("^.*?({.*)$", result)
            #print(Res)
            if len(Res) != 0:
                #Res_list = Res[0].split('~m~122~m~')
                Res_list = re.split('~m~...~m~',Res[0])

                for Res in Res_list:
                    jsonRes = json.loads(Res)
                    #return(jsonRes)
                    #print(jsonRes)
                    if jsonRes["m"] == "qsd":
                        symbol = jsonRes["p"][1]["n"]
                        bid = jsonRes["p"][1]["v"]["bid"]
                        bid_size = jsonRes["p"][1]["v"]["bid_size"]
                        ask = jsonRes["p"][1]["v"]["ask"]
                        ask_size = jsonRes["p"][1]["v"]["ask_size"]
                        print(f"{symbol} -> {bid_size}@{bid} | {ask_size}@{ask}")
            else:
                # ping packet
                sendPingPacket(ws, result)
        except KeyboardInterrupt:
            print("\nGoodbye!")
            exit(0)
        except Exception as e:
            print(f"ERROR: {e}\nTradingView message: {result}")
            continue
        
async def socketJob_async(ws):
    while True:
        try:
            result = await ws.recv()
            #print(result)
            if "quote_completed" in result or "session_id" in result:
                continue
            Res = re.findall("^.*?({.*)$", result)
            #print(Res)
            if len(Res) != 0:
                #Res_list = Res[0].split('~m~122~m~')
                Res_list = re.split('~m~[0-9]+~m~',Res[0])
                #print(Res_list)
                for Res in Res_list:
                    jsonRes = json.loads(Res)
                    #return(jsonRes)
                    #print(jsonRes)
                    if jsonRes["m"] == "qsd":
                        symbol = jsonRes["p"][1]["n"]
                        bid = jsonRes["p"][1]["v"]["bid"]
                        bid_size = jsonRes["p"][1]["v"]["bid_size"]
                        ask = jsonRes["p"][1]["v"]["ask"]
                        ask_size = jsonRes["p"][1]["v"]["ask_size"]
                        dt = datetime.datetime.now(timezone.utc)
                        timestamp = int(dt.timestamp()*1000)
                        datetimeStr = dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                        orderbook = {'asks':[[ask,ask_size]],
                                     'bids':[[bid,bid_size]],
                                     'datetime':datetimeStr,
                                     'timestamp':timestamp,
                                     'symbol':symbol}
                        orderbooks[symbol + '@CME'] = orderbook

                        print(f"{symbol} -> {bid_size}@{bid} | {ask_size}@{ask} | {datetimeStr}")
            else:
                # ping packet
                await sendPingPacket_async(ws, result)
        except KeyboardInterrupt:
            print("\nGoodbye!")
            exit(0)
        except Exception as e:
            print(f"ERROR: {e}\nTradingView message: {result}")
            continue

def getSymbolId(pair, market):
    data = search(pair, market)
    symbol_name = data["symbol"]
    try:
        broker = data["prefix"]
    except KeyError:
        broker = data["exchange"]
    symbol_id = f"{broker.upper()}:{symbol_name.upper()}"
    print(symbol_id, end="\n\n")
    return symbol_id

def get_auth_token():
    sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    username = 'username'
    password = 'password'
    data = {"username": 'username', "password": 'password', "remember": "on"}
    headers = {
        'Referer': 'https://www.tradingview.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'Pragma':'no-cache',
        'Cache-Control': 'no-cache',
    }
    auth_token = False
    auth_token = "eyJhbGciOiJSUzUxMiIsImtpZCI6IkdaeFUiLCJ0eXAiOiJKV1QifQ.eyJ1c2VyX2lkIjo0Njk5NjQ0NSwiZXhwIjoxNjg3ODQzODUzLCJpYXQiOjE2ODc4Mjk0NTMsInBsYW4iOiJwcm9fcmVhbHRpbWUiLCJleHRfaG91cnMiOjEsInBlcm0iOiJjYm90LGNib3RfbWluaSxjbWUsY21lLWZ1bGwsY21lX21pbmksY29tZXgsY29tZXhfbWluaSxueW1leCxueW1leF9taW5pIiwic3R1ZHlfcGVybSI6InR2LWNoYXJ0cGF0dGVybnMsdHYtcHJvc3R1ZGllcyx0di12b2x1bWVieXByaWNlIiwibWF4X3N0dWRpZXMiOjEwLCJtYXhfZnVuZGFtZW50YWxzIjowLCJtYXhfY2hhcnRzIjo0LCJtYXhfYWN0aXZlX2FsZXJ0cyI6MTAwLCJtYXhfYWN0aXZlX3ByaW1pdGl2ZV9hbGVydHMiOjEwMCwibWF4X2FjdGl2ZV9jb21wbGV4X2FsZXJ0cyI6MTAwLCJtYXhfc3R1ZHlfb25fc3R1ZHkiOjksIm1heF9jb25uZWN0aW9ucyI6MjB9.p7Udvh5zjq0FSkBsYHz4C3aCuvcQy043jrGDjOstjH4Rx5gO9SYUipIBcQOgAFEXXyojiA9zSEs49QkVFOy7EQruLrt6qq5TVUZL2gDU3Dl_1eo5jlVEr8maJ0gPZE8K7kN8t2N-xREuIKigq3TyMVycXLEbwzLiLmYPk2cFNpw"
    while(auth_token == False):
        response = requests.post(url=sign_in_url, data=data, headers=headers)
        print(response)
        auth_token = response.json().get('user',dict()).get('auth_token',False)
        print('auth_token: ',auth_token)
        time.sleep(0.5)
    return auth_token

# =============================================================================
# def main(pair, market):
# 
#     # serach btcusdt from crypto category
#     symbol_id = getSymbolId(pair, market)
# 
#     # create tunnel
#     tradingViewSocket = "wss://data.tradingview.com/socket.io/websocket"
#     headers = json.dumps({"Origin": "https://data.tradingview.com"})
#     ws = create_connection(tradingViewSocket, headers=headers)
#     session = generateSession()
#     auth_token = get_auth_token()
#     # Send messages
#     priceType = 'ask'
#     sendMessage(ws, "quote_create_session", [session])
#     sendMessage(ws, "set_auth_token", [auth_token])
#     sendMessage(ws, "quote_set_fields", [session,'bid','ask','bid_size','ask_size'])
#     sendMessage(ws, "quote_add_symbols", [session, pair,'CME:BTC2!'])
# 
#     # Start job
#     #asyncio.get_event_loop().run_until_complete(receive_message(ws))
#     test = socketJob(ws,priceType)
#     
#     return(test)
# =============================================================================

async def tv_async(pairs):

    # serach btcusdt from crypto category
    #symbol_id = getSymbolId(pair, market)

    # create tunnel
    tradingViewSocket = "wss://data.tradingview.com/socket.io/websocket"
    #headers = json.dumps({"Origin": "https://data.tradingview.com"})
    headers = {"Origin": "https://data.tradingview.com"}
    
    async with websockets.connect(tradingViewSocket,extra_headers=headers) as websocket:
        session = generateSession()
        auth_token = get_auth_token()
        await sendMessage_async(websocket, "quote_create_session", [session])
        await sendMessage_async(websocket, "set_auth_token", [auth_token])
        await sendMessage_async(websocket, "quote_set_fields", [session,'bid','ask','bid_size','ask_size'])
        await sendMessage_async(websocket, "quote_add_symbols", [session, *pairs])
        #orderbooks['CME'+] = dict()
        await socketJob_async(websocket)
    #ws = create_connection(tradingViewSocket, headers=headers)

    # Send messages
    #priceType = 'ask'

    # Start job
    #asyncio.get_event_loop().run_until_complete(receive_message(ws))
   #await socketJob_async(ws,priceType)

# =============================================================================
# def handle_all_orderbooks(orderbooks):
#     try:
#         #print('We have the following orderbooks:')
#         df = pd.DataFrame.from_dict(orderbooks,orient='index')
#         df['symbol_exchange'] = df.index
#         df['best_ask']=df.asks.str[0]
#         df['best_ask_size']=df.best_ask.str[1]
#         df['best_ask']=df.best_ask.str[0]
#         df['best_bid']=df.bids.str[0]
#         df['best_bid_size']=df.best_bid.str[1]
#         df['best_bid']=df.best_bid.str[0]
#         df['bidAsk_Spread']= df['best_ask'] - df['best_bid'] 
#         #print(df)
#         columns = ['symbol_exchange','best_bid','best_ask','best_bid_size','best_ask_size','bidAsk_Spread']
#         df = df.filter(columns)
#         st.dataframe(df,use_container_width=True)
# # =============================================================================
# #     for symbol,orderbook in orderbooks.items():
# #         print(ccxt.pro.Exchange.iso8601(orderbook['timestamp']), symbol, orderbook['asks'][0], orderbook['bids'][0])
# # =============================================================================
#     except Exception as e:
#         pass
# =============================================================================

def handle_all_orderbooks(orderbooks):
    #print('We have the following orderbooks:')
    size = 5 # size for coin trade size
    try:
        df = pd.DataFrame.from_dict(orderbooks,orient='index')
        df['symbol_exchange'] = df.index
        df['best_ask']=df.asks.str[0]
        df['best_ask_size']=df.best_ask.str[1]
        df['best_ask']=df.best_ask.str[0]
        df['best_bid']=df.bids.str[0]
        df['best_bid_size']=df.best_bid.str[1]
        df['best_bid']=df.best_bid.str[0]
        df['best_spread'] = df['best_ask'] - df['best_bid']
        
        #getting average trade size
        df['tradeSize']= 5
        df.loc[df['symbol_exchange'].str.contains('ETH'),'tradeSize'] = 50 #ETH contracts
        df.loc[df['symbol_exchange'].str.contains('T:BT'),'tradeSize'] = 100000 #inverse contracts
        df.loc[df['symbol_exchange'].str.contains('T:ET'),'tradeSize'] = 100000 #inverse contracts
        df.loc[df['symbol_exchange'].str.contains('D:ET'),'tradeSize'] = 100000 #inverse contracts
        df.loc[df['symbol_exchange'].str.contains('D:BT'),'tradeSize'] = 100000 #inverse contracts
        df['avgAsksForSize'] = None
        df['avgBidsForSize'] = None

        for i in df.index:
            df.at[i,'avgAsksForSize'] = getAvgPriceSize(df.loc[i,'asks'],df.loc[i,'tradeSize'])
            df.at[i,'avgBidsForSize'] = getAvgPriceSize(df.loc[i,'bids'],df.loc[i,'tradeSize'])
        #print(df)
# =============================================================================
#         df['avgAsksForSize'] = df['asks'].apply(getAvgPriceSize,size=size)
#         df['avgBidsForSize'] = df['bids'].apply(getAvgPriceSize,size=size)
# =============================================================================
        df['avgSpreadForSize'] = df['avgAsksForSize'].str[0]  - df['avgBidsForSize'].str[0]
        columns = ['symbol_exchange','best_bid','best_ask','best_bid_size','best_ask_size','best_spread','tradeSize','avgBidsForSize','avgAsksForSize','avgSpreadForSize','datetime']
        df = df.filter(columns)
        df.reset_index(inplace=True,drop=True)
        df.to_pickle("./prices.pkl")
        #container = st.container()
        #container.dataframe(df,use_container_width=True)
        listOfPairTuple = [('CME:BTC1!@CME','BTC/TUSD@binance'),('CME:BTC2!@CME','BTC/TUSD@binance'),
                           ('CME:BTC1!@CME','BTC/USDT@binance'),('CME:BTC2!@CME','BTC/USDT@binance'),
                           ('CME:ETH1!@CME','ETH/TUSD@binance'),('CME:ETH2!@CME','ETH/TUSD@binance'),
                           ('CME:ETH1!@CME','ETH/USDT@binance'),('CME:ETH2!@CME','ETH/USDT@binance'),
                           ('CME:ETH1!@CME','ETH/BUSD:BUSD@binanceusdm'),('CME:ETH2!@CME','ETH/BUSD:BUSD@binanceusdm'),
                           ('CME:ETH1!@CME','ETH/USDT:USDT@binanceusdm'),('CME:ETH2!@CME','ETH/USDT:USDT@binanceusdm'),
                           ('CME:BTC1!@CME','BTC/BUSD:BUSD@binanceusdm'),('CME:BTC2!@CME','BTC/BUSD:BUSD@binanceusdm'),
                           ('CME:BTC1!@CME','BTC/USDT:USDT@binanceusdm'),('CME:BTC2!@CME','BTC/USDT:USDT@binanceusdm'),
                           ('CME:BTC1!@CME','BTC/USDT:USDT-230630@binanceusdm'),('CME:ETH1!@CME','ETH/USDT:USDT-230630@binanceusdm'),
                           ('BTC/USDT@binance','BTC/TUSD@binance'),('BTC/BUSD:BUSD@binanceusdm','BTC/TUSD@binance'),
                           ('BTC/USDT:USDT@binanceusdm','BTC/TUSD@binance'),('ETH/BUSD:BUSD@binanceusdm','ETH/TUSD@binance'),
                           ('ETH/USDT:USDT@binanceusdm','ETH/USDT@binance'),('BTC/USDT:USDT-230630@binanceusdm','BTC/USDT@binance'),
                           ('ETH/USDT:USDT-230630@binanceusdm','ETH/USDT@binance')]
        dict_spread = getCrossContractbidAskSpread(listOfPairTuple)
        df = pd.DataFrame.from_dict(dict_spread,orient='index',columns=['best_bid - best_ask'])
        df.to_pickle("./spreads.pkl")  
        
        #container.dataframe(df)

    except Exception as e:
        print(e)
# =============================================================================
#     for symbol,orderbook in orderbooks.items():
#         print(ccxt.pro.Exchange.iso8601(orderbook['timestamp']), symbol, orderbook['asks'][0], orderbook['bids'][0])
# =============================================================================

def getAvgPriceSize(price_size,size):
    price_df = pd.DataFrame(price_size)
    price_df.rename(columns={0:'price',1:'size'}, inplace=True)
    if(price_df.shape[0] == 1):
        return(price_size[0])
    price_df['product'] =  price_df['price'] * price_df['size']
    price_df['cumu_size'] = price_df['size'].cumsum()
    price_df['cumu_product'] = price_df['product'].cumsum()
    price_df['avg_price'] = (price_df['cumu_product'] / price_df['cumu_size'])
    try:
        indexSize = price_df[price_df.cumu_size >= size].index[0]
    except IndexError:
        toReturn = [round(price_df['avg_price'].iloc[-1],2),round(price_df['cumu_size'].iloc[-1],2)]
        return(toReturn)
    return([round(price_df['avg_price'].loc[indexSize],2),round(price_df['cumu_size'].loc[indexSize],2)])

def getCrossContractbidAskSpread(listOfPairTuple):
    dict_spread = dict()
    for pairs in listOfPairTuple:
        dict_spread[pairs[0]+'-'+pairs[1]] = orderbooks[pairs[0]].get('bids')[0][0] - orderbooks[pairs[1]].get('asks')[0][0]
    return(dict_spread)

def handle_all_orderbooks_spread(orderbooks):
    print('Spreads')
    for exchange_id, orderbooks_by_symbol in orderbooks.items():
        for symbol in orderbooks_by_symbol.keys():
            orderbook = orderbooks_by_symbol[symbol]
            print(ccxt.pro.Exchange.iso8601(orderbook['timestamp']), exchange_id, symbol, orderbook['asks'][0], orderbook['bids'][0])

async def handling_loop(orderbooks):
    delay = 0.5
    while True:
        await sleep(delay)
        handle_all_orderbooks(orderbooks)
        #handle_all_orderbooks_spread(orderbooks)

async def restart_loop(orderbooks):
    delay = 60*60*1
    while True:
        await sleep(delay)
        raise RestartException('Restarting Program')
        #handle_all_orderbooks_spread(orderbooks)

async def symbol_loop(exchange, symbol):
    if(str(exchange) in ['BitMEX']):
        while True:
            try:
                orderbook = await exchange.watch_order_book(symbol)
                
                #adding datetime
                dt = datetime.datetime.now(timezone.utc)
                dtStr = int(dt.timestamp()*1000)
                dtISO = dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                orderbook['timestamp'] = dtStr
                orderbook['datetime'] = dtISO
                #print(dtISO)
                #orderbooks[exchange.id] = orderbooks.get(exchange.id, {})
                orderbooks[symbol+'@'+exchange.id] = orderbook
            except Exception as e:
                print(str(e))
                # raise e  # uncomment to break all loops in case of an error in any one of them
                break  # you can break just this one loop if it fails
    else:
        while True:
            try:
                orderbook = await exchange.watch_order_book(symbol)
                #print(orderbook)
                #orderbooks[exchange.id] = orderbooks.get(exchange.id, {})
                orderbooks[symbol+'@'+exchange.id] = orderbook
            except Exception as e:
                print(str(e))
                # raise e  # uncomment to break all loops in case of an error in any one of them
                break  # you can break just this one loop if it fails


async def exchange_loop(exchange_id, symbols):
    exchange = getattr(ccxt.pro, exchange_id)()
    #markets = await exchange.load_markets()
    #print(exchange,markets)
    loops = [symbol_loop(exchange, symbol) for symbol in symbols]
    await gather(*loops)
    await exchange.close()


async def main():
    bybit_symbols = ['BTC/USD:BTC','BTC/USD:BTC-230630','BTC/USDT:USDT','ETH/USD:ETH','ETH/USDT:USDT']
    binance_symbols = ['BTC/USDT','ETH/USDT','BTC/TUSD','ETH/TUSD','BTC/BUSD','ETH/BUSD']
    #binanceusdm_symbols = ['BTC/USDT','BTC/BUSD','ETH/USDT','ETH/BUSD','BTCUSDT_230331','ETHUSDT_230331']
    binanceusdm_symbols = ['BTC/USDT:USDT','ETH/USDT:USDT','BTC/BUSD:BUSD','ETH/BUSD:BUSD','BTC/USDT:USDT-230630','ETH/USDT:USDT-230630']
    #binanceusdm_symbols = ['BTC/USDT:USDT']
    bitmex_symbols = ['BTC/USD:BTC','BTC/USD:BTC-230630','BTC/USD:BTC-230630','BTC/USDT:USDT','ETH/USDT:USDT','ETH/USDT:USDT-230630']
    #bitmex_symbols = ['BTC/USD:BTC']
    exchanges = {
        'bybit': bybit_symbols,
        'binance': binance_symbols,
        'binanceusdm': binanceusdm_symbols,
        'bitmex': bitmex_symbols,

    }
    loops = []
    #loops = [exchange_loop(exchange_id, symbols) for exchange_id, symbols in exchanges.items()]
    loops += [tv_async(["CME:BTC1!","CME:BTC2!","CME:ETH1!","CME:ETH2!",
                        "FX:USDJPY","FX:USDEUR","FX:USDSGD","FX:USDKRW",
                        "FX:USDAUD","FX:USDGBP","FX:USDHKD","FX:USDCNY",
                        "FX:USDCNH"])]
    #loops += [handling_loop(orderbooks)]
    loops += [restart_loop(orderbooks)]

    await gather(*loops)

if __name__ == "__main__":
# =============================================================================
#     st.set_page_config(layout="wide")
#     with st.empty(): # Modified to use empty container
#         try:
#             asyncio.run(main())
#         except KeyboardInterrupt:
#             exit()
# =============================================================================
# =============================================================================
#     favicon = Image.open(Path(__file__).parent / 'assets/favicon.png')
#     st.set_page_config(layout="wide",
#                        page_title="Spreads @ CODA",
#                        page_icon=favicon)
#     st.title("Spreads @ CODA")
#     #st.set_page_config(layout="wide")
#     #st.title("Spreads @ CODA")
#     placeholder = st.empty()
#     with placeholder:
#         try:
#             asyncio.run(main())
#         except KeyboardInterrupt:
#             exit()
# =============================================================================
    while(True):
        try:
            asyncio.run(main())
        except RestartException as e:
            print(e)
            print('Sleeping for 2 seconds')
            time.sleep(2)
    #asyncio.run(tv_async("CME:BTC1!"))

