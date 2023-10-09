import logging
import sys
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8.8s] %(name)s -- %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
import sammchardy_binance.binance as binance
import asyncio
from binance import AsyncClient, BinanceSocketManager
from local.binance_api_key import balazs_trader_api_key, balazs_trader_api_secret
import json
import pandas as pd
import binanceapi.utils as utils
import time

logger = logging.getLogger('test_binance')

async def print_chunk(streams, **kwargs):
    index_df = index_df_from_streams(streams)
    async for idf, dic in utils.get_chunk(index_df, **kwargs):
        print({k:pd.DataFrame(v) for k,v in dic.items()})
        print(idf)
        return
        
def normalize_stream_name(stream):
    if '@' not in stream:
        stream = stream.lower() + '@trade'
    return stream

def decompose_stream_name(stream):
    spl = stream.split('@')
    symbol = spl[0]
    if len(spl)==1:
        return symbol, ''
    else:
        return symbol, '@'.join(spl[1:])


async def test2():
    client = await utils.get_binance_client()
    #depth = await client.get_order_book(symbol='BNBBTC')
    #prices = await client.get_all_tickers()
#    import pdb; pdb.set_trace()
    data = await client.get_historical_trades(symbol='BTCUSDT')
    for i,t in enumerate(data):
        print(i, pd.Timestamp(t['time'], unit='ms'))
    await client.close_connection()


async def test3():
    async with utils.get_binance_client() as client:
        info = await client.get_exchange_info()
        return info

async def print_symbols(streams):
    coro = print_chunk(streams)
    await main(coro)

@utils.interruptible()
async def test_one(stream, **kwargs):
    await utils.multi_chunk(stream, **kwargs)

@utils.interruptible()
async def main():
    #streams = utils.create_streams_list(['trade','bookTicker', 'depth20'])
    #streams = ['btcusdt@trade', 'btcusdt@bookTicker', 'btcusdt@depth20']
    streams = ['eth-220923-1500-c@index']
    #streams = ['btcusdt@trade', 'ethusdt@trade']
    await utils.multi_chunk(streams, localfile=True, timeout=10, counter=1000000, is_option=True )
    
if __name__ == '__main__':
    asyncio.run(main())

    
