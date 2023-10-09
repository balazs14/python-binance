import asyncio
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8.8s] %(name)s -- %(message)s",
)
logging.getLogger().setLevel(logging.INFO)

import binanceapi.generators as gen
import binanceapi.dataio as dio
import binanceapi.stream_selection as sel
import time

async def test_option_arr():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='arr',
                                   symboltype='option')
    fr = time.time_ns()
    save_loop = gen.main_loop(selector, max_count=300,
                               timeout=10, max_chunks=2,
                               localfile=True, prefix='unittest')
    await save_loop
    loader = dio.load(selector, prefix='unittest', localfile=True, fr=fr, force_reload=True)
    df = await loader

    return df

async def test_option_arr_load():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='arr',
                                   symboltype='option')
    loader = dio.load(selector, prefix='unittest', localfile=True)
    df = await loader

    return df

async def test_spot_depth():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='depth20',
                                   symbols=['btcusdt','ethusdt'],
                                   symboltype='spot')
    fr = time.time_ns()
    save_loop = gen.main_loop(selector, max_count=300,
                               timeout=10, max_chunks=2,
                               localfile=False, prefix='unittest')
    await save_loop
    loader = dio.load(selector, prefix='unittest', localfile=False, fr=fr)
    df = await loader

    return df

async def test_spot_depth_load():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='depth20',
                                   symboltype='spot')
    loader = dio.load(selector, prefix='unittest', localfile=False)
    df = await loader

    return df

async def test_option_trade():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='trade',
                                   symboltype='option')
    fr = time.time_ns()
    save_loop = gen.main_loop(selector, max_count=300,
                               timeout=10, max_chunks=2,
                               localfile=False, prefix='unittest')
    await save_loop
    loader = dio.load(selector, prefix='unittest', localfile=False, fr=fr)
    df = await loader

    return df

async def test_option_trade_load():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='trade',
                                   symboltype='option')
    loader = dio.load(selector, prefix='unittest', localfile=False)
    df = await loader

    return df

async def test_spot_trade():
    selector = sel.StreamSelection(exchange='binance',
                                   symbols=['btcusdt','ethusdt'],
                                   datatype='trade',
                                   symboltype='spot')
    fr = time.time_ns()
    save_loop = gen.main_loop(selector, max_count=300,
                               timeout=10, max_chunks=2,
                               localfile=False, prefix='unittest')
    await save_loop
    loader = dio.load(selector, prefix='unittest', localfile=False, fr=fr)
    df = await loader

    return df

async def test_spot_trade_load():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='trade',
                                   symboltype='spot')
    loader = dio.load(selector, prefix='unittest', localfile=False)
    df = await loader

    return df

async def test_load(exchange='binance', datatype='trade',
                    symboltype='spot', symbols=None,
                    fr=0, to='now', prefix='unittest',
                    localfile=False):
    selector = sel.StreamSelection(exchange=exchange,
                                   datatype=datatype,
                                   symboltype=symboltype,
                                   symbols=symbols)
    loader = dio.load(selector, prefix=prefix,
                      fr=fr, to=to, localfile=localfile)
    df = await loader

    return df

async def test_perp_arr():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='arr',
                                   symboltype='perp')
    fr = time.time_ns()
    save_loop = gen.main_loop(selector, max_count=300,
                               timeout=10, max_chunks=2,
                               localfile=True, prefix='unittest')
    await save_loop
    loader = dio.load(selector, prefix='unittest', localfile=True, fr=fr)
    df = await loader

    return df

async def test_perp_arr_load():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='arr',
                                   symboltype='perp')
    loader = dio.load(selector, prefix='unittest', localfile=True)
    df = await loader

    return df

async def test_any(datatype, symboltype, symbols=None, localfile=True):
    selector = sel.StreamSelection(exchange='binance',
                                   datatype=datatype,
                                   symboltype=symboltype,
                                   symbols=symbols)
    fr = time.time_ns()
    save_loop = gen.main_loop(selector, max_count=300,
                               timeout=10, max_chunks=2,
                               localfile=localfile, prefix='unittest')
    await save_loop

    loader = dio.load(selector, prefix='unittest', localfile=localfile, fr=fr, force_reload=True)
    df = await loader

    return df

