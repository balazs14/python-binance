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

import asyncio
import io
import async_timeout
import aiofiles
import awswrangler as wr
import sammchardy_binance.binance as binance
import contextlib
import functools
import glob
import json
import os
import pandas as pd
import signal
import time

logger = logging.getLogger('utils')

exchange = 'binance'

def set_layer(layer='test'):
    global local_bucket
    global s3_bucket
    local_bucket='/my/data/'+layer
    s3_bucket = 's3://balazs-big-data-project/'+layer


@contextlib.asynccontextmanager
async def get_binance_client():
    from local.binance_api_key import balazs_trader_api_key, balazs_trader_api_secret
    client = await binance.AsyncClient.create(api_key=balazs_trader_api_key,
                                              api_secret=balazs_trader_api_secret)
    logger.info(f'got client {client}')
    yield client
    await client.close_connection()
    logger.info('closed connection')


def sync_get_binance_client():
    from sammchardy_binance.binance.client import Client
    from local.binance_api_key import balazs_trader_api_key, balazs_trader_api_secret
    client = Client(balazs_trader_api_key, balazs_trader_api_secret)
    return client

class InterruptError(Exception):
    pass

class TimeoutError(Exception):
    pass


def stream_included(stream, streams, symbols, datatypes):
    if datatypes == 'arr':
        return stream.endswith('@arr')
    if not streams and not symbols and not datatypes : return True
    if bool(streams):
        if not isinstance(streams, (list,tuple)):
            streams = streams.split(',')
        return stream in streams
    sym, dt = stream.split('@')[0], '@'.join(stream.split('@')[1:])
    res = True
    if bool(symbols):
        if not isinstance(symbols, (list,tuple)):
            symbols = symbols.split(',')
        symbols = [ s.lower() for s in symbols]
        res &= sym.lower() in symbols
    if bool(datatypes):
        if not isinstance(datatypes, (list,tuple)):
            datatypes = datatypes.split(',')
        res &= dt in datatypes
    return res

async def save(index_df, dic, localfile=False, **kwargs):
    index_df = index_df.reset_index()

    timestamp = time.time_ns()
    index_df['timestamp'] = timestamp

    datatypes = index_df.datatype.unique()
    streams = {d: index_df.query(f'datatype=="{d}"').stream.unique() for d in datatypes  }
    data_dfs = {d: pd.concat([pd.DataFrame(v) for k,v in dic.items() if k in streams[d]])
                for d in datatypes}
    num_data = {}
    assert len(index_df.exchange.unique()) == 1
    exchange = index_df.exchange.unique()[0]

    for datatype in datatypes:
        data_df = data_dfs[datatype]
        num_data[datatype] = len (data_df)
        if data_df.empty: continue
        key = f'{exchange}/{datatype}/{timestamp}.parquet'
        index_df.loc[(index_df.datatype == datatype),'key'] = key
        if localfile:
            dirname = f'{local_bucket}/data_df/{exchange}/{datatype}'
            os.makedirs(dirname, exist_ok=True)
            if len(data_df):
                async with aiofiles.open(f'{local_bucket}/data_df/' + key,
                                         mode='wb') as f:
                    await f.write(data_df.to_parquet())
                    await f.flush()
        else:
            data_df['start'] = int(index_df.start[0])
            data_df['end'] = int(index_df.end[0])
            wr.s3.to_parquet(
                data_df,
                path=f"{s3_bucket}/data_df/{exchange}/{datatype}",
                dataset=True,
                partition_cols=['stream','start','end']
            )

    logger.info(f'saving localfile={localfile}\n{index_df}')

    if localfile:
        dirname = f'{local_bucket}/index_df'
        os.makedirs(dirname, exist_ok=True)
        fname = f'{local_bucket}/index_df/{timestamp}.csv'
        logger.debug(fname)
        async with aiofiles.open(fname, mode='w') as f:
            await f.write(index_df.to_csv())
            await f.flush()
    else:
        # index will not be used in load, but this is a way to store metadata
        wr.s3.to_csv(
            index_df,
            path=f"{s3_bucket}/index_df",
            dataset=True,
            partition_cols=['timestamp','stream','start', 'end']
        )

    logger.info(f'saved index, data for {pd.Timestamp(timestamp)} {timestamp}')
    logger.info(f'data size {num_data}')


async def load(streams=None, symbols=None, datatypes=None,
               fr=None, to=None, localfile=False, return_index_df=False,  **kwargs):
    timestamp_fr = pd.Timestamp('2000-01-01').value \
        if fr is None else pd.Timestamp(fr).value
    timestamp_to = time.time_ns() \
        if to is None else pd.Timestamp(to).value
    if streams is not None and not isinstance(streams, (list,tuple)): streams = [streams]
    if symbols is not None and not isinstance(symbols, (list,tuple)): symbols = [symbols]
    if datatypes is not None and not isinstance(datatypes, (list,tuple)): datatypes = [datatypes]
    logger.info(f'getting localfile={localfile} from {pd.Timestamp(fr)} to {pd.Timestamp(to)} {streams}')
    def _filter(x):
        start, end = int(x['start']), int(x['end'])
        return end>=timestamp_fr \
            and start<=timestamp_to \
            and stream_included(x['stream'], streams, symbols, datatypes)

    if localfile:
        all_files = glob.glob(f"{local_bucket}/index_df/*.csv")
        index_df = pd.concat([pd.read_csv(f)
                              for f in all_files],
                              ignore_index=True)
        index_df = index_df[index_df.apply(_filter, axis = 1)]
        async def read_one(key):
            async with aiofiles.open(f'{local_bucket}/data_df/{key}', mode='rb') as f:
                content = await f.read()
            buf = io.BytesIO(content)
            df = pd.read_parquet(buf)
            return  df
        bar = [read_one(k) for k in index_df.key.unique() ]
        foo = await asyncio.gather(*bar)
        data_df = pd.concat(foo)

    else:
        # index will not be used in load, but this is a way to store metadata
        def safe_read_csv(*args, **kwargs):
            try:
                return wr.s3.read_csv(*args, **kwargs)
            except wr.exceptions.NoFilesFound:
                return pd.DataFrame()
        index_df = safe_read_csv(
            path=f"{s3_bucket}/index_df",
            dataset=True,
            partition_filter = _filter)
        index_df = index_df[index_df.apply(_filter, axis = 1)]

        def safe_read_parquet(*args, **kwargs):
            try:
                return wr.s3.read_parquet(*args, **kwargs)
            except wr.exceptions.NoFilesFound:
                return pd.DataFrame()

        data_df = pd.concat([ safe_read_parquet(
        path=f"{s3_bucket}/data_df/{exchange}/{datatype}",
        dataset=True,
        partition_filter = _filter) for datatype in datatypes])

    data_df = data_df[data_df.apply(lambda x: x.recv >= timestamp_fr
                                    and x.recv <= timestamp_to
                                    and stream_included(x.stream, streams, symbols, datatypes), axis=1)]
    if return_index_df:
        return data_df, index_df
    else:
        return data_df

def create_option_streams_list(streams=None, symbols=None, datatypes=None):
    client = sync_get_binance_client()
    info = client.options_exchange_info()
    option_symbol_df = pd.DataFrame(info['optionSymbols'])
    #option symbols are uppercase :(
    symbol_list = option_symbol_df.symbol.values
    stream_list = [s + '@' + t
                   for s in symbol_list
                   for t in ['trade','bookTicker','depth20','depth5','depth10', 'index', 'ticker', 'arr'] ]

    filtered = [ s for s in stream_list if stream_included(s, streams, symbols, datatypes)]
    return filtered

def create_spot_streams_list(streams=None, symbols=None, datatypes=None):
    client = sync_get_binance_client()
    info = client.get_exchange_info()
    symbol_df = pd.DataFrame(info['symbols'])
    symbol_list = symbol_df.loc[symbol_df.permissions.apply(lambda x: 'MARGIN' in x), 'symbol']
    stream_list = [(s.upper() if t=='arr' else s.lower()) + '@' + t
                   for s in symbol_list
                   for t in ['trade','bookTicker','depth20','depth5','depth10', 'ticker', 'arr'] ]

    filtered = [ s for s in stream_list if stream_included(s, streams, symbols, datatypes)]
    return filtered

def create_streams_list(streams=None, symbols=None, datatypes=None, is_option=False):
    if is_option:
        return create_option_streams_list(streams, symbols, datatypes)
    else:
        return create_spot_streams_list(streams, symbols, datatypes)

def interruptible():
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            global stop_event
            stop_event = asyncio.Event()
            def inner_ctrl_c_signal_handler(sig, frame):
                '''
                function that gets called when the user issues a
                keyboard interrupt (ctrl+c)
                '''
                logger.info("SIGINT caught!")
                stop_event.set()
            def inner_sigterm_signal_handler(loop):
                logger.info('received SIGINT')
                for task in asyncio.Task.all_tasks():
                    task.cancel()
                logger.info('raised CancelledError')
            # signal.signal(signal.SIGINT, inner_ctrl_c_signal_handler)
            loop = asyncio.get_event_loop()
            loop.add_signal_handler(signal.SIGINT, functools.partial(
                inner_sigterm_signal_handler, loop))
            return await func(*args, **kwargs)

        return wrapped
    return wrapper

def mark_up_index_df(index_df):
    index_df = index_df.reset_index()
    index_df['symbol'] = index_df.apply(lambda x: x.stream.split('@')[0].upper(), axis=1)
    index_df['datatype'] = index_df.apply(lambda x: '@'.join(x.stream.split('@')[1:]), axis=1)
    index_df['exchange'] = exchange
    index_df['ndata'] = 0
    index_df = index_df.set_index('stream')
    return index_df

def index_df_from_streams(streams):
    if not isinstance(streams, (list,tuple)):
        streams = [streams]
    index_df = mark_up_index_df(pd.DataFrame({'stream': streams}))
    return index_df

def process_payload(idf, res, data, timestamp, stream):
    if stream=='!miniTicker@arr':
        for d in data:
            d['recv'] = timestamp
            key = d['s'].upper() + '@arr'
            d['stream'] = key
            if key in res:
                res[key].append(d)
                idf.loc[key,'ndata'] = idf.loc[key,'ndata'] + 1
    else:
        data['recv'] = timestamp
        data['stream'] = stream
        res[stream].append(data)
        idf.loc[stream,'ndata'] = idf.loc[stream,'ndata'] + 1
    return

async def multi_chunk(index_df, **kwargs):
    if kwargs.get('debug', False):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if isinstance(index_df,str):
        index_df = [index_df]
    if isinstance(index_df, list):
        index_df = index_df_from_streams(index_df)
    async for idf, dic in get_chunk(index_df, **kwargs):
        await save(idf, dic, **kwargs)

async def get_chunk(index_df, counter=100000, timeout=10, **kwargs):
    idf = mark_up_index_df(index_df)
    idf['start'] = time.time_ns()
    idf['end'] = idf.start

    streams = idf.index.to_list()
    # collect the @arr datatypes into a single stream
    streams_has_arr = '@arr' in ','.join(streams)
    streams_sans_arr = [s for s in streams if '@arr' not in s]
    if streams_has_arr:
        streams = streams_sans_arr + ['!miniTicker@arr']
    datatypes = idf.datatype.unique()
    res = {}

    async with contextlib.AsyncExitStack() as stack:
        client = await stack.enter_async_context(get_binance_client())
        bm = binance.BinanceSocketManager(client)
        logger.debug(f'socket manager {bm}')
        try: # handle interrupts, cancels
            if kwargs.get('is_option', False):
                ts = bm.options_multiplex_socket(streams)
                logger.debug(f'options socket {ts}') 
            else:
                ts = bm.multiplex_socket(streams)
                logger.debug(f'spot socket {ts}')
            async with ts as tscm:
                while True:
                    idf['start'] = time.time_ns()
                    try: #handle timeout
                        count = counter
                        res = {d:[] for d in idf.index.to_list()}
                        async with async_timeout.timeout(timeout if timeout>0 else None):
                            logger.debug(f'entered timeout context')
                            while count:
                                logger.debug(f'entered count loop')
                                payload = await tscm.recv()
                                logger.debug(f'got tick {payload}')
                                timestamp = time.time_ns()
                                if 'data' in payload:
                                    stream = payload['stream']
                                    process_payload(idf, res, payload['data'], timestamp, stream)
                                    count = count - 1
                        idf['end'] = time.time_ns()
                        yield idf, res


                    except asyncio.TimeoutError as e:
                        logger.info(f'caught timeout {e}')
                        idf['end'] = time.time_ns()
                        yield idf, res
                pass # end while true
            pass # end async with ts
        except (InterruptError, asyncio.CancelledError)  as e:
            logger.info(f'caught interrupt: {e}')
            idf['end'] = time.time_ns()
            yield idf, res
        except Exception as e:
            logger.info(f'caught unknown exception: {e}')


    logger.info(f'Exiting generator')
    return

