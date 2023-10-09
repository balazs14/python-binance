import logging
logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)

import sys
import asyncio
import io
import aiofiles
import awswrangler as wr
import contextlib
import functools
import glob
import json
import os
import pandas as pd
import signal
import time

LOCAL_BUCKET = '/my/data'
S3_BUCKET = 's3://balazs-big-data-project'

async def save(start, end, stream_selector,
               data_df, **kwargs):
    prefix = kwargs.get('prefix', 'test')
    localfile = kwargs.get('localfile', True)
    symbols = stream_selector.extract_symbols(data_df)    
    index_df = pd.DataFrame({'symbol':symbols})
    timestamp = end
    index_df['timestamp'] = timestamp
    timestamp_str = str(pd.to_datetime(end)).replace(' ','_')
    index_df['start'] = start
    index_df['end'] = end
    exchange = stream_selector.exchange
    datatype =  stream_selector.datatype
    symboltype =  stream_selector.symboltype
    index_df['datatype'] = datatype
    index_df['symboltype'] = symboltype
    index_df['exchange'] = exchange

    index_df = index_df.set_index('symbol')
    if len(data_df) == 0: 
        logger.info(f'data size {len(data_df)} ignoring')
        return
    index_df['ndata'] = stream_selector.groupby_symbol(data_df).apply(len)
    
    data_dir  = f'{prefix}/data_df/{exchange}/{datatype}/{symboltype}'
    index_dir = f'{prefix}/index_df'
    data_key  = f'{data_dir}/{timestamp_str}.parquet'
    index_key = f'{index_dir}/{exchange}_{datatype}_{symboltype}_{timestamp_str}.csv'

    index_df['key'] = data_key

    index_df = index_df.reset_index()
    

    if localfile:
        os.makedirs(f'{LOCAL_BUCKET}/{data_dir}', exist_ok=True)
        os.makedirs(f'{LOCAL_BUCKET}/{index_dir}', exist_ok=True)
        async with aiofiles.open(f'{LOCAL_BUCKET}/{data_key}',
                                 mode='wb') as f:
            await f.write(data_df.to_parquet())
            await f.flush()
        async with aiofiles.open(f'{LOCAL_BUCKET}/{index_key}', mode='w') as f:
            await f.write(index_df.to_csv())
            await f.flush()
    else:
        wr.s3.to_parquet(
            data_df,
            path=f"{S3_BUCKET}/{data_key}",
        )
        wr.s3.to_csv(
            index_df,
            path=f"{S3_BUCKET}/{index_key}",
        )

    logger.info(f'saved index, data for {timestamp_str} localfile={localfile}\n{index_df}')
    logger.info(f'data size {len(data_df)} index size {len(index_df)}')

def load_index_df(prefix, localfile):
    index_dir = f'{prefix}/index_df'
    if localfile:
        all_files = glob.glob(f"{LOCAL_BUCKET}/{index_dir}/*.csv")
        index_df = pd.concat([pd.read_csv(f)
                              for f in all_files],
                              ignore_index=True)
    else:
        # index will not be used in load, but this is a way to store metadata
        def safe_read_csv(*args, **kwargs):
            try:
                return wr.s3.read_csv(*args, **kwargs)
            except wr.exceptions.NoFilesFound:
                return pd.DataFrame()
        index_df = safe_read_csv(
            path=f"{S3_BUCKET}/{index_dir}",
            path_suffix=".csv",
            use_threads=True)
    return index_df

def check_and_return_cached_df(key):
    if os.path.exists(key):
        return pd.read_csv(key)
    else:
        return None

def cache_df(df,key):
    df.to_csv(key)

def today_string(day=None):
    import datetime
    if day is None:
        today = datetime.datetime.today().date()
    else:
        today = pd.to_datetime(day).date()
    return f'{today.year:04d}-{today.month:02d}-{today.day:02d}'
    
def key_base(today=None):
    path = '/my/data/cached_index/' + today_string(today) + '/'
    import pathlib
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    return path

def index_df_cache_path(prefix, localfile=False, as_of_date=None):
    today = today_string(as_of_date)
    key = key_base(today) + f'_{prefix}_{"local" if localfile else "s3"}'
    return key

def get_cached_index_df(prefix, localfile=False, force_reload=False, as_of_date=None):
    key = index_df_cache_path(prefix, localfile, as_of_date)
    df = check_and_return_cached_df(key)
    if df is None or force_reload:
        df = load_index_df(prefix, localfile)
        cache_df(df, key)
    return df

def reset_index_df_cache(prefix, localfile=False, as_of_date=None):
    key = index_df_cache_path(prefix, localfile, as_of_date)
    if os.path.isfile(key):
        os.remove(key)
        logger.info(f"Index cache {key} removed")
    else:    
        logger.info(f"Index cache was empty: {key} file not found")
    
def index_df_get_keys(index_df, filter_fn=None):
    filtered = index_df[index_df.apply(filter_fn, axis = 1)] \
        if filter_fn else index_df
    return filtered.key

def index_df_get_symbols(index_df, filter_fn=None):
    filtered = index_df[index_df.apply(filter_fn, axis = 1)] \
        if filter_fn else index_df
    return filtered.symbol

def index_df_filtered(index_df, filter_fn=None):
    filtered = index_df[index_df.apply(filter_fn, axis = 1)] \
        if filter_fn else index_df
    return filtered

def filter_index_stream_selector(stream_selector, fr=None, to=None):
    symbols = stream_selector.symbols
    datatypes = stream_selector.datatype
    symboltypes = stream_selector.symboltype
    exchanges = stream_selector.exchange
    return filter_index_rows(symbols=symbols,
                             datatypes=datatypes,
                             exchanges=exchanges,
                             symboltypes=symboltypes,
                             fr=fr, to=to)

def filter_index_rows(symbols=None,
                      datatypes=None, exchanges=None,
                      symboltypes=None,
                      fr=None, to=None):
    timestamp_fr = pd.to_datetime('2000-01-01').value \
        if fr is None else pd.to_datetime(fr).value
    timestamp_to = time.time_ns() \
        if to is None else pd.to_datetime(to).value
    if symbols is not None and not isinstance(symbols, (list,tuple)):
        symbols = [symbols]
    if datatypes is not None and not isinstance(datatypes, (list,tuple)):
        datatypes = [datatypes]
    if exchanges is not None and not isinstance(exchanges, (list,tuple)):
        exchanges = [exchanges]
    if symboltypes is not None and not isinstance(symboltypes, (list,tuple)):
        symboltypes = [symboltypes]
    def _filter(x):
        start, end = int(x['start']), int(x['end'])
        res =  end>=timestamp_fr and start<=timestamp_to
        if 'datatype' in x and datatypes: res &= x['datatype'] in datatypes 
        if 'exchange' in x and exchanges: res &= x['exchange'] in exchanges 
        if 'symboltype' in x and symboltypes: res &= x['symboltype'] in symboltypes 
        # if the symboltype is option, then do not filter on symbols, because they are different in the past!
        if 'symboltype' in x and symboltypes and x['symboltype'] != 'option':
            if 'symbol' in x and symbols: res &= x['symbol'] in symbols 
        return res 
    return _filter

async def read_one_local_file(key):
    async with aiofiles.open(f'{LOCAL_BUCKET}/{key}', mode='rb') as f:
        content = await f.read()
        buf = io.BytesIO(content)
        data_df = pd.read_parquet(buf)
        return data_df
    
async def load_many_data_df(keys, localfile, df_filt_fn):
    if isinstance(keys,str): keys = [keys]
    if localfile:
        tasks = [read_one_local_file(key) for key in keys]
        res = await asyncio.gather(*tasks)
        if res == []: return pd.DataFrame()
        data_df = pd.concat(res)
    else:
        data_df = wr.s3.read_parquet(
            path=[f"{S3_BUCKET}/{key}" for key in keys],
            use_threads=True)
    if df_filt_fn is None:
        return data_df
    return df_filt_fn(data_df)
        
async def load_data_df(index_df, localfile, df_filt_fn):
    data_df = await load_many_data_df(index_df.key.unique(),
                                localfile, df_filt_fn)
    return data_df
        
async def load(stream_selector,
               fr=0, to='now', as_of_date=None, **kwargs):
    prefix = kwargs.get('prefix', 'test')
    localfile = kwargs.get('localfile', True)
    force_reload = kwargs.get('force_reload', False)
    index_df = get_cached_index_df(prefix, localfile, force_reload=force_reload, as_of_date=as_of_date)
    filter_fn = filter_index_stream_selector(stream_selector,
                                             fr=fr, to=to)
    filtered_index_df = index_df_filtered(index_df, filter_fn=filter_fn)
    df_filt_fn = stream_selector.filter_by_time(fr=fr, to=to)
    return await load_data_df(index_df=filtered_index_df,
                              localfile=localfile,
                              df_filt_fn=df_filt_fn)

def normalize_data_df(data_df):
    data_df = data_df.reset_index()
    if 'symbol' not in data_df:
        data_df['symbol'] = data_df.s
    if 'level_0' in data_df and 'timestamp' not in data_df:
        data_df['timestamp'] = data_df['level_0']
    return data_df

async def raw_data_iterator(stream_selector, fr=0, to='now', prefix='test', localfile=False, force_reload=False, chunked=True, as_of_date=None) :
    cached_index_df = get_cached_index_df(prefix=prefix, localfile=localfile, force_reload=force_reload, as_of_date=as_of_date)
    #pre-filter by time
    time_idx = (pd.to_datetime(fr).value <= cached_index_df.end) & (cached_index_df.start <= pd.to_datetime(to).value)
    cached_index_df = cached_index_df.loc[time_idx]
    filter_fn = filter_index_stream_selector(stream_selector, fr=fr, to=to)
    index_df = index_df_filtered(cached_index_df, filter_fn=filter_fn)
    df_filt_fn = stream_selector.filter_by_time(fr=fr, to=to)
    keys = index_df.key.unique()
    if localfile:
        for key in keys:
            df =  df_filt_fn(read_one_local_file(key))
            if df.empty: continue
            yield normalize_data_df(df)
    else:
        if chunked:
            data_iterator = wr.s3.read_parquet(
                path=[f"{S3_BUCKET}/{key}" for key in keys],
                use_threads=True, chunked=True)
            for data_df in data_iterator:
                df = df_filt_fn(data_df)
                if df.empty: continue
                logger.debug(f'{keys[0]} "{df.E.min()}", "{df.E.max()}" length {len(df)}')
                yield normalize_data_df(df)
        else:
            for key in keys:
                data_df = wr.s3.read_parquet(
                    path=f"{S3_BUCKET}/{key}",
                    use_threads=True)
                df = df_filt_fn(data_df)
                if df.empty: continue
                logger.debug(f'{key} first event time "{df.E.min()}", "{df.E.max()}" length {len(df)}')
                yield normalize_data_df(df)
                
class Subgrid:
    def __init__(self, grid, step):
        self.grid = grid
        self.step = step
        self.counter = 0
    def empty(self):
        return  self.startidx >= len(self.grid)
    def update(self):
        self.startidx = self.step * self.counter
        self.prestartidx = max(0, self.startidx-1)
        self.endidx = min(self.startidx + self.step, len(self.grid))
        if self.empty():
            return False
        self.startts = self.grid[self.startidx]
        self.prestartts = self.grid[self.prestartidx]
        self.endts = self.grid[self.endidx-1]
        return True
    def __iter__(self):
        return self
    def __next__(self):
        if not self.update() : raise StopIteration
        self.counter += 1
        return self
    def daterange(self):
        return self.grid[self.startidx:self.endidx]
    def __str__(self):
        return f'grid {self.grid}, subgrid {self.startts} {self.endts}'
    
class ChunkQueue:
    '''
    queue of dataframes with columns symbol and timestamp
    '''
    def __init__(self, aiterator):
        self.aiterator = aiterator
        self.queue = []
    def get_timestamp(self,ichunk,iitem):
        if not self.queue: return None
        if len(self.queue[ichunk]) == 0: return None
        return self.queue[ichunk].iloc[iitem].timestamp
    def first_start(self): return self.get_timestamp(0,0)
    def first_end(self): return self.get_timestamp(0,-1)
    def last_start(self): return self.get_timestamp(-1,0)
    def last_end(self): return self.get_timestamp(-1,-1)
    def info(self,tag):
        return f'{tag} queue length {len(self.queue)}' \
            f' fs {self.first_start()} fe {self.first_end()}' \
            f' ls {self.last_start()} le {self.last_end()}'
        
    def cleanup(self, subgrid):
        logger.debug(self.info('   clean'))
        if not self.queue: return None
        while (self.first_end() is not None) and (self.first_end() < subgrid.prestartts): 
            self.queue.pop(0)
            logger.debug(self.info('cleanup after pop'))
    async def catchup(self, subgrid):
        if subgrid.empty(): return pd.DataFrame()
        logger.debug(self.info('+ catchup'))
        while len(self.queue)==0  or self.last_start() < subgrid.endts:
            try:
                self.queue.append(await self.aiterator.__anext__())
                logger.debug(self.info('catchup after append'))
            except StopAsyncIteration as e:
                break
        if len(self.queue)>0 :
            return self.queue[-1]
        else:
            return pd.DataFrame()
    async def reindex(self, subgrid):
        last = await self.catchup(subgrid)
        if last.empty : return pd.DataFrame()
        self.cleanup(subgrid)
        if len(self.queue) == 0: return pd.DataFrame()
        df_all = pd.concat(self.queue)
        def _reindex(df):
            df = df.sort_index()
            df = df[~df.index.duplicated(keep='last')]
            df = df.reindex(subgrid.daterange(), method='ffill', limit=1)
            #df_clean.index.name = 'timestamp'
            #df_clean = df_clean.drop(columns=['symbol'])
            return df
        #df_final = df_all.set_index('timestamp').groupby('symbol').apply(lambda x: _reindex(x)).copy()
        df_final = _reindex(df_all.set_index(['timestamp','symbol']).unstack('symbol'))
        df_final.index.name = 'timestamp'
        df_final = df_final.stack('symbol').reset_index().copy()
        return df_final

        
async def azip_chunks(aits, grid, step):
    queues = [ChunkQueue(ait) for ait in aits]
    for subgrid in Subgrid(grid, step):
        tasks = [q.reindex(subgrid) for q in queues]
        dfs = await asyncio.gather(*tasks)
        yield dfs

