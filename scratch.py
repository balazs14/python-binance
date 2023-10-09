import asyncio
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-8.8s] %(name)s -- %(message)s",
)
logging.getLogger().setLevel(logging.INFO)

import binanceapi.generators as gen
import binanceapi.dataio as dio
import binanceapi.stream_selection as sel
import time
import pandas as pd

async def test_loader():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='arr',
                                   symboltype='option')
    loader = dio.load(selector, prefix='prod1026',
                      fr='2022-10-27 01:00:00', to='2022-10-27 02:00:00', localfile=False)
    return await loader

def process_depth(row):
    bids = pd.DataFrame(np.stack(row.bids).astype(float),columns='p s'.split())
    asks = pd.DataFrame(np.stack(row.asks).astype(float),columns='p s'.split())

    return bids.p[0]

async def test_iterator(iterator):
    res = []
    async for df in iterator:
        res.append(df)
    return pd.concat(res)

def reindex(df):
    grid = pd.date_range('2022-10-27 01:00:00','2022-10-27 02:00:00', freq='1s')
    rei = df.set_index('E').groupby('s').apply(lambda x: x.reindex(grid,method='ffill'))
    print (rei.reset_index(drop=True).groupby('s').apply(len))

async def biglist(fr='2022-10-27 00:00:00', to='2022-10-27 00:30:00', freq='600s', as_of_date=None)  :
    from stream_selection import StreamSelection
    import dataio as dio

    async def one(sel):
        grid = pd.date_range(fr,to,freq=freq)
        lis = []
        async_iterator = dio.raw_data_iterator(sel, prefix='prod1026',
                                               localfile=False, force_reload=False, fr=fr, to=to, as_of_date=as_of_date)
        async for df in  dio.chunked_data_iterator( grid, 10, async_iterator):
            lis.append(df)
        return pd.concat(lis)

    perp = await one(StreamSelection('binance','aggTrade','perp','ETHUSDT'))
    spot = await one(StreamSelection('binance','aggTrade','spot','ETHUSDT'))
    option = await one(StreamSelection('binance','arr','option'))
    option.index.names=['s1','timestamp']
    option = option.reset_index().drop(columns=['s1'])
    spot.index.names=['s1','timestamp']
    spot = spot.reset_index().drop(columns=['s1'])
    perp.index.names=['s1','timestamp']
    perp = perp.reset_index().drop(columns=['s1'])

    def _split_option_symbol(x):
        if isinstance(x,pd._libs.missing.NAType): return x, x, x, x
        spl = x.split('-')
        underlying = spl[0]+'USDT'
        expiry = pd.to_datetime('20'+spl[1]+' 08:00:00')
        strike = float(spl[2])
        putcall = spl[3]
        return underlying, expiry, strike, putcall

    option['U'] = option.s.apply(lambda x:_split_option_symbol(x)[0])
    option['EXP'] = option.s.apply(lambda x:_split_option_symbol(x)[1])
    option['K'] = option.s.apply(lambda x:_split_option_symbol(x)[2])
    option['pc'] = option.s.apply(lambda x:_split_option_symbol(x)[3])

    option['tte'] = pd.to_datetime(option.EXP) - option.timestamp

    option = option.query('U=="ETHUSDT"')
    option = option.set_index(['U','timestamp'])
    option['UP'] = spot.set_index(['s','timestamp']).p.astype(float)

    option['moneyness'] = option.UP - option.K

    return perp, spot, option

def option_process(df):
    for col in ['index',  'o', 'h', 'l', 'c', 'V', 'A', 'P',
                'p', 'Q', 'bo', 'ao', 'bq', 'aq', 'b', 'a', 'd', 't', 'g', 'v', 'vo',
                'mp', 'hl', 'll', 'eep']:
        df[col] = df[col].astype(float)
    df['underlying exp K pc'.split()] = df.symbol.str.split('-', expand=True)
    df['underlying'] = df.underlying+'USDT'
    df['exp'] = pd.to_datetime('20'+df.exp +' 08:00:00')
    df['K'] = df.K.astype(float)
    df['T'] = (pd.to_datetime(df.exp) - df.timestamp)/pd.Timedelta('365D')
    df = df.rename(columns={'bo':'Pbid',
                            'ao': 'Pask',
                            'c' : 'Plast',
                            'bq': 'Qbid',
                            'aq': 'Qask',
                            'Q' : 'Qlast',
                            'b' : 'Ibid',
                            'a' : 'Iask',
                            'vo': 'Imid',
                            'V' : 'Vol24',
    })
    df = df.query('underlying=="ETHUSDT"')
    df = df.set_index(['underlying','timestamp'])
    return df

def depth_process(df, levels, perp):
    bids_col = 'b' if perp else 'bids'
    asks_col = 'a' if perp else 'asks'
    df[[f'bid{li}' for li in range(levels)]] = pd.DataFrame(df[bids_col].to_list(), index=df.index)
    df[[f'ask{li}' for li in range(levels)]] = pd.DataFrame(df[asks_col].to_list(), index=df.index)
    for li in range(levels):
        df[[f'bp{li}', f'bq{li}']] = pd.DataFrame(df[f'bid{li}'].to_list(), index=df.index)
        df[[f'ap{li}', f'aq{li}']] = pd.DataFrame(df[f'ask{li}'].to_list(), index=df.index)
        df[f'bp{li}'] = df[f'bp{li}'].astype(float)
        df[f'bq{li}'] = df[f'bq{li}'].astype(float)
        df[f'ap{li}'] = df[f'ap{li}'].astype(float)
        df[f'aq{li}'] = df[f'aq{li}'].astype(float)

    df = df.drop(columns=[f'bid{li}' for li in range(levels)])
    df = df.drop(columns=[f'ask{li}' for li in range(levels)])
    df = df.drop(columns=[bids_col,asks_col])

    return df

def spot_process(df):
    for col in ['p','q']:
        df[col] = df[col].astype(float)
    return df


async def biglist2(fr='2022-10-30 00:00:00', to=None, freq='1min', step=1, as_of_date=None):
    from stream_selection import StreamSelection
    import dataio as dio

    fr = pd.Timestamp(fr)
    if to is None:
        to = fr + pd.Timedelta('1D')
    else :
        to = pd.Timestamp(to)

    grid = pd.date_range(fr,to,freq=freq)


    names = ['spot', 'sdepth', 'option','perp','pdepth'][:]
    sels = [
        StreamSelection('binance','aggTrade','spot',['ETHUSDT','BTCUSDT','BNBUSDT']),
        StreamSelection('binance','depth5','spot',['ETHUSDT','BTCUSDT','BNBUSDT']),
        StreamSelection('binance','arr','option'),
        StreamSelection('binance','aggTrade','perp',['ETHUSDT','BTCUSDT','BNBUSDT']),
        StreamSelection('binance','depth5','perp',['ETHUSDT','BTCUSDT','BNBUSDT']),
    ]
    aits = [dio.raw_data_iterator(sel, prefix='prod1030',
                                  localfile=False,
                                  force_reload=False,
                                  as_of_date=as_of_date,
                                  fr=fr, to=to) for sel in sels[:]]

    df_lists = {name:[] for name in names}

    async for dfs in dio.azip_chunks(aits, grid, step=step):
        if any(x.empty for x in dfs): continue
        dfmap = dict(zip(names,dfs))

        for name, df in dfmap.items():
            print (name,df.timestamp.min(), df.timestamp.max())

        if 'perp' in dfmap:
            perp = spot_process(dfmap['perp'])
            df_lists['perp'].append(perp)
        if 'pdepth' in dfmap:
            pdepth = depth_process(dfmap['pdepth'],5,perp=True)
            df_lists['pdepth'].append(pdepth)
        if 'spot' in dfmap:
            spot = spot_process(dfmap['spot'])
            df_lists['spot'].append(spot)
        if 'sdepth' in dfmap:
            sdepth = depth_process(dfmap['sdepth'],5,perp=False)
            df_lists['sdepth'].append(sdepth)
        if 'option'in dfmap:
            option = option_process(dfmap['option'])
            if 'perp' in dfmap:
                option['Rlast'] = perp.set_index(['symbol','timestamp']).p
            if 'pdepth' in dfmap:
                option['Rbid'] = pdepth.set_index(['symbol','timestamp']).bp0
                option['Rask'] = pdepth.set_index(['symbol','timestamp']).ap0
            if 'spot' in dfmap:
                option['S'] = spot.set_index(['symbol','timestamp']).p
                option['moneyness'] = option.S - option.K
            if 'sdepth' in dfmap:
                option['Sbid'] = sdepth.set_index(['symbol','timestamp']).bp0
                option['Sask'] = sdepth.set_index(['symbol','timestamp']).ap0
            df_lists['option'].append(option)


    df_all = {k:pd.concat(df_lists[k]) for k in dfmap}
    for k in dfmap:
        df_all[k].to_parquet(f'/my/notebooks/{k}_{fr.date()}.parquet')

    return df_all

if __name__ == "__main__":
    for fr in ['2023-01-'+s for s in '25 26 27 28 29'.split()]:
        fr = pd.to_datetime(fr)
        asyncio.run(biglist2(fr, to, freq='1min', step=5, as_of_date='2023-02-10'))

