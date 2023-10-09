import asyncio
import pandas as pd
import logging
logger = logging.getLogger(__name__)

import sammchardy_binance.binance as binance
from binanceapi.utils import sync_get_binance_client, get_binance_client

logger = logging.getLogger('stream_selection')

SYMBOLTYPE_SPOT = 'spot'
SYMBOLTYPE_PERP = 'perp'
SYMBOLTYPE_OPTION = 'option'


class StreamSelection:
    def __init__(self, exchange, datatype, symboltype,
                 symbols=None, symbol_filter=None):
        self.exchange = exchange
        self.datatype = datatype
        self.symboltype = symboltype
        # normalize symbols to upper case
        if isinstance(symbols, str):
            symbols = symbols.split(',')
        self._symbols = [s.upper() for s in symbols] \
            if symbols else None
        self.symbol_filter = symbol_filter
        self.reinit()

    def reinit(self):
        self.symbols = self._extract()
        self.stream_list = self._compute_stream_list()
        
    def __str__(self):
        return f'{self.exchange} {self.datatype} {self.symboltype} {self.symbols} ({len(self.symbols)} symbols)'
        
    def _extract(self):
        if self.exchange == 'binance':
            return [s.upper() for s in
                    self._binance_extract(self._symbols,
                                          self.symbol_filter)]
        else:
            raise NotImplementedError
        
    def _compute_stream_list(self):
        if self.exchange == 'binance':
            return self._binance_compute_stream_list()
        else:
            raise NotImplementedError
        
    def _create_filter(self, key, **kwargs):
        if key is None or key == 'NO_FILTER':
            return lambda x: True
        elif key == 'MARGINABLE':
            if self.symboltype != SYMBOLTYPE_SPOT:
                raise RuntimeError('MARGINABLE filter only available for spot')
            symbol_df = pd.DataFrame(self.exchange_info['symbols'])
            all_symbols_list = symbol_df.loc[symbol_df.permissions.apply(lambda x: 'MARGIN' in x), 'symbol']
            return lambda x: x in all_symbols_list
        else:
            raise NotImplementedError
    

    def _binance_extract(self, symbols, symbol_filter):
        '''access exchange resources to come up with a list of symbols
        The logic of determining what to listen to'''
        if self.symboltype == SYMBOLTYPE_OPTION:
            client = sync_get_binance_client()
            self.exchange_info = client.options_exchange_info()
            option_symbol_df = pd.DataFrame(
                self.exchange_info['optionSymbols'])
            #option symbols are uppercase :(
            all_symbols_list = option_symbol_df.symbol.values
        elif self.symboltype == SYMBOLTYPE_SPOT:
            client = sync_get_binance_client()
            self.exchange_info = client.get_exchange_info()
            symbol_df = pd.DataFrame(
                self.exchange_info['symbols'])
            all_symbols_list = [s.upper()
                                for s in symbol_df.symbol.values]
        elif self.symboltype == SYMBOLTYPE_PERP:
            client = sync_get_binance_client()
            self.exchange_info = client.futures_exchange_info()
            symbol_df = pd.DataFrame(
                self.exchange_info['symbols'])
            all_symbols_list = [s.upper()
                                for s in symbol_df.symbol.values]
                
        def in_original_list(s):
            return not symbols or s in symbols
        filter_fn = self._create_filter(key=symbol_filter)
        self.symbols = [s for s in all_symbols_list if in_original_list(s) and filter_fn(s) ]
                               
        if not all([s in all_symbols_list for s in self.symbols]):
            raise RuntimeError(f'there are invalid symbols among {self.symbols}')
        return self.symbols
    
    def _binance_compute_stream_list(self):
        if self.datatype == 'arr':
            return ['!miniTicker@arr']
        elif self.symboltype == SYMBOLTYPE_SPOT:
            if self.datatype in ['aggTrade','trade','bookTicker','depth20','depth5','depth10', 'index', 'ticker',]:
                return [s.lower() +'@'+self.datatype for s in self.symbols]            
        elif self.symboltype == SYMBOLTYPE_OPTION:
            if self.datatype in ['trade','bookTicker','depth20','depth5','depth10', 'index', 'ticker',]:
                return [s+'@'+self.datatype for s in self.symbols]
        elif self.symboltype == SYMBOLTYPE_PERP:
            if self.datatype in ['aggTrade','bookTicker','depth20','depth5','depth10', 'index', 'ticker',]:
                return [s.lower() +'@'+self.datatype for s in self.symbols]
        raise NotImplementedError

    #-----------------------------------------------------
    # get sockets

    async def get_socket(self, stack):
        if self.exchange == 'binance':
            return await self._binance_get_socket(stack)
        else:
            raise NotImplementedError

    async def _binance_get_socket(self, stack):
        client = await stack.enter_async_context(
            get_binance_client())
        bm = binance.BinanceSocketManager(client)
        logger.debug(f'socket manager {bm}')
        if self.symboltype == SYMBOLTYPE_OPTION:
            ts = bm.options_multiplex_socket(self.stream_list)
            logger.debug(f'options socket {ts}')
        elif self.symboltype == SYMBOLTYPE_SPOT:
            ts = bm.multiplex_socket(self.stream_list)
            logger.debug(f'spot socket {ts}')
        elif self.symboltype == SYMBOLTYPE_PERP:
            ts = bm.futures_multiplex_socket(self.stream_list)
            logger.debug(f'perp socket {ts}')
        tscm = await stack.enter_async_context(ts)
        return tscm
            
    #--------------------------------------------------------------
    # how to process output
    
    def get_payload_processor(self):
        if self.exchange == 'binance':
            if self.datatype == 'arr':
                return self._binance_arr_payload_processor()
            elif self.datatype.startswith('depth'):
                return self._binance_depth_payload_processor()
            else:
                return self._binance_other_payload_processor()
        else:
            raise NotImplementedError
        
    def get_chunk_processor(self):
        if self.exchange == 'binance':
            if self.datatype == 'arr':
                return self._binance_arr_chunk_processor()
            elif self.datatype.startswith('depth'):
                return self._binance_depth_chunk_processor()
            else:
                return self._binance_other_chunk_processor()
        else:
            raise NotImplementedError

    def extract_symbols(self, data_df):
        if self.exchange == 'binance':
            if len(data_df) == 0: return []
            return data_df.s.unique()
        else:
            raise NotImplementedError

    def groupby_symbol(self, data_df):
        if self.exchange == 'binance':
            return data_df.groupby('s')
        else:
            raise NotImplementedError

    def filter_by_time(self, fr, to):
        if fr is None: fr = 0
        if to is None: to = 'now'
        fr = pd.to_datetime(fr)
        to = pd.to_datetime(to)
        if fr>to:
            logger.error(f'from {fr} is later than to {to}')
        if self.exchange == 'binance':
            def _filter(df):
                if self.symboltype == SYMBOLTYPE_OPTION:
                    idx = pd.to_datetime(df.E) > fr
                    idx &= pd.to_datetime(df.E) <= to
                else:
                    idx = df.s.isin(self.symbols)
                    idx &= pd.to_datetime(df.E) > fr
                    idx &= pd.to_datetime(df.E) <= to
                return df.loc[idx]
            return _filter
        else:
            raise NotImplementedError

    #--------------------------------------------------------------
    # binance arr

    def _binance_arr_payload_processor(self):
        def process(count, accumulator, payload, timestamp):
            if 'data' not in payload:
                return count
            data = payload['data']
            df = pd.DataFrame(data)
            df['timestamp'] = timestamp
            accumulator.append(df)
            return count + len(data)
        return process

    def _binance_arr_chunk_processor(self):
        def process(accumulator):
            if len(accumulator) == 0: return pd.DataFrame()
            df =  pd.concat(accumulator)
            df['timestamp'] = pd.to_datetime(df.timestamp)
            df['E'] = pd.to_datetime(df.E, unit='ms')
            # normalize resolution
            df['timestamp'] = df.timestamp.astype('datetime64[us]')
            df['E'] = df.E.astype('datetime64[us]')
            return df
        return process

    #--------------------------------------------------------------
    # binance depth

    def _binance_depth_payload_processor(self):
        def process(count, accumulator, payload, timestamp):
            if 'data' not in payload:
                return count
            data = payload['data']
            symbol = payload['stream'].split('@')[0].upper()
            data['s'] = symbol
            data['timestamp'] = timestamp
            data['E'] = timestamp
            accumulator.append(data)
            return count + 1
        return process
        
    def _binance_depth_chunk_processor(self):
        def process(accumulator):
            if len(accumulator) == 0: return pd.DataFrame()
            df = pd.DataFrame(accumulator)
            df['timestamp'] = pd.to_datetime(df.timestamp)
            df['E'] = pd.to_datetime(df.E, unit='ns')
            # normalize resolution
            df['timestamp'] = df.timestamp.astype('datetime64[us]')
            df['E'] = df.E.astype('datetime64[us]')
            return df
        return process

    #--------------------------------------------------------------
    # binance other

    def _binance_other_payload_processor(self):
        def process(count, accumulator, payload, timestamp):
            if 'data' not in payload:
                return count
            data = payload['data']
            data['timestamp'] = timestamp
            accumulator.append(data)
            return count + 1
        return process
        
    def _binance_other_chunk_processor(self):
        def process(accumulator):
            if len(accumulator) == 0: return pd.DataFrame()
            df =  pd.DataFrame(accumulator)
            df['timestamp'] = pd.to_datetime(df.timestamp)
            df['E'] = pd.to_datetime(df.E, unit='ms')
            # normalize resolution
            df['timestamp'] = df.timestamp.astype('datetime64[us]')
            df['E'] = df.E.astype('datetime64[us]')
            return df
        return process

        
    
class DataSelection(StreamSelection):
    def __init__(self, exchange, datatype, symboltype,
                 symbols=None, symbol_filter=None, fr=0, to='now'):
        self.fr = fr
        self.to = to
        StreamSelection.__init__(self, exchange, datatype, symboltype,
                                 symbols, symbol_filter)
