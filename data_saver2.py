#! /usr/bin/python3
import logging
import asyncio
import binanceapi.utils as utils
import binanceapi.generators as gen
from binanceapi.stream_selection import StreamSelection
logger = logging.getLogger('data_saver')

@utils.interruptible()
async def save_data(prefix='prod', exchange='binance',
                    symbols='btcusdt,ethusdt', datatype='trade',
                    symboltype='spot', timeout=3600, max_count=1000000,
                    localfile=False, debug=False):
    selector = StreamSelection(exchange=exchange, datatype=datatype,
                               symboltype=symboltype, symbols=symbols)
    logger.info(f'Saving  {selector}')
    await gen.main_loop(selector, timeout=timeout, localfile=localfile,
                        max_count=max_count, prefix=prefix)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Save Binance Data')
    parser.add_argument('--prefix', default='test')
    parser.add_argument('--exchange', default='binance')
    parser.add_argument('--datatype')
    parser.add_argument('--symboltype')
    parser.add_argument('--symbols', default=None)
    parser.add_argument('--timeout', default=3600, type=int)
    parser.add_argument('--max_count', default=1000000, type=int)
    parser.add_argument('--localfile', action='store_true')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    asyncio.run(save_data(**vars(args)))




