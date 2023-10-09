#! /usr/bin/python3
import logging
import asyncio
import binanceapi.utils as utils

logger = logging.getLogger('data_saver')

@utils.interruptible()
async def save_data(prefix='prod', streams=None, symbols='btcusdt,ethusdt',
                    datatypes='trade,depth20', timeout=10, counter=1000000, localfile=False,
                    is_option=False, debug=False):
    logger.info(f'starting to save data in {prefix}, timeout {timeout}, '
                f'counter {counter}, localfile {localfile} debug {debug}')
    filtered = utils.create_streams_list(streams, symbols, datatypes, is_option)
    if datatypes == 'arr':
        logger.info(f'listening to miniticker ({len(filtered)} symbols)')
    else:
        logger.info(f'listening to {len(filtered)} streams')
    utils.s3_bucket ='s3://balazs-big-data-project/' + prefix
    utils.local_bucket='/my/data/' + prefix
    await utils.multi_chunk(filtered, localfile=localfile, debug=debug, timeout=timeout, counter=counter, is_option=is_option)

@utils.interruptible()
async def test_save_data():
    await save_data(prefix='test', streams=None, symbols='BTCUSDT', datatypes='ticker',
                    timeout=10, counter=10, localfile=True, is_option=False,
                    debug=True)

@utils.interruptible()
async def test_save_data_option():
    await save_data(prefix='test', streams=None, symbols='ETH-220923-1600-P', datatypes='ticker',
                    timeout=10, counter=10, localfile=True, is_option=True,
                    debug=False)

@utils.interruptible()
async def test_save_data_all_options():
    await save_data(prefix='test', streams=None, symbols=None, datatypes='arr',
                    timeout=10, counter=100000, localfile=False, is_option=True,
                    debug=False)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Save Binance Data')
    parser.add_argument('--prefix', default='test')
    parser.add_argument('--streams', default=None)
    parser.add_argument('--symbols', default=None)
    parser.add_argument('--datatypes', default='depth20')
    parser.add_argument('--timeout', default=60, type=int)
    parser.add_argument('--counter', default=10, type=int)
    parser.add_argument('--localfile', action='store_true')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--is_option', action='store_true')
    args = parser.parse_args()
    asyncio.run(save_data(**vars(args)))




