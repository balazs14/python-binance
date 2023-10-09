#! /usr/bin/python3
import asyncio
import binanceapi.utils as utils
import logging

logger = logging.getLogger('data_loader')

@utils.interruptible()
async def load_data(prefix='prod', streams=None, symbols='btcusdt,ethusdt',
                    datatypes='trade,depth20', localfile=False, fr=None, to=None,
                    return_index_df=False, is_option=False):
    logger.info(f'starting to load data in {prefix}, '
                f'localfile {localfile}')
    filtered = utils.create_streams_list(streams, symbols, datatypes, is_option)
    if datatypes == 'arr':
        logger.info(f'reading  miniticker ({len(filtered)} symbols)')
    else:
        logger.info(f'reading  {len(filtered)} streams')
    utils.s3_bucket ='s3://balazs-big-data-project/' + prefix
    utils.local_bucket='/my/data/' + prefix
    datatypes = [] if datatypes is None else datatypes.split(',')
    symbols = [] if symbols is None else symbols.split(',')
    return await utils.load(streams=filtered, symbols=symbols, datatypes=datatypes,
                            localfile=localfile, fr=fr, to=to,
                            return_index_df=return_index_df)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Load Binance Data')
    parser.add_argument('--prefix', default='prod1030')
    parser.add_argument('--streams', default=None)
    parser.add_argument('--symbols', default=None)
    parser.add_argument('--datatypes', default='trade,bookTicker,depth20')
    parser.add_argument('--localfile', default=False)
    parser.add_argument('--fr', default=None)
    parser.add_argument('--to', default=None)
    args = parser.parse_args()
    df = asyncio.run(load_data(**vars(args)))



