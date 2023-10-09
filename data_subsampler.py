#! /usr/bin/python3
import asyncio
import binanceapi.utils as utils
import logging
import test.scratch as scr
logger = logging.getLogger('data_subsampler')

@utils.interruptible()
async def subsample_data(fr, to, as_of_date, freq, step):
    await scr.biglist2(fr=fr, to=to,  as_of_date=as_of_date, freq=freq, step=step)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Subsample Binance Data')
    parser.add_argument('--fr')
    parser.add_argument('--to', default=None)
    parser.add_argument('--as_of_date', default='2023-02-10')
    parser.add_argument('--freq', default='1min')
    parser.add_argument('--step', default=1, type=int)
    args = parser.parse_args()
    df = asyncio.run(subsample_data(**vars(args)))



