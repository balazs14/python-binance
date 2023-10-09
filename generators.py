import asyncio
import io
import async_timeout
import aiofiles
import sammchardy_binance.binance as binance
import contextlib
import functools
import glob
import json
import os
import pandas as pd
import signal
import time
import binanceapi.dataio as dio
import binanceapi.utils as utils

import logging
logger = logging.getLogger(__name__)

class InterruptError(Exception):
    pass

class TimeoutError(Exception):
    pass


async def main_loop(stream_selector, **kwargs):
    process_chunk = stream_selector.get_chunk_processor()
    max_chunks = kwargs.get('max_chunks',0)
    ichunk = 0
    async for start, end, accumulator in get_chunk(
            stream_selector, **kwargs):
        ichunk += 1
        df = process_chunk(accumulator)
        await dio.save(start, end, stream_selector, df, **kwargs)
        if max_chunks > 0 and ichunk >= max_chunks: break

async def get_chunk(stream_selector,  **kwargs):
    stream_selector.reinit()
    max_count = kwargs.get('max_count', 100000)
    timeout  = kwargs.get('timeout', 10)
    async with contextlib.AsyncExitStack() as stack:
        try:
            process_payload = stream_selector.get_payload_processor()
            socket = await stream_selector.get_socket(stack)
            while True:
                try:
                    start = time.time_ns()
                    end, count, accumulator = start, 0, []
                    async with async_timeout.timeout(timeout
                                                     if timeout>0
                                                     else None):
                        logger.info(f'entered timeout context {timeout}'
                                     f' and count loop {max_count}')
                        while count < max_count:
                            payload = await socket.recv()
                            logger.debug(f'got tick {payload}'[:100])
                            end  = time.time_ns()
                            count = process_payload(count, accumulator,
                                                    payload, end)
                    end  = time.time_ns()
                    logger.info(f'reached max_count {count}')
                    yield start, end, accumulator

                except asyncio.TimeoutError as e:
                    end  = time.time_ns()
                    logger.info(f'caught timeout {e}, count was {count}')
                    yield start, end, accumulator
                pass # end timeout handling
            pass # end while True
        except (InterruptError, asyncio.CancelledError)  as e:
            end  = time.time_ns()
            logger.info(f'caught interrupt: {e}, count was {count}')
            yield start, end, accumulator
        except Exception as e:
            logger.info(f'caught unknown exception: {e}')

    pass # end async stack
    logger.info(f'Exiting generator')
    return

