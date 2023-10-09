#!/bin/bash
export PYTHONPATH=/my
cd /my/binanceapi
# must exec python, so that signals (sigterm and sigint) are properly propagated from supervisor

exec python3 /my/binanceapi/data_saver2.py --prefix=prod1030 --max_count=1000000 --timeout=3600 $@

