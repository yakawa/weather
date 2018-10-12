#!/usr/bin/env python3

import os
import queue
import datetime
import time
from threading import Thread
import json
import logging
import logging.handlers
import argparse
import pathlib

import requests

__VERSION__ = '0.0.1'
__DESC__ = 'GFS SST Getter'

URL_BASE = 'http://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/sst.{year:04d}{month:02d}{day:02d}/rtgssthr_grb_0.083.grib2'
FILE_BASE = 'sst.{year:04d}{month:02d}{day:02d}'

CACHE = pathlib.Path('/home/weather/cache/SST')
LOG = pathlib.Path('/home/weather/log/downloader_GFS_SST.log')

NUM_THREAD = 1

download_que = queue.Queue()
logger = logging.getLogger(__name__)

def setup_logger():
    handler = logging.handlers.RotatingFileHandler(str(LOG), maxBytes=1*1024*1024, backupCount=10)
    formatter = logging.Formatter('%(asctime)s [%(levelname)-8s] - %(message)s')
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)


def get_init():
    now = datetime.datetime.utcnow()
    now = now.replace(second=0, microsecond=0)
    target = now
    if 23 <= target.hour:
        target = target.replace(hour=0, minute=0)
    else:
        target = (target - datetime.timedelta(days=1)).replace(hour=0, minute=0)
    return target


def make_que():
    for ft in range(1):
        download_que.put(ft)


def downloader(init, data_dir):
    while True:
        ft = download_que.get()
        fp = data_dir / FILE_BASE.format(year=init.year, month=init.month, day=init.day)
        fp_t = data_dir / (FILE_BASE.format(year=init.year, month=init.month, day=init.day) + '.tmp')
        if fp.exists() is True:
            logger.info("{} alredy got".format(init))
            download_que.task_done()
            continue
        url = URL_BASE.format(year=init.year, month=init.month, day=init.day)

        try:
            if (fp_t).exists() is True:
                (fp_t).unlink()

            print(url)
            res = requests.get(url,stream=True, timeout=60)

            if res.status_code == 200 or res.status_code == 206:
                with fp_t.open(mode='wb') as f:
                    for chunk in res.iter_content(2 * 1024 * 1024):
                        f.write(chunk)
                fp_t.rename(fp)
                download_que.task_done()
                logger.info("{} got".format(init))
            else:
                download_que.put(ft)
                logger.info("{} retry (status={})".format(init, res.status_code))
                time.sleep(60)
                download_que.task_done()

        except Exception as e:
            logger.info("{} Error".format(init))
            logger.debug("{}".format(e))
            download_que.put(ft)
            time.sleep(60)
            download_que.task_done()


def main(args):
    setup_logger()
    if args.init is None:
        init = get_init()
    else:
        init = datetime.datetime.strptime(args.init, '%Y%m%d')

    logger.info("Start {} SST".format(init))

    if CACHE.is_dir() is False and CACHE.exists() is False:
        CACHE.mkdir(parents=True)
    make_que()

    for i in range(NUM_THREAD):
        worker = Thread(target=downloader, args=(init, CACHE))
        worker.setDaemon(True)
        worker.start()

    download_que.join()
    logger.info("End {} SST".format(init))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    parser.add_argument('-i', '--init', metavar='INIT', type=str, help='Initial Time')
    args = parser.parse_args()

    main(args)
