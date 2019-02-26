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
from timeout_decorator import timeout, TimeoutError

__VERSION__ = '0.0.1'
__DESC__ = 'GEFS Getter'

URL_BASE = 'https://ftp.ncep.noaa.gov/data/nccf/com/gens/prod/gefs.{year:04d}{month:02d}{day:02d}/{hour:02d}/pgrb2ap5/ge{kind}.t{hour:02d}z.pgrb2a.0p50.f{ft:03d}'
FILE_BASE = 'gefs.{kind}.{year:04d}{month:02d}{day:02d}_{hour:02d}_{ft:03d}'

CACHE = pathlib.Path('/home/weather/cache/GEFS')
LOG = pathlib.Path('/home/weather/log/downloader_GEFS_0p5.log')

NUM_THREAD = 10

download_que = queue.Queue()
logger = logging.getLogger(__name__)

kind = ["avg", "c00", "p01", "p02", "p03", "p04", "p05", "p06", "p07", "p08", "P09", "p10", "p11", "p12", "p13", "p14", "p15", "p16", "p17", "p18", "p19", "p20", "spr"]

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
    target = now - datetime.timedelta(hours=3, minutes=30)
    if 0 <= target.hour < 6:
        target = target.replace(hour=0, minute=0)
    elif 6 <= target.hour < 12:
        target = target.replace(hour=6, minute=0)
    elif 12 <= target.hour < 18:
        target = target.replace(hour=12, minute=0)
    else:
        target = target.replace(hour=18, minute=0)
    return target


def make_que():
    for ft in range(0, 192, 3):
        for k in kind:
            download_que.put([ft, k])
    for ft in range(192, 384, 6):
        for k in kind:
            download_que.put([ft, k])

def downloader(init, data_dir):
    while True:
        ft, k = download_que.get()
        fp = data_dir / FILE_BASE.format(year=init.year, month=init.month, day=init.day, hour=init.hour, ft=ft, kind=k)
        fp_t = data_dir / (FILE_BASE.format(year=init.year, month=init.month, day=init.day, hour=init.hour, ft=ft, kind=k) + '.tmp')
        if fp.exists() is True:
            logger.info("{} kind={} FT={} alredy got".format(init, k, ft))
            download_que.task_done()
            continue
        url = URL_BASE.format(year=init.year, month=init.month, day=init.day, hour=init.hour, ft=ft, kind=k)

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
                logger.info("{} {} FT={} got".format(init, k, ft))
            else:
                download_que.put([ft, k])
                logger.info("{} {} FT={} retry (status={})".format(init, k, ft, res.status_code))
                time.sleep(60)
                download_que.task_done()

        except Exception as e:
            logger.info("{} {} FT={} Error".format(init, k, ft))
            logger.debug("{}".format(e))
            download_que.put([ft, k])
            time.sleep(60)
            download_que.task_done()

@timeout(6 * 3600)
def main(args):
    setup_logger()
    if args.init is None:
        init = get_init()
    else:
        init = datetime.datetime.strptime(args.init, '%Y%m%d%H')

    logger.info("Start {} GFS".format(init))

    if CACHE.is_dir() is False and CACHE.exists() is False:
        CACHE.mkdir(parents=True)
    make_que()

    for i in range(NUM_THREAD):
        worker = Thread(target=downloader, args=(init, CACHE))
        worker.setDaemon(True)
        worker.start()

    download_que.join()
    logger.info("End {} GFS".format(init))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    parser.add_argument('-i', '--init', metavar='INIT', type=str, help='Initial Time')
    args = parser.parse_args()

    try:
        main(args)
    except TimeoutError:
        logger.error("Total Timed out")
