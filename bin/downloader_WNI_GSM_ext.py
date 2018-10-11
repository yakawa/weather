#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import hashlib
import argparse
import json
import os
import logging
import logging.handlers
import sys
import fcntl

import requests
import timeout_decorator

HOME = os.path.expanduser('~')
CONFIG = HOME + '/.weather.json'
CACHE = HOME + '/cache/GSM'
LOG = HOME + '/log/downloader_WMI_GSM.log'
LOCK = HOME + '/lock/downloader_WNI_GSM_ext.lock'

URL = ['http://labs.weathernews.jp/JMA_GSM/nph-dl.cgi?FILE=GRIB2/{year:04d}/{month:02d}/{day:02d}/Z__C_RJTD_{year:04d}{month:02d}{day:02d}{hour:02d}0000_GSM_GPV_Rjp_L-pall_FD0318-0800_grib2.bin',
       'http://labs.weathernews.jp/JMA_GSM/nph-dl.cgi?FILE=GRIB2/{year:04d}/{month:02d}/{day:02d}/Z__C_RJTD_{year:04d}{month:02d}{day:02d}{hour:02d}0000_GSM_GPV_Rjp_Lsurf_FD0315-0800_grib2.bin',
       'http://labs.weathernews.jp/JMA_GSM/nph-dl.cgi?FILE=GRIB2/{year:04d}/{month:02d}/{day:02d}/Z__C_RJTD_{year:04d}{month:02d}{day:02d}{hour:02d}0000_GSM_GPV_Rjp_L-pall_FD0806-1100_grib2.bin',
       'http://labs.weathernews.jp/JMA_GSM/nph-dl.cgi?FILE=GRIB2/{year:04d}/{month:02d}/{day:02d}/Z__C_RJTD_{year:04d}{month:02d}{day:02d}{hour:02d}0000_GSM_GPV_Rjp_Lsurf_FD0803-1100_grib2.bin',       
]


logger = logging.getLogger(__name__)
handler = logging.handlers.RotatingFileHandler(LOG, maxBytes=1*1024*1024, backupCount=10)
formatter = logging.Formatter('%(asctime)s [%(levelname)-8s] - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

def get_file(url, token):
    t = url.split('/')[-1]
    if os.path.isfile(CACHE + '/' + t) is True:
        logger.info('Already got: {}'.format(t))
        return t
    cookies = {'mdbauth': token}
    req = requests.get(url, cookies=cookies)
    if req.status_code == 200:
        with open(CACHE + '/' + t + '.tmp', 'wb') as f:
            f.write(req.content)
        os.rename(CACHE + '/' + t + '.tmp', CACHE + '/' + t)
        logger.info('Downloaded: {}'.format(t))
        return t
    return

    
def get_gsm_jp(init, token, url):
    ret = []
    u = url.format(year=init.year, month=init.month, day=init.day, hour=init.hour)
    ret.append(get_file(u, token))
    
    return ret

    
def get_authority(userid, passwd):
    url = 'http://weathernews.jp/my/setting/cgi_ua/mylogin.cgi'
    params = {
        'mwsid': userid,
        'menu': 'default',
        'mwspw': passwd,
    }
    res = requests.put(url, data=params)
    if res.status_code != 200:
        return None
    return res.cookies['mdbauth']

    
def get_init_time():
    now = datetime.datetime.utcnow()
    now = now.replace(second=0, microsecond=0)
    target = now - datetime.timedelta(hours=6, minutes=0)
    if 0 <= target.hour < 12:
        target = target.replace(hour=12, minute=0) - datetime.timedelta(days=1)
    else:
        target = target.replace(hour=12, minute=0)
    return target
    

@timeout_decorator.timeout(10*60)
def main(args):
    with open(CONFIG, 'r') as f:
        conf = json.loads(f.read())
    auth = get_authority(conf['WNI']['email'], conf['WNI']['passwd'])

    init = get_init_time()

    for url in URL:
        get_gsm_jp(init, auth, url)

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GFS Getter')
    parser.add_argument('-i', '--init', metavar='INIT', type=str, help='Initial Time', default=None)
    
    args = parser.parse_args()
    with open(LOCK, "w") as lockFile:
        try:
            fcntl.flock(lockFile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(args)
        except IOError:
            logger.info('process already exists')
            sys.exit(0)
    
