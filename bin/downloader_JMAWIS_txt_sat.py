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
CACHE = HOME + '/cache/JMAWIS'
LOG = HOME + '/log/downloader_JMAWIS.log'
LOCK = HOME + '/lock/downloader_JMAWIS_txt_sat.lock'

URL = 'http://www.wis-jma.go.jp/data/syn?ContentType=Text&Category=Satellite&Type=Alphanumeric&Access=Open&Subcategory=SARAD&Subcategory=SATEM&Subcategory=SATOB'

logger = logging.getLogger(__name__)
handler = logging.handlers.RotatingFileHandler(LOG, maxBytes=1*1024*1024, backupCount=10)
formatter = logging.Formatter('%(asctime)s [%(levelname)-8s] - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

@timeout_decorator.timeout(60)
def download(url):
    fn = url.split('/')[-1]
    if os.path.exists(CACHE + '/' + fn):
        logger.info('{} already exists!'.format(fn))
        return
    
    req = requests.get(url)

    if req.status_code != 200:
        logger.error('{} something wrong'.format(fn))
        return
        
    with open(CACHE + '/' + fn + '.tmp', 'wb') as f:
        f.write(req.content)
    os.rename(CACHE + '/' + fn + '.tmp', CACHE + '/' + fn)
        
def get_url_list():
    req = requests.get(URL)

    if req.status_code != 200:
        logger.error('Data feed retuened error code: {}'.format(req.status_code))
        return []
    for url in req.text.split('\n'):
        yield url

def main(args):
    if not os.path.exists(CACHE):
        os.makedirs(CACHE)
        
    for url in get_url_list():
        download(url)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GFS Getter')
    
    args = parser.parse_args()
    with open(LOCK, "w") as lockFile:
        try:
            fcntl.flock(lockFile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(args)
        except IOError:
            logger.info('process already exists')
            sys.exit(0)
    
