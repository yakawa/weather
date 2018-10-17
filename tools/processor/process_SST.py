#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import pathlib
import datetime
import shutil

__DESC__ = 'process SST'

DST = pathlib.Path('/home/DATA/outgoing/wrf')

def get_latest_sst():
    dt = None
    for f in DST.iterdir():
        if f.name.startswith('sst.'):
            dt = datetime.datetime.strptime(f.name, 'sst.%Y%m%d')
    return dt

def main(args):
    fp = pathlib.Path(args.FILE)
    init = datetime.datetime.strptime(fp.name, 'sst.%Y%m%d')
    cur = get_latest_sst()
    if cur is not None:
        if init <= cur:
            return
        (DST / cur.strftime('sst.%Y%m%d')).unlink()
    shutil.copy(str(fp), str(DST / fp.name))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    parser.add_argument('FILE', type=str, help='File Path')

    args = parser.parse_args()

    main(args)
