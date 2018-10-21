#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import pathlib
import datetime
import shutil

__DESC__ = 'process GFS'

DST = pathlib.Path('/home/DATA/outgoing/wrf/')

FILE_BASE = 'gfs.{yr:04d}{mo:02d}{dy:02d}_{hr:02d}_{ft:03d}'


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


def make_fp_que(init):
    ret = []
    if init.hour == 12:
        for ft in range(0, 396, 12):
            ret.append(FILE_BASE.format(yr=init.year, mo=init.month, dy=init.day, hr=init.hour, ft=ft))
    else:
        for ft in range(0, 123, 3):
            ret.append(FILE_BASE.format(yr=init.year, mo=init.month, dy=init.day, hr=init.hour, ft=ft))
    return ret


def main(args):
    fp = pathlib.Path(args.FILE)
    if args.init is None:
        init = get_init()
    else:
        init = datetime.datetime.strptime(args.init, '%Y%m%d%H')

    fq = make_fp_que(init)

    if DST.exists() is False:
        DST.mkdir(parent=True)

    if fp.name in fq:
        shutil.copy(str(fp), str(DST / fp.name))

    with (DST / (fp.name + '.done')).open('w') as f:
        f.write("")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    parser.add_argument('FILE', type=str, help='File Path')
    parser.add_argument('-i', '--init', metavar='INIT', type=str, help='Initial Time')
    args = parser.parse_args()

    main(args)
