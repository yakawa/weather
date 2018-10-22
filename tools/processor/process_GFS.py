#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import pathlib
import datetime
import shutil

__DESC__ = 'process GFS'

DST_short = pathlib.Path('/home/DATA/outgoing/wrf_short/')
DST_week = pathlib.Path('/home/DATA/outgoing/wrf_week/')

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


def make_fp_que_short(init):
    ret = []
    for ft in range(0, 123, 3):
        ret.append(FILE_BASE.format(yr=init.year, mo=init.month, dy=init.day, hr=init.hour, ft=ft))
    return ret

def make_fp_que_week(init):
    ret = []
    if init.hour == 12 or init.hour == 0:
        for ft in range(0, 396, 12):
            ret.append(FILE_BASE.format(yr=init.year, mo=init.month, dy=init.day, hr=init.hour, ft=ft))
    return ret


def main(args):
    fp = pathlib.Path(args.FILE)
    if args.init is None:
        init = get_init()
    else:
        init = datetime.datetime.strptime(args.init, '%Y%m%d%H')

    fq_short = make_fp_que_short(init)
    fq_week = make_fp_que_week(init)

    if DST_short.exists() is False:
        DST_short.mkdir()

    if DST_week.exists() is False:
        DST_week.mkdir()


    if fp.name in fq_short:
        shutil.copy(str(fp), str(DST_short / fp.name))
        with (DST_short / (fp.name + '.done')).open('w') as f:
            f.write("")

    if fp.name in fq_week:
        shutil.copy(str(fp), str(DST_week / fp.name))
        with (DST_week / (fp.name + '.done')).open('w') as f:
            f.write("")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    parser.add_argument('FILE', type=str, help='File Path')
    parser.add_argument('-i', '--init', metavar='INIT', type=str, help='Initial Time')
    args = parser.parse_args()

    main(args)
