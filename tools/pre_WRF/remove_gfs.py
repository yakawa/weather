#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse
import pathlib
import datetime
import time
import sys

__DESC__ = 'GFS Remover for sync directory'

GFS_BASE_DIR = pathlib.Path('/home/DATA/outgoing/wrf/')
GFS_BASE_FILE = 'gfs.{year:04d}{month:02d}{day:02d}_{hour:02d}_{ft:03d}'


def get_gfs_init(cur):
    if cur is None:
        now = datetime.datetime.utcnow()
    else:
        now = datetime.datetime.strptime(cur, '%Y%m%d%H%M')

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

def make_gfs_s_que(init):
    q = []
    for ft in range(0, 123, 3):
        q.append(GFS_BASE_FILE.format(year=init.year, month=init.month, day=init.day, hour=init.hour, ft=ft))
    return q

def make_gfs_l_que(init):
    q = []
    for ft in range(0, 396, 12):
        q.append(GFS_BASE_FILE.format(year=init.year, month=init.month, day=init.day, hour=init.hour, ft=ft))
    return q


def main(args):
    if args.init is None:
        if args.TYPE.startswith('GFS'):
            init = get_gfs_init(args.current_time)
        else:
            init = None
    else:
        init = datetime.datetime.strptime(args.init, '%Y%m%d%H')

    if args.TYPE == 'GFS_s':
        fq = make_gfs_s_que(init)
    elif args.TYPE == 'GFS_l':
        fq = make_gfs_l_que(init)

    for fn in fq:
        f = (GFS_BASE_DIR / fn)
        if f.exists() is True:
            f.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    parser.add_argument('-i', '--init', metavar='INIT', type=str, help='Initial Time')
    parser.add_argument('-c', '--current-time', metavar='TIME', type=str, help='Run Time')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('TYPE', type=str, choices=['GFS_l', 'GFS_s'])
    args = parser.parse_args()

    main(args)
