# -*- coding: utf-8 -*-
import digdag
import pathlib
import sys
import datetime

DATA_DIR = pathlib.Path('/home/DATA/incoming/wrf')

def get_latest_date():
    sst_tm = datetime.datetime(year=1, month=1, day=1)
    gfs_tm = datetime.datetime(year=1, month=1, day=1)

    for f in sorted(DATA_DIR.iterdir()):
        if f.name.startswith('sst.'):
            tm = datetime.datetime.strptime(f.name, 'sst.%Y%m%d')
            if sst_tm <= tm:
                sst_tm = tm
        if f.name.startswith('gfs.'):
            tm = datetime.datetime.strptime(f.name[0:15], 'gfs.%Y%m%d_%H')
            if gfs_tm <= tm:
                gfs_tm = tm
    digdag.env.store({
        'sst_tm': sst_tm.strftime('%Y-%m-%d_%H:%M:%S'),
        'gfs_tm': gfs_tm.strftime('%Y-%m-%d_%H:%M:%S'),
    })
