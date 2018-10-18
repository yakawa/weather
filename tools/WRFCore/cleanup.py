#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__DESC__ = 'WRF Cleanupper'

import argparse
import pathlib

WRF_dir = pathlib.Path('/home/WRF/WRF')
WPS_dir = pathlib.Path('/home/WRF/WPS')

def delete_wrf_dir():
    file_prefix = ('met_em.d', 'rsl.', 'wrfinput', 'wrfbdy', 'wrfrst_', 'wrfout_')
    for f in (WRF_dir / 'run').iterdir():

        if f.name.startswith(file_prefix):
            f.unlink()
            continue


def delete_wps_dir():
    file_prefix = ('FILE:', 'GRIBFILE', 'met_em.d', 'Vtable')
    for f in WPS_dir.iterdir():

        if f.name.startswith(file_prefix):
            f.unlink()
            continue


def main(args):
    delete_wrf_dir()
    delete_wps_dir()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    args = parser.parse_args()

    main(args)
