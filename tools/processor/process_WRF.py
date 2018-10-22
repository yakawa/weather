#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__DESC__ = 'Process WRF'

import argparse
import pathlib
import subprocess

def main(args):
    fp = pathlib.Path(args.FILE)
    if fp.name == 'done':
        subprocess.run(['/usr/local/bin/digdag', 'start', 'WRF', 'wrf', '--session', 'now'], check=False, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    parser.add_argument('FILE', type=str, help='File Path')
    args = parser.parse_args()

    main(args)
