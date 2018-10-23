#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__DESC__ = 'Process WRF'

import argparse
import pathlib
import subprocess

def main(args):
    fp = pathlib.Path(args.FILE)
    if fp.name == 'done':
        r = subprocess.run(['/bin/sh', '-c', '/usr/local/bin/digdag start WRF post --session now'], check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__DESC__)
    parser.add_argument('FILE', type=str, help='File Path')
    args = parser.parse_args()

    main(args)
