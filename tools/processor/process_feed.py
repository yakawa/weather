#!/usr/bin/env python3

import sys
import subprocess

HOST_dst = 'weather-process.hiyorimi.jp'
KEY = '/home/weather/.ssh/id_rsa'
USER = 'weather'
SCP = '/usr/bin/scp'

def scp(src, dst):
    subprocess.check_call([SCP, '-i', KEY, '{src}'.format(src=src), '{user}@{host}:{dst}'.format(user=USER, host=HOST_dst, dst=src)], shell=False)

def main():
    fn = sys.argv[-1]
    scp(fn, fn)

if __name__ == '__main__':
    main()
