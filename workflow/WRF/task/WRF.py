# -*- coding: utf-8 -*-

import digdag

import datetime
import pathlib
from timeout_decorator import timeout

class WRFTools:
    def __init__(self):
        pass

    @classmethod
    def get_init_time(cls, cur=None):
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


class WRFPreProcess:
    def __init__(self):
        self.DATA_dir = pathlib.Path('/home/weather/outgoing/wrf')

    @timeout(3600)
    def check_files(self):
        init = WRFTools.get_init_time()
        digdag.env.store({'init': init,})
        prefix = init.strftime('gfs.%Y%m%d_%H_')
        for fn in self.DATA_dir.iterdir():
            if not fn.name.stratswith(prefix):
                fn.unlink()


    def remove_gfs(self):
        init = digdag.env.params['init']
        prefix = init.strftime('gfs.%Y%m%d_%H_')
        for fn in self.DATA_dir.iterdir():
            if fn.name.stratswith(prefix):
                fn.unlink()
