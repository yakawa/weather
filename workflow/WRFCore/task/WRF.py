# -*- coding utf-8 -*-

import digdag
import pathlib
import datetime
import sys

import jinja2

class WRF():
    def __init__(self):
        self.WRF_dir = pathlib.Path('/home/WRF/WRF/run')
        self.WPS_dir = pathlib.Path('/home/WRF/WPS')
        self.DATA_dir = pathlib.Path('/home/DATA/incoming/wrf')
        self.TEMPLATE_dir = pathlib.Path('/home/weather/etc/WRF/template')

    def cleanup(self):
        self.delete_wps_dir()
        self.delete_wrf_dir()

    def delete_wps_dir(self):
        file_prefix = ('FILE:', 'GRIBFILE', 'met_em.d', 'Vtable')
        for f in self.WPS_dir.iterdir():

            if f.name.startswith(file_prefix):
                f.unlink()
                continue

    def delete_wrf_dir(self):
        file_prefix = ('met_em.d', 'rsl.', 'wrfinput', 'wrfbdy', 'wrfrst_', 'wrfout_')
        for f in self.WRF_dir.iterdir():

            if f.name.startswith(file_prefix):
                f.unlink()
                continue

    def get_latest_date(self):
        sst_tm = datetime.datetime(year=1, month=1, day=1)
        gfs_tm = datetime.datetime(year=1, month=1, day=1)

        for f in self.DATA_dir.iterdir():
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

    def fillin_sst_template(self):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(self.TEMPLATE_dir), encoding='utf8'))
        tmpl = env.get_template('namelist.wps.sst')

        lat = digdag.env.params['WRF']['lat']
        lon = digdag.env.params['WRF']['lon']
        dx = digdag.env.params['WRF']['dx']
        dy = digdag.env.params['WRF']['dy']
        nx = digdag.env.params['WRF']['nx']
        ny = digdag.env.params['WRF']['ny']
        tm = digdag.env.params['sst_tm']

        with (self.WPS_dir / 'namelist.wps').open('w') as f:
            f.write(tmpl.render(tm=tm, lat=lat, lon=lon, dx=dx, dy=dy, nx=nx, ny=ny))
