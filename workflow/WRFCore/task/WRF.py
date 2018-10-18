# -*- coding utf-8 -*-

import digdag
import pathlib
import datetime
import sys
import subprocess
import os

import jinja2

class WRF():
    def __init__(self):
        self.WRF_dir = pathlib.Path('/home/WRF/WRF/run')
        self.WPS_dir = pathlib.Path('/home/WRF/WPS')
        self.DATA_dir = pathlib.Path('/home/DATA/incoming/wrf')
        self.TEMPLATE_dir = pathlib.Path('/home/weather/etc/WRF/template')
        self.VTABLE = self.WPS_dir / 'Vtable'

        self.CSH = '/bin/csh'


    def cleanup(self):
        self.delete_wps_dir()
        self.delete_wrf_dir()

    def delete_wps_dir(self):
        file_prefix = ('FILE:', 'GRIBFILE', 'met_em.d', 'Vtable', 'SST:')
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

        proj = digdag.env.params['WRF']['map']
        lat = digdag.env.params['WRF']['lat']
        lon = digdag.env.params['WRF']['lon']
        dx = digdag.env.params['WRF']['dx']
        dy = digdag.env.params['WRF']['dy']
        nx = digdag.env.params['WRF']['nx']
        ny = digdag.env.params['WRF']['ny']
        tm = digdag.env.params['sst_tm']

        with (self.WPS_dir / 'namelist.wps').open('w') as f:
            f.write(tmpl.render(tm=tm, lat=lat, lon=lon, dx=dx, dy=dy, nx=nx, ny=ny, map=proj))

    def fillin_gfs_template(self):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(self.TEMPLATE_dir), encoding='utf8'))
        tmpl = env.get_template('namelist.wps.gfs')

        proj = digdag.env.params['WRF']['map']
        lat = digdag.env.params['WRF']['lat']
        lon = digdag.env.params['WRF']['lon']
        dx = digdag.env.params['WRF']['dx']
        dy = digdag.env.params['WRF']['dy']
        nx = digdag.env.params['WRF']['nx']
        ny = digdag.env.params['WRF']['ny']
        sst_tm = digdag.env.params['sst_tm']
        start_tm = digdag.env.params['gfs_tm']
        tm = datetime.datetime.strptime(start_tm, '%Y-%m-%d_%H:%M:%S')
        if tm.hour == 12:
            end_tm = (tm + datetime.timedelta(hours=16*24)).strftime('%Y-%m-%d_%H:%M:%S')
            interval = 6 * 3600
            run_hour = 16 * 24
        else:
            end_tm = (tm + datetime.timedelta(hours=5*24)).strftime('%Y-%m-%d_%H:%M:%S')
            interval = 12 * 3600
            run_hour = 5 * 24

        with (self.WPS_dir / 'namelist.wps').open('w') as f:
            f.write(tmpl.render(start_tm=start_tm, end_tm=end_tm, sst_tm=sst_tm, lat=lat, lon=lon, dx=dx, dy=dy, nx=nx, ny=ny, map=proj, run_hour=run_hour, interval=interval))

    def fillin_wrf_template(self):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(self.TEMPLATE_dir), encoding='utf8'))
        tmpl = env.get_template('namelist.input')

        dx = digdag.env.params['WRF']['dx']
        dy = digdag.env.params['WRF']['dy']
        nx = digdag.env.params['WRF']['nx']
        ny = digdag.env.params['WRF']['ny']
        tm = digdag.env.params['gfs_tm']
        start_tm = datetime.datetime.strptime(start_tm, '%Y-%m-%d_%H:%M:%S')
        step = 90
        if tm.hour == 12:
            end_tm = (tm + datetime.timedelta(hours=16*24))
            interval = 6 * 3600
            run_hour = 16 * 24
        else:
            end_tm = (tm + datetime.timedelta(hours=5*24))
            interval = 12 * 3600
            run_hour = 5 * 24
        tm_s_year = start_tm.year
        tm_s_month = start_tm.month
        tm_s_day = start_tm.day
        tm_s_hour = start_tm.hour
        tm_e_year = end_tm.year
        tm_e_month = end_tm.month
        tm_e_day = end_tm.day
        tm_e_hour = end_tm.hour



        with (self.WRF_dir / 'namelist.wps').open('w') as f:
            f.write(tmpl.render(run_hour=run_hour,
                                tm_s_year=tm_s_year, tm_s_month=tm_s_month, tm_s_day=tm_s_day, tm_s_hour=tm_s_hour,
                                tm_e_year=tm_e_year, tm_e_month=tm_e_month, tm_e_day=tm_e_day, tm_e_hour=tm_e_hour,
                                interval=interval, step=step, nx=nx, ny=ny, dx=dx, dy=dy))

    def preprocess_sst(self):
        cwd = os.getcwd()
        os.chdir(self.WPS_dir)
        tm = datetime.datetime.strptime(digdag.env.params['sst_tm'], '%Y-%m-%d_%H:%M:%S')
        subprocess.run([self.CSH, str(self.WPS_dir / 'link_grib.csh'), str(self.DATA_dir / tm.strftime('sst.%Y%m%d'))], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
        if self.VTABLE.exists() is True:
            self.VTABLE.unlink()
        self.VTABLE.symlink_to(self.WPS_dir / 'ungrib' / 'Variable_Tables' / 'Vtable.SST')
        subprocess.run([str(self.WPS_dir / 'ungrib.exe'),], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, check=True)
        os.chdir(cwd)

    def preprocess_gfs(self):
        cwd = os.getcwd()
        os.chdir(self.WPS_dir)
        tm = datetime.datetime.strptime(digdag.env.params['gfs_tm'], '%Y-%m-%d_%H:%M:%S')
        subprocess.run([self.CSH, str(self.WPS_dir / 'link_grib.csh'), str(self.DATA_dir / tm.strftime('gfs.%Y%m%d_%H'))], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
        if self.VTABLE.exists() is True:
            self.VTABLE.unlink()
        self.VTABLE.symlink_to(self.WPS_dir / 'ungrib' / 'Variable_Tables' / 'Vtable.GFS')
        subprocess.run([str(self.WPS_dir / 'ungrib.exe'),], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, check=True)
        os.chdir(cwd)

    def process(self):
        cwd = os.getcwd()
        os.chdir(self.WPS_dir)
        subprocess.run([str(self.WPS_dir / 'metgrid.exe'),], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, check=True)
        os.chdir(cwd)
