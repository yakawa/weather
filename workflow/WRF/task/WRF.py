# -*- coding: utf-8 -*-

import digdag

import datetime
import pathlib
import sys
import subprocess
import os
import time
import logging
import socket
import multiprocessing

import jinja2

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

    @classmethod
    def set_hostname(cls):
        host = socket.gethostname().split('.')[0]
        digdag.env.store({
            'hostname' : host,
            })

    @classmethod
    def set_run_time(cls):
        tm = datetime.datetime.strptime(digdag.env.params['session_time'], '%Y-%m-%dT%H:%M:%S+00:00')
        init = WRFTools.get_init_time(tm.strftime('%Y%m%d%H%M'))
        digdag.env.store({
            'run': {
                'year': init.year,
                'month': init.month,
                'day': init.day,
                'hour': init.hour,
                }
            })

class WRFBase():
    def __init__(self):
        self.WRF_dir = pathlib.Path('/home/WRF/WRF/run')
        self.WPS_dir = pathlib.Path('/home/WRF/WPS')


class WRFPreProcess(WRFBase):
    def __init__(self):
        super().__init__()

        self.DATA_short_dir = pathlib.Path('/home/DATA/outgoing/wrf_short')
        self.DATA_week_dir = pathlib.Path('/home/DATA/outgoing/wrf_week')

    def _make_fp_que_short(self, init):
        q = []
        for ft in range(0, 123, 3):
            q.append('gfs.{}_{:03d}.done'.format(init.strftime('%Y%m%d_%H'), ft))
        return q

    def _make_fp_que_week(self, init):
        q = []
        if init.hour == 12 or init.hour == 0:
            for ft in range(0, 396, 12):
                q.append('gfs.{}_{:03d}.done'.format(init.strftime('%Y%m%d_%H'), ft))
        return q

    def check_files_short(self):
        tm = datetime.datetime.strptime(digdag.env.params['session_time'], '%Y-%m-%dT%H:%M:%S+00:00')
        init = WRFTools.get_init_time(tm.strftime('%Y%m%d%H%M'))
        digdag.env.store({'init': init.strftime('gfs.%Y%m%d_%H_'),})

        fq = self._make_fp_que_short(init)

        for fn in fq:
            print("Checking {}".format(self.DATA_short_dir / fn))
            while True:
                if (self.DATA_short_dir / fn).exists() is True:
                    break
                time.sleep(5)

        prefix = init.strftime('gfs.%Y%m%d_%H_')
        for fn in self.DATA_short_dir.iterdir():
            if ((not fn.name.startswith(prefix)) and fn.name.startswith('gfs')) or fn.name.endswith('.done'):
                fn.unlink()

    def check_files_week(self):
        tm = datetime.datetime.strptime(digdag.env.params['session_time'], '%Y-%m-%dT%H:%M:%S+00:00')
        init = WRFTools.get_init_time(tm.strftime('%Y%m%d%H%M'))
        digdag.env.store({'init': init.strftime('gfs.%Y%m%d_%H_'),})

        fq = self._make_fp_que_week(init)

        for fn in fq:
            print("Checking {}".format(self.DATA_week_dir / fn))
            while True:
                if (self.DATA_week_dir / fn).exists() is True:
                    break
                time.sleep(5)

        prefix = init.strftime('gfs.%Y%m%d_%H_')
        for fn in self.DATA_week_dir.iterdir():
            if ((not fn.name.startswith(prefix)) and fn.name.startswith('gfs')) or fn.name.endswith('.done'):
                fn.unlink()

    def remove_gfs_short(self):
        prefix = digdag.env.params['init']
        for fn in self.DATA_short_dir.iterdir():
            if fn.name.startswith(prefix):
                fn.unlink()
        for fn in self.DATA_week_dir.iterdir():
            if fn.name.startswith(prefix):
                fn.unlink()

    def remove_gfs_week(self):
        prefix = digdag.env.params['init']
        for fn in self.DATA_week_dir.iterdir():
            if fn.name.startswith(prefix):
                fn.unlink()

class WRF(WRFBase):
    def __init__(self):
        super().__init__()
        self.DATA_gfs_dir = pathlib.Path('/home/DATA/incoming/wrf')
        self.DATA_sst_dir = pathlib.Path('/home/DATA/incoming/sst')

        self.TEMPLATE_dir = pathlib.Path('/home/weather/etc/WRF/template')
        self.VTABLE = self.WPS_dir / 'Vtable'

        self.CSH = '/bin/csh'
        self.MPIRUN = '/usr/bin/mpirun'

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

        for f in self.DATA_gfs_dir.iterdir():
            if f.name.startswith('gfs.'):
                tm = datetime.datetime.strptime(f.name[0:15], 'gfs.%Y%m%d_%H')
                if gfs_tm <= tm:
                    gfs_tm = tm
        for f in self.DATA_sst_dir.iterdir():
            if f.name.startswith('sst.'):
                tm = datetime.datetime.strptime(f.name, 'sst.%Y%m%d')
                if sst_tm <= tm:
                    sst_tm = tm

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
        sst_tm = datetime.datetime.strptime(digdag.env.params['sst_tm'], '%Y-%m-%d_%H:%M:%S').strftime('%Y-%m-%d_%H')
        start_tm = digdag.env.params['gfs_tm']
        tm = datetime.datetime.strptime(start_tm, '%Y-%m-%d_%H:%M:%S')
        hostname = digdag.env.params['hostname']
        if hostname == 'wrf002':
            end_tm = (tm + datetime.timedelta(hours=16*24)).strftime('%Y-%m-%d_%H:%M:%S')
            interval = 12 * 3600
            run_hour = 16 * 24
        else:
            end_tm = (tm + datetime.timedelta(hours=5*24)).strftime('%Y-%m-%d_%H:%M:%S')
            interval = 3 * 3600
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
        start_tm = datetime.datetime.strptime(tm, '%Y-%m-%d_%H:%M:%S')
        hostname = digdag.env.params['hostname']
        step = 90
        if hostname == 'wrf002':
            end_tm = (start_tm + datetime.timedelta(hours=16*24))
            interval = 12 * 3600
            run_hour = 16 * 24
        else:
            end_tm = (start_tm + datetime.timedelta(hours=5*24))
            interval = 3 * 3600
            run_hour = 5 * 24
        tm_s_year = start_tm.year
        tm_s_month = start_tm.month
        tm_s_day = start_tm.day
        tm_s_hour = start_tm.hour
        tm_e_year = end_tm.year
        tm_e_month = end_tm.month
        tm_e_day = end_tm.day
        tm_e_hour = end_tm.hour



        with (self.WRF_dir / 'namelist.input').open('w') as f:
            f.write(tmpl.render(run_hour=run_hour,
                                tm_s_year=tm_s_year, tm_s_month=tm_s_month, tm_s_day=tm_s_day, tm_s_hour=tm_s_hour,
                                tm_e_year=tm_e_year, tm_e_month=tm_e_month, tm_e_day=tm_e_day, tm_e_hour=tm_e_hour,
                                interval=interval, step=step, nx=nx, ny=ny, dx=dx, dy=dy))

    def preprocess_sst(self):
        cwd = os.getcwd()
        os.chdir(self.WPS_dir)
        tm = datetime.datetime.strptime(digdag.env.params['sst_tm'], '%Y-%m-%d_%H:%M:%S')
        subprocess.run([self.CSH, str(self.WPS_dir / 'link_grib.csh'), str(self.DATA_sst_dir / tm.strftime('sst.%Y%m%d'))], check=True)
        if self.VTABLE.exists() is True:
            self.VTABLE.unlink()
        self.VTABLE.symlink_to(self.WPS_dir / 'ungrib' / 'Variable_Tables' / 'Vtable.SST')
        subprocess.run([str(self.WPS_dir / 'ungrib.exe'),], check=True)
        os.chdir(cwd)

    def preprocess_gfs(self):
        cwd = os.getcwd()
        os.chdir(self.WPS_dir)
        tm = datetime.datetime.strptime(digdag.env.params['gfs_tm'], '%Y-%m-%d_%H:%M:%S')
        subprocess.run([self.CSH, str(self.WPS_dir / 'link_grib.csh'), str(self.DATA_gfs_dir / tm.strftime('gfs.%Y%m%d_%H'))], check=True)
        if self.VTABLE.exists() is True:
            self.VTABLE.unlink()
        self.VTABLE.symlink_to(self.WPS_dir / 'ungrib' / 'Variable_Tables' / 'Vtable.GFS')
        subprocess.run([str(self.WPS_dir / 'ungrib.exe'),], check=True)
        os.chdir(cwd)

    def preprocess_wrf(self):
        cwd = os.getcwd()
        os.chdir(self.WPS_dir)
        subprocess.run([str(self.WPS_dir / 'metgrid.exe'),], check=True)

        for fn in self.WPS_dir.iterdir():
            if fn.name.startswith('met_em.'):
                if (self.WRF_dir / fn.name).exists() is True:
                    (self.WRF_dir / fn.name).unlink()
                (self.WRF_dir / fn.name).symlink_to(fn)

        os.chdir(self.WRF_dir)

        subprocess.run([self.MPIRUN, '-np', '1', str(self.WRF_dir / 'real.exe'),], check=True)

        os.chdir(cwd)


    def run_wrf(self):
        cwd = os.getcwd()
        os.chdir(self.WRF_dir)
        os.environ['OMP_NUM_THREADS'] = str(multiprocessing.cpu_count())
        os.environ['WRF_EM_CORE'] = '1'
        os.environ['WRF_NMM_CORE'] = '0'
        os.environ['WRF_DA_CORE'] = '0'
        os.environ['MP_STACK_SIZE'] = '64000000'

        subprocess.run([self.MPIRUN, str(self.WRF_dir / 'wrf.exe'),], check=True)

        os.chdir(cwd)
