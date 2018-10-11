# -*- coding: utf-8 -*-

bind = 'unix:/home/weather/webApp/www.hiyorimi.jp/tmp/socket.sock'
backlog = 2048

workers = 1
worker_class = 'sync'
worker_connections = 1000
max_request = 5000
timeout = 60
keepalive = 2

debug = False
spew = False

preload_app = True
dameon = False
pidfile = '/home/weather/webApp/www.hiyorimi.jp/tmp/webApp.pid'
user = 'weather'
group = 'weather'

logfile = '/home/weather/webApp/www.hiyorimi.jp/log/webApp.log'
loglevel = 'info'
logconf = None

proc_name = 'www'
