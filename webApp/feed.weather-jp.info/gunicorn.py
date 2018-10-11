#!/bin/env python3
# -*- coding: utf-8 -*-

bind = 'unix:/home/weather/webApp/feed.weather-jp.info/tmp/JMA_XML.sock'
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
pidfile = '/home/weather/webApp/feed.weather-jp.info/tmp/JMA_XML.pid'
user = 'weather'
group = 'weather'

logfile = '/home/weather/webApp/feed.weather-jp.info/tmp/gunicorn.log'
loglevel = 'info'
logconf = None

proc_name = 'JMA_XML'
