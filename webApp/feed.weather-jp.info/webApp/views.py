# -*- coding: utf-8 -*-

import datetime
import os

from  webApp import app

from flask import request


@app.route('/subscribe', methods=['GET', 'POST'])
def subscribe():
  if request.method == 'GET':
    msg = request.args.get('hub.challenge')
  else:
    tm = datetime.datetime.utcnow()
    fn = '/home/weather/cache/JMAXML/{}.{:03d}.rss'.format(tm.strftime('%Y%m%d%H%M%S'), tm.microsecond // 1000)
    with open(fn + '.tmp', 'wb') as f:
      f.write(request.data)
    os.rename(fn + '.tmp', fn)
    return "OK"
  return msg, 202

@app.route('/')
def index():
  return "OK"
