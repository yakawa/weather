# -*- coding: utf-8 -*-

import flask

app = flask.Flask(__name__, static_url_path='/static')

app.secret_key = '7719ecb16ffb81593a212741574fee16'

import webApp.views
import webApp.controllers
import webApp.models
