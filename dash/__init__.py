#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import flask
import dash
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server,external_stylesheets=external_stylesheets)

from flaskexample import views