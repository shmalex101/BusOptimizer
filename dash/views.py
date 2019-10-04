#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlalchemy
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from sqlalchemy.dialects import postgresql
import numpy as np
import plotly.graph_objs as go

def connect():
    #Returns a connection and a metadata object
    # We connect with the help of the PostgreSQL URL
    user = 'alex'
    password = 'password'
    host = 'ec2-100-20-48-52.us-west-2.compute.amazonaws.com'
    db = 'postgres'
    port = 5432
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)
    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')
    # We then bind the connection to MetaData()
    #meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con

con = connect()
# load transformed data from postgres
df = pd.read_sql_query('SELECT * FROM tripstat2 LIMIT 316',con).sort_values(by=['route_id'])
#make dropdown items unique
routeid = df.route_id.unique()
#define app layout
app.layout = html.Div([
                dcc.Dropdown(id='dropdown', options=[
                    {'label': i, 'value': i} for i in routeid.tolist()
                    ], multi=False, placeholder='Route selection...',
    value='MTA NYCT_Q83'),
        
                html.Div([
                    dcc.Graph(
                        id='example-graph',
                        ),
                dash_table.DataTable(
                    id='table',
                    columns=[{"name": i, "id": i} for i in df.columns],
                    style_table={'overflowX': 'scroll'},
                    style_cell={
                                'height': 'auto',
                                'minWidth': '0px', 'maxWidth': '10px',
                                'whiteSpace': 'normal'
    })
   
               ])
                ],style = {'display': 'inline-block', 'width': '50%'})

#define callback for updating figure from dropdown
@app.callback(
    dash.dependencies.Output(component_id='example-graph', component_property='figure'),
    [dash.dependencies.Input(component_id='dropdown', component_property='value')]
)
def update_figure(dropdown_value):
    con = connect()
    if dropdown_value is None:
        dropdown_value = '\'MTA NYCT_Q83\''     
    else:
        dff = pd.read_sql_query('SELECT * \
                                FROM tripstat2\
                                WHERE route_id = \''+str(dropdown_value)+ \
                                '\'',con).sort_values(by=['hour']) 
    x = dff.hour.to_list()   
    y= dff.avg_dur.to_list()
    dff = dff.to_dict('records')
    return {
            'data': [{'x':x, \
                      'y':y, \
                      'type': 'bar', \
                      'name': 'Avg'}],
            'layout':go.Layout(xaxis={'title':'start time (hour)','range': [-0.5, 23.5]},
                               yaxis={'title':'trip duration (min)'},
                               title='Average Trip Duration')
            }

#define callback for updating table from dropdown
@app.callback(
    dash.dependencies.Output(component_id='table', component_property='data'),
    [dash.dependencies.Input(component_id='dropdown', component_property='value')]
)
def update_output_div(dropdown_value):
    con = connect()
    if dropdown_value is None:
        return df.to_dict('records')
    
    dff = pd.read_sql_query('SELECT * \
                            FROM tripstat2\
                            WHERE route_id = \''+str(dropdown_value)+ \
                            '\' LIMIT 50',con).round(1).sort_values(by=['hour']) 
    
    return dff.to_dict('records')

