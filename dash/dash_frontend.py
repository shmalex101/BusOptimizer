#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 19:06:28 2019

@author: monster
"""

import sqlalchemy
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from sqlalchemy.dialects import postgresql
import psycopg2

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

df = pd.read_sql_query('SELECT * FROM postgres LIMIT 10',con)
app = dash.Dash(__name__)

app.layout = html.Div([
        dcc.Dropdown(id='dropdown', options=[
            {'label': i, 'value': i} for i in df.inferred_route_id.values.tolist()
            ], multi=False, placeholder='Route selection...'),
    
        dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in df.columns])
       ]) 


@app.callback(
    dash.dependencies.Output(component_id='table', component_property='data'),
    [dash.dependencies.Input(component_id='dropdown', component_property='value')]
)
def update_output_div(dropdown_value):
    con = connect()
    if dropdown_value is None:
        return df.to_dict('records')
    
    dff = pd.read_sql_query('SELECT * \
                            FROM postgres\
                            WHERE inferred_route_id = \''+str(dropdown_value)+ \
                            '\' LIMIT 50',con)
    return dff.to_dict('records')

if __name__ == '__main__':
    app.run_server(debug=True)
