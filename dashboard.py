import dash
import dash_bootstrap_components as dbc
from dash import html
from dash import dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import time
import sys


app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app = dash.Dash(__name__, )




app.layout = html.Div([
    html.Div([
        html.Div([
            html.Img(src=app.get_asset_url('logo.png'),
                     id='corona-image', style={'height': '200px',
                                               'width': '200px',
                                               'margin-bottom': '10px'})
        ], className='one-third column'),
        html.Div([
            html.Div([
                html.H3('DMS 2.0', style={'margin-bottom': '0px'}),
                html.H5('Data Generation Tool', style={'margin-bottom': '0px'})
            ])
        ], className='one-half column title_color', id='title'),
        html.Div([
            html.H6('Last Ingestion: TEST')

        ], className='one-third column title_color', id='title1')
    ], id='header', className='row flex-display', style={'margin-bottom': '25px'}),
], id='mainContainer', style={'display': 'flex', 'flex-direction': 'column'})

if __name__ == "__main__":
    app.run_server()
