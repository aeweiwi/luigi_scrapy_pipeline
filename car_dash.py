import dash
import dash_core_components as dcc
import dash_html_components as html
import sqlite3
import pandas as pd
from dash.dependencies import Input, Output, State
import plotly.plotly as py
import plotly.graph_objs as go


cnx = sqlite3.connect('mydb.db')
df = pd.read_sql_query("SELECT * FROM cars", cnx)
df['car_year'] = pd.to_numeric(df['car_year'], errors='coerce').astype(int)
df['price'] = pd.to_numeric(df['price'], errors='coerce').astype(float)

functions = ['price', 'count']

app = dash.Dash()
app.layout = html.Div(children=
    [
        html.Div([
            html.Div([html.H1('Explore Dashboard')], className="col-md-8")
        ], id='div_header', className='row justify-content-center'),
        html.Div([
            html.Div([
                html.Div([
                    dcc.Dropdown(
                        options=[
                            {'label': col, 'value': col}
                            for col in df.columns
                        ],
                        value=[],
                        id = 'sel_grpby',
                        multi=True)
                ], id='div_grpby_input'),
                html.Div([
                    dcc.RadioItems(
                        options=[
                            {'label': fname, 'value': fname}
                            for fname in functions
                        ], value='count', id='rd_func')
                ], id='div_func_input')
            ], id='div_input', className= "col-md-4")

        ],  className='row justify-content-center'),

        html.Div([
            html.Div([
                dcc.Graph(
                    id='grph_output'
                )
            ], id='div_output', className= "col-md-12")
        ], className='row')
    ],
    style={"class": "row"}
)

external_css = ["https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css",
                "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css", ]

for css in external_css:
    app.css.append_css({"external_url": css})

    external_js = ["http://code.jquery.com/jquery-3.3.1.min.js",
               "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js"]

for js in external_js:
    app.scripts.append_script({"external_url": js})

def make_col_name(col):
    if isinstance(col, tuple):
        return '.'.join(col)
    elif isinstance(col, str):
        return col
    return '.'.join(col)


def get_output_figure(stat_df):


    data = [
        go.Scatter(x=stat_df.index, y=stat_df[col],
                   name=make_col_name(col),  mode='lines+markers',
                   marker={
                       'size': 15,
                       'opacity': 0.5,
                       'line': {'width': 0.5, 'color': 'white'}
                   })
        for col in stat_df.columns]
    layout = dict(
        title = "Manually Set Date Range",
        xaxis = dict(
            range = ['1950-01-01','2019-01-01'])
    )

    retval = {
        'data': data,
        'layout': layout
    }
    print ('0000000000000000')
    print (retval)
    print ('0000000000000000')
    return retval


@app.callback(
    Output('grph_output', 'figure'),
    [Input('sel_grpby', 'value'),
     Input('rd_func', 'value')])
def update_output(sel_grpby, rd_func):
    print ('---------')
    print (sel_grpby)
    print ('---------')
    if 'car_year' not in sel_grpby:
        sel_grpby = ['car_year'] + sel_grpby
    print ('------------------')
    print ('sel_grpby ', sel_grpby)
    print ('------------------')
    if len(sel_grpby) > 1:
        if rd_func == 'count':
            stat_df = df.groupby(sel_grpby)['car_model'].count().unstack('car_year').T

        else:
            stat_df = df.groupby(sel_grpby)['price'].mean().unstack('car_year').T
        output_figure = get_output_figure(stat_df)
        return output_figure
    else:
        return 'by default year is included in group by crietera, please insert another one!'








if __name__ == "__main__":
    app.run_server(debug=False)
