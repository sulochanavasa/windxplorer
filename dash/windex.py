# Import required libraries
import pickle
import copy
import pathlib
import dash
import math
import datetime as dt
import pandas as pd
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html

from db import get_site_avg_yearly, get_site_avg_monthly, get_site_avg_daily, get_site_info_db, get_site_total_daily_capacity, get_partial_sites, get_site_total_monthly_capacity, get_site_total_yearly_capacity

# Multi-dropdown options
from constants import STATES, SITE_FILTER_OPTIONS

# get relative data folder
PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("data").resolve()

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server

state_options = [
    {"label": str(STATES[state]), "value": str(state)} for state in STATES
]

site_filter_options = [
    {"label": str(SITE_FILTER_OPTIONS[option]), "value": str(option)} for option in SITE_FILTER_OPTIONS
]

# Load site info data
df = pd.DataFrame(get_site_info_db())

# Get the partial site_id's we have
#site_list = list(set(get_partial_sites()))
#df = df[df['site_id'].isin(site_list)]

# Pick the columns we want and make 'site_id' the index
df = df[['site_id', 'site_score', 'state', 'fraction_of_usable_area', 'lat', 'lon']]
df.set_index('site_id', inplace=True)

default_site_id =list(df.index.values)[0]

# Create global chart template
#mapbox_access_token = "pk.eyJ1Ijoic3Vsb2NoYW5hdmFzYSIsImEiOiJjazZpMDExdzMyempsM2pvYmt1dGl6d3NlIn0.jTaza9CXzAYEA9R_r8pjuQ"
mapbox_access_token = "pk.eyJ1IjoiamFja2x1byIsImEiOiJjajNlcnh3MzEwMHZtMzNueGw3NWw5ZXF5In0.fk8k06T96Ml9CLGgKmk81w"

layout = dict(
    autosize=True,
    automargin=True,
    margin=dict(l=30, r=30, b=20, t=40),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
    title="WindExplorer Map View",
    mapbox=dict(
        accesstoken=mapbox_access_token,
        style="light",
        center=dict(lon=-100, lat=41),
        zoom=3,
    ),
)

# Create app layout
app.layout = html.Div(
    [
        # empty Div to trigger javascript file for graph resizing
        html.Div(id="output-clientside"),
        html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3(
                                    "WIND EXPLORER",
                                    style={"margin-bottom": "0px"},
                                ),
                                html.H5(
                                    "By Sulochana Vasa", style={"margin-top": "0px"}
                                ),
                            ]
                        )
                    ],
                    className="container",
                    id="title",
                ),
            ],
            id="header",
            className="row flex-display",
            style={"margin-bottom": "25px"},
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="map_graph", style={"height": "100%"})],
                    className="eleven columns",
                ),
                html.Div(
                    [
                        html.Div(
                            [html.H6(id="site_id_text"), html.P("Site ID")],
                            id="site_id",
                            className="mini_container",
                        ),
                        html.Div(
                            [html.H6(id="site_score_text"), html.P("Site Score")],
                            id="site_score",
                            className="mini_container",
                        ),
                        html.Div(
                            [html.H6(id="state_text"), html.P("State")],
                            id="state",
                            className="mini_container",
                        ),
                        html.Div(
                            [html.H6(id="usable_area_text"), html.P("Usable Area")],
                            id="usable_area",
                            className="mini_container",
                        ),
                    ],
                    id="info-container",
                    className="two colums container-display",
                    style={"display": "flex", "flex-direction": "column"},
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.P("Filter by State", className="control_label"),
                        dcc.Dropdown(
                            id="state_dropdown",
                            options=state_options,
                            value="all",
                            className="dcc_control",
                        ),
                    ],
                    className="pretty_container six columns",
                    id="state-filter-options",
                ),
                html.Div(
                    [
                        html.P("Filter by Feature", className="control_label"),
                        dcc.Dropdown(
                            id="feature_drop_down",
                            options=site_filter_options,
                            value="power",
                            className="dcc_control",
                        ),
                    ],
                    className="pretty_container six columns",
                    id="feature-filter-options",
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="daily_graph")],
                    className="pretty_container five columns",
                ),
                html.Div(
                    [dcc.Graph(id="monthly_graph")],
                    className="pretty_container five columns",
                ),
                html.Div(
                    [dcc.Graph(id="yearly_graph")],
                    className="pretty_container five columns",
                ),
            ],
            className="row flex-display",
        ),
    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)

# Selectors -> site_info_text
@app.callback(
    [
        Output("site_id_text", "children"),
        Output("site_score_text", "children"),
        Output("state_text", "children"),
        Output("usable_area_text", "children"),
    ],
    [Input("map_graph", "hoverData")],
)
def update_site_info_text(map_graph_hover):
    if map_graph_hover is None:
        site_id = default_site_id
    else:
        point = map_graph_hover['points'][0]
        site_id = point['customdata']

    dff = df[df.index.isin([site_id])]

    return site_id, dff['site_score'], dff['state'], dff['fraction_of_usable_area']

# Create callbacks
app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="resize"),
    Output("output-clientside", "children"),
    [Input("map_graph", "figure")],
)

# Selectors -> main graph
@app.callback(
    Output("map_graph", "figure"),
    [Input("state_dropdown", "value")],
    [State("map_graph", "relayoutData")]
)
def make_map_graph_figure(state_selector, map_graph_layout):
    traces = []

    tdf = df
    state = state_selector
    if isinstance(state_selector, (list)):
        state = state_selector[0]

    try:
        if state != "" and state != "None":
            tdf = df[df['state'] == STATES[state]]
            if tdf.dropna().empty:
                tdf = df
    except KeyError:
        tdf = df

    for site_score, dff in tdf.groupby("site_score"):
        data = dict(
                type="scattermapbox",
                lon=dff["lon"],
                lat=dff["lat"],
                text=dff.index,
                customdata=dff.index,
                name='Score ' + str(site_score),
                marker=dict(size=4, opacity=0.6),
            )
        traces.append(data)

    # relayoutData is None by default, and {'autosize': True} without relayout action
    if map_graph_layout is not None and state_selector is not None:
        if "mapbox.center" in map_graph_layout.keys():
            lon = float(map_graph_layout["mapbox.center"]["lon"])
            lat = float(map_graph_layout["mapbox.center"]["lat"])
            zoom = float(map_graph_layout["mapbox.zoom"])
            layout["mapbox"]["center"]["lon"] = lon
            layout["mapbox"]["center"]["lat"] = lat
            layout["mapbox"]["zoom"] = zoom

    figure = dict(data=traces, layout=layout)
    return figure

# Map graph -> daily graph
@app.callback(
    Output("daily_graph", "figure"), 
    [
        Input("map_graph", "hoverData"),
        Input("feature_drop_down", "value")
    ]
)
def make_daily_graph_figure(map_graph_hover, feature_drop_down_value):

    layout_individual = copy.deepcopy(layout)

    if map_graph_hover is None:
        site_id = default_site_id
    else:
        point = map_graph_hover['points'][0]
        site_id = point['customdata']
    
    daily_df = None
    title = ""
    if feature_drop_down_value == "speed":
        daily_df = pd.DataFrame(get_site_avg_daily(site_id))
        title = "Daily Wind Speed (%s)" % site_id 
    else:
        daily_df = pd.DataFrame(get_site_total_daily_capacity(site_id))
        title = "Daily Wind Capacity (%s)" % site_id 

    if daily_df is None or daily_df.dropna().empty:
        site_id = 0
    else:
        daily_df['date'] = pd.to_datetime(daily_df[['year', 'month', 'day']], infer_datetime_format=True)
    
    if site_id == 0:
        annotation = dict(
            text="No data available",
            x=0.5,
            y=0.5,
            align="center",
            showarrow=False,
            xref="paper",
            yref="paper",
        )
        layout_individual["annotations"] = [annotation]
        layout_individual["title"] = "Daily Chart"
        data = []
    else:
        data = [
            dict(
                type="bar",
                name="Daily Wind Speed Average",
                x=daily_df['date'],
                y=daily_df['dailypower'] if feature_drop_down_value == "power" else daily_df['avgwindspeed']
            ),
        ]
        layout_individual["title"] = title

    figure = dict(data=data, layout=layout_individual)
    return figure

# Map graph -> monthly graph
@app.callback(
    Output("monthly_graph", "figure"), 
    [
        Input("map_graph", "hoverData"),
        Input("feature_drop_down", "value")
    ]
)
def make_monthly_graph_figure(map_graph_hover, feature_drop_down_value):
    layout_individual = copy.deepcopy(layout)

    if map_graph_hover is None:
        site_id = default_site_id
    else:
        point = map_graph_hover['points'][0]
        site_id = point['customdata']

    monthly_df = None
    title = ""
    if feature_drop_down_value == "speed":
        monthly_df = pd.DataFrame(get_site_avg_monthly(site_id))
        title = "Monthly Wind Speed (%s)" % site_id 
    else:
        monthly_df = pd.DataFrame(get_site_total_monthly_capacity(site_id))
        title = "Monthly Wind Capacity (%s)" % site_id 

    if monthly_df is None or monthly_df.dropna().empty:
        site_id = 0
    else:
        monthly_df['date'] = pd.to_datetime([f'{y}-{m}-01' for y, m in zip(monthly_df.year, monthly_df.month)])
    
    if site_id == 0:
        annotation = dict(
            text="No data available",
            x=0.5,
            y=0.5,
            align="center",
            showarrow=False,
            xref="paper",
            yref="paper",
        )
        layout_individual["annotations"] = [annotation]
        layout_individual["title"] = "Monthly Chart"
        data = []
    else:
        data = [
            dict(
                type="bar",
                name="Monthly Wind Capacity",
                x=monthly_df['date'],
                y=monthly_df['monthlypower'] if feature_drop_down_value == "power" else monthly_df['avgwindspeed']
            ),
        ]
        layout_individual["title"] = title

    figure = dict(data=data, layout=layout_individual)
    return figure

# Map graph -> yearly graph
@app.callback(
    Output("yearly_graph", "figure"), 
    [
        Input("map_graph", "hoverData"),
        Input("feature_drop_down", "value")
    ]
)
def make_yearly_graph_figure(map_graph_hover, feature_drop_down_value):

    layout_individual = copy.deepcopy(layout)

    if map_graph_hover is None:
        site_id = default_site_id
    else:
        point = map_graph_hover['points'][0]
        site_id = point['customdata']

    yearly_df = None
    title = ""
    if feature_drop_down_value == "speed":
        yearly_df = pd.DataFrame(get_site_avg_yearly(site_id))
        title = "Yearly Wind Speed (%s)" % site_id 
    else:
        yearly_df = pd.DataFrame(get_site_total_yearly_capacity(site_id))
        title = "Yearly Wind Capacity (%s)" % site_id 

    if yearly_df is None or yearly_df.dropna().empty:
        site_id = 0
    else:
        yearly_df['date'] = pd.to_datetime([f'{y}-01-01' for y in yearly_df.year])
    
    if site_id == 0:
        annotation = dict(
            text="No data available",
            x=0.5,
            y=0.5,
            align="center",
            showarrow=False,
            xref="paper",
            yref="paper",
        )
        layout_individual["annotations"] = [annotation]
        layout_individual["title"] = "Yearly Chart"
        data = []
    else:
        data = [
            dict(
                type="bar",
                name="Yearly Wind Capacity",
                x=yearly_df['date'],
                y=yearly_df['yearlypower'] if feature_drop_down_value == "power" else yearly_df['avgwindspeed']
            ),
        ]
        layout_individual["title"] = title

    figure = dict(data=data, layout=layout_individual)
    return figure

# Main
if __name__ == "__main__":
    app.run_server(port=9050, debug=True)
