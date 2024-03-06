"""
GPS real-time visualization using Dash

Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
Copyright (c) 2021. All Rights Reserved.
"""

# The imports
import os
import json
import numpy
import planar
import random
import plotly.graph_objs as go
from jupyter_dash import JupyterDash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

# A boolean flag to indicate that the reason for this run is either "Simulation" or "Actual Field Work"
field_work = True

# The data storage collections
"""
TODO: Change the initial positioning of the Rx installation
"""
gps_logs_dir = '<gps_logs_directory_location>'
number_of_files = len(os.listdir(gps_logs_dir))
latitudes, longitudes, altitudes = [40.76617367], [-111.84793933], [1451.121]

# Jupyter Dash: Layout Configurations
app = JupyterDash('Odin | SPAVE-28G | Freya | Dash')
app.layout = html.Div([
    html.H1('Odin | SPAVE-28G: Real-Time Rx Route Visualization',
            style={'font-color': 'blue', 'font-family': 'rockwell'}),
    dcc.Interval(
        id='interval-component',
        interval=100,
        n_intervals=0
    ),
    dcc.Graph(id='graph', figure={}),
])

if field_work:
    # Make a note of the GPS log files that have already been read
    read_files = []
    for f in os.listdir(gps_logs_dir):
        lat, long, alt = latitudes[-1], longitudes[-1], altitudes[-1]
        if f.endswith('json') and f not in read_files:
            with open(''.join([gps_logs_dir, f]), 'r') as gps_data:
                gps_data_dict = json.loads(gps_data.read())
                # Account for discrepancies in logging across modules
                if not isinstance(gps_data_dict['latitude'], float):
                    if isinstance(gps_data_dict['latitude']['component'], float) and \
                            gps_data_dict['latitude']['component'] != 0.0:
                        is_valid = True
                        # Parse the assembled component from the Member
                        lat = gps_data_dict['latitude']['component']
                # Account for discrepancies in logging across modules
                if not isinstance(gps_data_dict['longitude'], float):
                    if isinstance(gps_data_dict['longitude']['component'], float) and \
                            gps_data_dict['longitude']['component'] != 0.0:
                        is_valid = True
                        # Parse the assembled component from the Member
                        long = gps_data_dict['longitude']['component']
                # Account for discrepancies in logging across modules
                if not isinstance(gps_data_dict['altitude_ellipsoid'], float):
                    if isinstance(gps_data_dict['altitude_ellipsoid']['component'], float) and \
                            gps_data_dict['altitude_ellipsoid']['component'] != 0.0:
                        is_valid = True
                        # Parse the assembled component from the Member
                        alt = gps_data_dict['altitude_ellipsoid']['component']
                read_files.append(f)
            latitudes.append(lat)
            longitudes.append(long)
            altitudes.append(alt)

# Global "latitudes" and "longitudes" array for dash update callback
latitudes__, longitudes__ = [], []

# Simulate the routes for this testing...
# Route-1
for i in range(1, 120):
    latitudes.append(latitudes[i - 1] + 0.00005 * random.random())
    longitudes.append(longitudes[i - 1] + 0.00005 * random.random())
    altitudes.append(altitudes[i - 1])

# Route-2
for i in range(120, 240):
    latitudes.append(latitudes[i - 1] + 0.00005 * random.random())
    longitudes.append(longitudes[i - 1] - 0.00005 * random.random())
    altitudes.append(altitudes[i - 1])

# Route-3
for i in range(240, 500):
    latitudes.append(latitudes[i - 1] - 0.00005 * random.random())
    longitudes.append(longitudes[i - 1] - 0.00005 * random.random())
    altitudes.append(altitudes[i - 1])


def determine_zoom_level(__latitudes, __longitudes):
    """
    Determine the zoom level

    Args:
        __latitudes: The collection of latitudes up to this point in time
        __longitudes: The collection of longitudes up to this point in time

    Returns: The determined zoom level and center of the rendered visualization
    """
    all_pairs = []
    for lon__, lat__ in zip(longitudes, latitudes):
        all_pairs.append((lon__, lat__))
    b_box = planar.BoundingBox(all_pairs)
    if b_box.is_empty:
        return 0, (0, 0)
    area = b_box.height * b_box.width
    zoom = numpy.interp(area, [0, 5 ** -10, 4 ** -10, 3 ** -10, 2 ** -10, 1 ** -10, 1 ** -5],
                        [20, 17, 16, 15, 14, 7, 5])
    return zoom, b_box.center


# Define callback to update graph
@app.callback(
    Output('graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def stream_fig(index):
    """
    Update Figure with new data callback

    Returns: The updated figure
    """
    global latitudes__, longitudes__
    # Data availability limit heck
    if index >= number_of_files:
        return
    latitudes__.append(latitudes[index])
    longitudes__.append(longitudes[index])
    map_layout = go.Layout(title='Odin | SPAVE-28G: Real-Time Rx Visualization', autosize=True, hovermode='closest',
                           margin=dict(t=0, b=0, l=0, r=0), width=2256, height=1504)
    fig__ = go.Figure(data=go.Scattermapbox(lat=latitudes__, lon=longitudes__, mode='markers',
                                            marker=go.scattermapbox.Marker(size=9)),
                      layout=map_layout)
    zoom, center = determine_zoom_level(latitudes__, longitudes__)
    fig__.update_layout(
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            accesstoken='<mapbox_access_token>',
            bearing=0,
            center=dict(
                lat=center[1],
                lon=center[0]
            ),
            pitch=0,
            zoom=zoom
        ),
    )
    return fig__


# Start the server and start visualization
app.run_server(mode='external', port='<port>', dev_tools_ui=True, debug=True, dev_tools_hot_reload=True, threaded=True)
