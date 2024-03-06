"""
GPS TxRealm and/or RxRealm route (and/or signal power) visualizations using Bokeh and the Google Maps API

This Python script encapsulates the operations involved in the visualization of the routes traversed by the Tx and Rx
realm units -- on the Google Maps API [2D Maps | 3D Maps | Satellite | Hybrid | Earth | Roads].

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ.
Copyright (c) 2021. All Rights Reserved.
"""

# The imports
import os
import json
import pandas
from bokeh.plotting import gmap
from bokeh.io import export_png
from bokeh.palettes import brewer
from dataclasses import make_dataclass
from bokeh.models import GMapOptions, ColumnDataSource, ColorBar, LinearColorMapper

"""
Data Object Setup
"""
gps_coordinates = list()
gps_coordinates_dataframe = None
gps_coordinate = make_dataclass('GPS_Coordinate', [('latitude', float), ('longitude', float)])

"""
TODO: Configurations-I | Input and Output File Locations
"""
visualization_mode = 'route'  # Allowed modes = 'route', 'rx-power'

# Input directories and files

gps_log_files_dir = '<dataset_location>/routes/gps-data/urban-stadium/'
# gps_log_files_dir = '<dataset_location>/routes/gps-data/urban-campus-II/'

rx_power_matched_csv_file = 'urban-stadium-rx-power.csv'
# rx_power_matched_csv_file = 'urban-campus-II-rx-power.csv'
rx_power_matched_csv_dir = '<rx_power_test_dataset_location>'

# Output directories and files

png_file_name = 'urban-stadium-route.png'
png_file_dir = '<output_directory_location>'
# png_file_name = 'urban-campus-II-route.png'

"""
TODO: Configurations-II | Map Visualization Options
"""
map_type = 'hybrid'  # Allowed types = 'satellite', 'terrain', 'hybrid', 'roadmap'

map_width, map_height, map_zoom_level, map_title = 3000, 5000, 20, 'Urban Stadium Route [Van]'
# map_width, map_height, map_zoom_level, map_title = 6300, 6300, 20, 'Urban Campus-II Route [Van]'

map_central = gps_coordinate(40.7640, -111.8479)  # urban-stadium central <latitude, longitude> in degrees
# map_central = gps_coordinate(40.7640, -111.8515)  # urban-campus-II central <latitude, longitude> in degrees

tx_location = gps_coordinate(40.766173670, -111.847939330)  # <latitude, longitude> in degrees
tx_pin_size, tx_pin_alpha, tx_pin_color = 80, 1.0, 'red'
rx_pins_size, rx_pins_alpha, rx_pins_color = 30, 1.0, 'yellow'
color_palette, color_palette_index = 'RdYlGn', 11

# urban-campus-II
# color_bar_width, color_bar_height, color_bar_label_size, color_bar_orientation = 125, 6250, '125px', 'vertical'

# urban-stadium
color_bar_width, color_bar_height, color_bar_label_size, color_bar_orientation = 125, 3950, '125px', 'vertical'

color_bar_layout_location = 'right'
google_maps_api_key = '<google_maps_api_key>'
png_file_export_timeout = 300  # In seconds [Selenium Requirements: <FireFox, GeckoDriver> | <Chromium, ChromeDriver>]

# Extraction: Read and Collect the JSON logs (GPS publishes/subscriptions corresponding to a certain realm) AND
# Collection: Create a Pandas Dataframe from a collection constituting the parsed GPS_Coordinate dataclass instances
if visualization_mode == 'route':
    for gps_log_file in os.listdir(gps_log_files_dir):
        with open(''.join([gps_log_files_dir, gps_log_file]), 'r') as gps_data:
            gps_data_dict = json.loads(gps_data.read())
            gps_coordinates.append(gps_coordinate(gps_data_dict['latitude']['component'],
                                                  gps_data_dict['longitude']['component']))
    gps_coordinates_dataframe = pandas.DataFrame(gps_coordinates)
else:
    gps_coordinates_dataframe = pandas.read_csv(''.join([rx_power_matched_csv_dir, rx_power_matched_csv_file]),
                                                names=['latitude', 'longitude', 'rx-power'])

# Visualization: Google Maps rendition of the specified route OR received signal power levels along the specified route
gps_coordinates_dataframe.drop(gps_coordinates_dataframe[gps_coordinates_dataframe['longitude'] <= -111.85].index,
                               inplace=True)  # Specific to urban-stadium to drop the west-side parking-lot
google_maps_options = GMapOptions(lat=map_central.latitude, lng=map_central.longitude,
                                  map_type=map_type, zoom=map_zoom_level)
figure = gmap(google_maps_api_key, google_maps_options, title=map_title, width=map_width, height=map_height)
figure_tx_point = figure.diamond([tx_location.longitude], [tx_location.latitude],
                                 size=tx_pin_size, alpha=tx_pin_alpha, color=tx_pin_color)

if visualization_mode == 'route':
    figure_rx_points = figure.circle('longitude', 'latitude', size=rx_pins_size, alpha=rx_pins_alpha,
                                     color=rx_pins_color, source=ColumnDataSource(gps_coordinates_dataframe))
else:
    palette = brewer[color_palette][color_palette_index]
    color_mapper = LinearColorMapper(palette=palette, low=gps_coordinates_dataframe['rx-power'].min(),
                                     high=gps_coordinates_dataframe['rx-power'].max())
    color_bar = ColorBar(color_mapper=color_mapper, width=color_bar_width, height=color_bar_height,
                         major_label_text_font_size=color_bar_label_size,
                         label_standoff=color_palette_index, orientation=color_bar_orientation)
    figure_rx_points = figure.circle('longitude', 'latitude', size=rx_pins_size, alpha=rx_pins_alpha,
                                     color={'field': 'rx-power', 'transform': color_mapper},
                                     source=ColumnDataSource(gps_coordinates_dataframe))
    figure.add_layout(color_bar, color_bar_layout_location)

# Output image file export
export_png(figure, filename=''.join([png_file_dir, png_file_name]), timeout=png_file_export_timeout)
# The End
