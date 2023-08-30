"""
Bokeh Google Maps Playground [Testing Script]

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

from bokeh.plotting import gmap
from bokeh.io import export_png
from bokeh.models import ColumnDataSource, GMapOptions

google_map_options = GMapOptions(lat=40.7650, lng=-111.8550, map_type='hybrid', zoom=21, tilt=-45)
google_map = gmap('AIzaSyCQq7tZREFvb8G1NbirMweUKv_TTp4aUUA', google_map_options, width=8500, height=3500)

google_map.circle(x='lon', y='lat', size=40, fill_color='red',
                  fill_alpha=1, source=ColumnDataSource(data=dict(lat=[40.765055], lon=[-111.853889])))

google_map.diamond(x='lon', y='lat', size=80, fill_color='blue',
                   fill_alpha=1, source=ColumnDataSource(data=dict(lat=[40.766173670], lon=[-111.847939330])))

export_png(google_map, filename='C:/Users/kesha/Downloads/test_map.png', width=8500, height=3500, timeout=12000)
