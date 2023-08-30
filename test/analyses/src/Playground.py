"""
Bokeh Rx-Power & Pathloss Route Visualizations Playground [Testing Script]

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

import datetime
import numpy as np
import pandas as pd
from enum import Enum
from typing import List
from bokeh.io import show
from scipy import constants
from bokeh.plotting import gmap
from dataclasses import dataclass, field
from bokeh.models import GMapOptions, ColumnDataSource, ColorBar, LinearColorMapper, FixedTicker

distns, pls = [], []
pi, c = np.pi, constants.speed_of_light
deg2rad, rad2deg = lambda x: x * (pi / 180.0), lambda x: x * (180.0 / pi)
linear_1, linear_2 = lambda x: 10 ** (x / 10.0), lambda x: 10 ** (x / 20.0)
decibel_1, decibel_2 = lambda x: 10.0 * np.log10(x), lambda x: 20.0 * np.log10(x)
gps_events, tx_imu_traces, rx_imu_traces, pdp_segments, pods, meas_pwrs = [], [], [], [], [], []


class PathlossApproaches(Enum):
    SPAVE28G_ODIN = 0
    TR38901_3GPP = 1
    M2135_ITUR = 2
    MM_MAGIC = 3
    METIS = 4


class Units(Enum):
    METERS = 0
    CENTIMETERS = 1
    MILLIMETERS = 2
    DEGREES = 3
    MINUTES = 4
    SECONDS = 5
    INCHES = 6
    FEET = 7
    YARDS = 8
    DIMENSIONLESS = 9
    UNKNOWN = 10


class FixType(Enum):
    NO_FIX = 0
    DEAD_RECKONING = 1
    TWO_DIMENSIONAL = 2
    THREE_DIMENSIONAL = 3
    GNSS = 4
    TIME_FIX = 5


class CarrierSolutionType(Enum):
    NO_SOLUTION = 0
    FLOAT_SOLUTION = 1
    FIXED_SOLUTION = 2


@dataclass(order=True)
class Member:
    is_high_precision: bool = False
    main_component: float = 0.0
    high_precision_component: float = 0.0
    component: float = 0.0
    precision: float = 0.0
    units: Units = Units.DIMENSIONLESS


@dataclass(order=True)
class GPSEvent:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    is_gnss_fix_ok: bool = False
    siv: int = 0
    fix_type: FixType = FixType.NO_FIX
    carrier_solution_type: CarrierSolutionType = CarrierSolutionType.NO_SOLUTION
    latitude: Member = Member()  # components: deg
    longitude: Member = Member()  # components: deg
    altitude_ellipsoid: Member = Member()  # components: m
    altitude_msl: Member = Member()  # components: m
    speed: Member = Member()  # components: ms-1
    heading: Member = Member()
    horizontal_acc: Member = Member()  # components: ms-2
    vertical_acc: Member = Member()  # components: ms-2
    speed_acc: Member = Member()
    heading_acc: Member = Member()
    ned_north_vel: Member = Member()
    ned_east_vel: Member = Member()
    ned_down_vel: Member = Member()
    pdop: Member = Member()
    mag_acc: Member = Member()
    mag_dec: Member = Member()
    geometric_dop: Member = Member()
    position_dop: Member = Member()
    time_dop: Member = Member()
    horizontal_dop: Member = Member()
    vertical_dop: Member = Member()
    northing_dop: Member = Member()
    easting_dop: Member = Member()
    horizontal_accuracy: Member = Member()  # components: ms-2
    vertical_accuracy: Member = Member()  # components: m
    ultra_core_length: int = 16
    core_length: int = 37
    total_length: int = 39


@dataclass(order=True)
class IMUTrace:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    yaw_angle: float = 0.0  # deg
    pitch_angle: float = 0.0  # deg


@dataclass(order=True)
class PDPSegment:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    sample_rate: float = 2e6  # sps
    rx_freq: float = 2.5e9  # Hz
    item_size: int = 8
    core_header_length: int = 149
    extra_header_length: int = 22
    header_length: int = 171
    num_samples: int = int(1e6)
    num_bytes: int = num_samples * item_size
    raw_rx_samples: np.array = np.array([], dtype=np.csingle)  # complex I/Q-64
    processed_rx_samples: np.array = np.array([], dtype=np.csingle)  # complex I/Q-64


@dataclass(order=True)
class Pod:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    gps_event: GPSEvent = GPSEvent()
    tx_imu_trace: IMUTrace = IMUTrace()
    rx_imu_trace: IMUTrace = IMUTrace()
    pdp_segment: PDPSegment = PDPSegment()
    elevation: float = 0.0  # m
    distance_2d: float = 0.0  # m
    distance_3d: float = 0.0  # m
    rx_power: float = 0.0  # dB
    tx_ant_gain: float = 0.0  # dB
    rx_ant_gain: float = 0.0  # dB
    pathloss: List = field(default_factory=lambda: [0.0 for _ in PathlossApproaches])  # dB


''' urban-campus-I route (semi-autonomous) (1400 E St) '''
# pwr_png, pl_png = 'urban_campus_I_pwr.png', 'urban_campus_I_pl.png'
# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'urban-campus-I'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7626), longitude=Member(component=-111.8486))

''' urban-campus-II route (fully-autonomous) (President's Circle) '''
pwr_png, pl_png = 'urban_campus_II_pwr.png', 'urban_campus_II_pl.png'
map_width, map_height, map_zoom_level, map_title = 8400, 2800, 20, 'urban-campus-II'
map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7650), longitude=Member(component=-111.8550))

''' urban-campus-III route (fully-autonomous) (100 S St) '''
# pwr_png, pl_png = 'urban_campus_III_pwr.png', 'urban_campus_III_pl.png'
# map_width, map_height, map_zoom_level, map_title = 5600, 2800, 21, 'urban-campus-III'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7651), longitude=Member(component=-111.8500))

''' urban-garage route (semi-autonomous) (NW Garage on 1460 E St) '''
# pwr_png, pl_png = 'urban_garage_pwr.png', 'urban_garage_pl.png'
# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'urban-garage'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7670), longitude=Member(component=-111.8480))

''' urban-stadium route (fully-autonomous) (E South Campus Dr) '''
# pwr_png, pl_png = 'urban_stadium_pwr.png', 'urban_stadium_pl.png'
# map_width, map_height, map_zoom_level, map_title = 5500, 3500, 20, 'urban-stadium'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7670), longitude=Member(component=-111.8480))

''' suburban-fraternities route (fully-autonomous) (S Wolcott St) '''
# pwr_png, pl_png = 'suburban_frats_pwr.png', 'suburban_frats_pl.png'
# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'suburban-fraternities'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7670), longitude=Member(component=-111.8480))

''' urban-vegetation route (fully-autonomous) (Olpin Union Bldg) '''
# pwr_png, pl_png = 'urban_vegetation_pwr.png', 'urban_vegetation_pl.png'
# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'urban-vegetation'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7655), longitude=Member(component=-111.8479))

color_bar_width, color_bar_height, color_bar_orientation = 100, 2600, 'vertical'
rx_offset, pl_offset, rx_tick_num, pl_tick_num, rx_tilt, pl_tilt = 1.0, 1.0, 5, 5, -45, -45
color_bar_layout_location, color_palette, color_bar_label_size = 'right', 'Magma256', '75px'
tx_pin_size, tx_pin_alpha, tx_pin_color, rx_pins_size, rx_pins_alpha = 160, 1.0, 'blue', 40, 1.0
google_maps_api_key, map_type, timeout = 'AIzaSyCQq7tZREFvb8G1NbirMweUKv_TTp4aUUA', 'hybrid', 6000

tx = GPSEvent(latitude=Member(component=40.766173670),
              longitude=Member(component=-111.847939330), altitude_ellipsoid=Member(component=1459.1210))


# Latitude getter (deg)
def latitude(y: GPSEvent) -> float:
    return y.latitude.component


# Longitude getter (deg)
def longitude(y: GPSEvent) -> float:
    return y.longitude.component


# Altitude getter (deg)
def altitude(y: GPSEvent) -> float:
    return y.altitude_ellipsoid.component


rx_google_maps_options = GMapOptions(map_type=map_type, zoom=map_zoom_level, tilt=rx_tilt,
                                     lat=latitude(map_central), lng=longitude(map_central))
pl_google_maps_options = GMapOptions(map_type=map_type, zoom=map_zoom_level, tilt=pl_tilt,
                                     lat=latitude(map_central), lng=longitude(map_central))

pl_kw, rx_kw, lat_kw, lon_kw = 'pathloss', 'rx-power', 'latitude', 'longitude'

rx_df = pd.read_csv('C:/Users/bkeshav1/Downloads/dataframes/urban_campus_II_rx_df.csv')
pl_df = pd.read_csv('C:/Users/bkeshav1/Downloads/dataframes/urban_campus_II_pl_df.csv')

rx_color_mapper = LinearColorMapper(high=rx_df[rx_kw].min() - rx_offset,
                                    low=rx_df[rx_kw].max() + rx_offset, palette=color_palette)
pl_color_mapper = LinearColorMapper(low=pl_df[pl_kw].min() - pl_offset,
                                    high=pl_df[pl_kw].max() + pl_offset, palette=color_palette)

rx_ticks = [_x for _x in np.linspace(rx_df[rx_kw].min(), rx_df[rx_kw].max(), rx_tick_num)]
pl_ticks = [_x for _x in np.linspace(pl_df[pl_kw].min(), pl_df[pl_kw].max(), pl_tick_num)]

rx_color_bar = ColorBar(width=color_bar_width, height=color_bar_height,
                        major_label_text_font_size=color_bar_label_size, ticker=FixedTicker(ticks=rx_ticks),
                        color_mapper=rx_color_mapper, label_standoff=len(rx_ticks), orientation=color_bar_orientation)
pl_color_bar = ColorBar(width=color_bar_width, height=color_bar_height,
                        major_label_text_font_size=color_bar_label_size, ticker=FixedTicker(ticks=pl_ticks),
                        color_mapper=pl_color_mapper, label_standoff=len(pl_ticks), orientation=color_bar_orientation)

rx_figure = gmap(google_maps_api_key, rx_google_maps_options, width=map_width, height=map_height)
pl_figure = gmap(google_maps_api_key, pl_google_maps_options, width=map_width, height=map_height)

rx_figure.toolbar.logo = None
rx_figure.toolbar_location = None
rx_figure.xaxis.major_tick_line_color = None
rx_figure.xaxis.minor_tick_line_color = None
rx_figure.yaxis.major_tick_line_color = None
rx_figure.yaxis.minor_tick_line_color = None
rx_figure.xaxis.major_label_text_font_size = '0pt'
rx_figure.yaxis.major_label_text_font_size = '0pt'

pl_figure.toolbar.logo = None
pl_figure.toolbar_location = None
pl_figure.xaxis.major_tick_line_color = None
pl_figure.xaxis.minor_tick_line_color = None
pl_figure.yaxis.major_tick_line_color = None
pl_figure.yaxis.minor_tick_line_color = None
pl_figure.xaxis.major_label_text_font_size = '0pt'
pl_figure.yaxis.major_label_text_font_size = '0pt'

rx_figure.add_layout(rx_color_bar)
pl_figure.add_layout(pl_color_bar)

rx_figure.diamond('lon', 'lat', size=tx_pin_size, fill_color=tx_pin_color, fill_alpha=tx_pin_alpha,
                  source=ColumnDataSource(data=dict(lat=[latitude(tx)], lon=[longitude(tx)])))
pl_figure.diamond('lon', 'lat', size=tx_pin_size, fill_color=tx_pin_color, fill_alpha=tx_pin_alpha,
                  source=ColumnDataSource(data=dict(lat=[latitude(tx)], lon=[longitude(tx)])))

rx_figure.circle(lon_kw, lat_kw, size=rx_pins_size, alpha=rx_pins_alpha,
                 color={'field': rx_kw, 'transform': rx_color_mapper}, source=ColumnDataSource(rx_df))
pl_figure.circle(lon_kw, lat_kw, size=rx_pins_size, alpha=rx_pins_alpha,
                 color={'field': pl_kw, 'transform': pl_color_mapper}, source=ColumnDataSource(pl_df))

rx_fig_loc = ''.join(['E:/Workspace/SPAVE-28G/test/analyses/', pwr_png])
pl_fig_loc = ''.join(['E:/Workspace/SPAVE-28G/test/analyses/', pl_png])

show(rx_figure)
# show(pl_figure)

# export_png(rx_figure, filename=rx_fig_loc, width=map_width + 100, height=map_height + 100, timeout=timeout)
# export_png(pl_figure, filename=pl_fig_loc, width=map_width + 100, height=map_height + 100, timeout=timeout)

print('SPAVE-28G | Consolidated Processing I | Received power map: {}'.format(rx_fig_loc))
print('SPAVE-28G | Consolidated Processing I | Pathloss map: {}'.format(pl_fig_loc))
