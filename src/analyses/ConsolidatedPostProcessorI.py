"""
This script encapsulates the operations involved in visualizing the antenna patterns and the Rx power maps of our
mmWave (28 GHz) V2X propagation modeling activities on the POWDER testbed in Salt Lake City. Subsequently, this
script  generates the pathloss maps of the routes traversed during this measurement campaign, along with the
plots of pathloss versus distance which will help us with next-generation mmWave V2V/V2I network design.
Also, we evaluate these empirical pathloss values against the ITU-R M.2135, 3GPP TR38.901, mmMAGIC,
and METIS outdoor urban micro-cellular (UMi) and/or urban macro-cellular (UMa) pathloss standards.

Lastly, as a part of this script, we evaluate the impact of blockages (moving/parked vehicles, pedestrians, trees) on
the pathloss behavior of 28 GHz signals in V2I/V2V settings, in addition to analytical studies of fitted models to
the empirical shadow fading results obtained from our measurements along various vehicular routes onsite.

Reference Papers:

@INPROCEEDINGS{PL-Models-I,
  author={Haneda, Katsuyuki and Zhang, Jianhua and Tan, Lei and Liu, Guangyi and Zheng, Yi and Asplund, Henrik, et al.},
  title={5G 3GPP-Like Channel Models for Outdoor Urban Microcellular and Macrocellular Environments},
  year={2016}, volume={}, number={}, pages={1-7}, doi={10.1109/VTCSpring.2016.7503971},
  booktitle={2016 IEEE 83rd Vehicular Technology Conference (VTC Spring)}}

@ARTICLE{PL-Models-II,
  author={Rappaport, Theodore S. and Xing, Yunchou and MacCartney, George R. and Molisch, Andreas F. et al.},
  title={Overview of mmWave Communications for 5G Wireless Networks—With a Focus on Propagation Models},
  year={2017}, volume={65}, number={12}, pages={6213-6230}, doi={10.1109/TAP.2017.2734243},
  journal={IEEE Transactions on Antennas and Propagation}}

@INPROCEEDINGS{Dynamic-Blockages,
  author={Haneda, Katsuyuki and Zhang, Jianhua and Tan, Lei and Liu, Guangyi and Zheng, Yi and Asplund, et al.},
  title={5G 3GPP-Like Channel Models for Outdoor Urban Microcellular and Macrocellular Environments},
  booktitle={2016 IEEE 83rd Vehicular Technology Conference (VTC Spring)},
  year={2016}, pages={1-7}, doi={10.1109/VTCSpring.2016.7503971}}.

@INPROCEEDINGS{Shadowing,
  title={28-GHz High-Speed Train Measurements and Propagation Characteristics Analysis},
  booktitle={2020 14th European Conference on Antennas and Propagation (EuCAP)},
  author={Park, Jae-Joon and Lee, Juyul and Kim, Kyung-Won and Kim, Myung-Don},
  year={2020}, pages={1-5}, doi={10.23919/EuCAP48036.2020.9135221}}.

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

import os
import re
import json
import plotly
import scipy.io
import datetime
import requests
import traceback
import dataclasses
import numpy as np
import pandas as pd
from enum import Enum
from pyproj import Proj
from geopy import distance
from scipy.stats import norm
import plotly.graph_objs as go
from json import JSONDecodeError
# from bokeh.plotting import gmap
# from bokeh.io import export_png
from typing import List, Tuple, Dict
from scipy.interpolate import interp1d
from dataclasses import dataclass, field
import sk_dsp_comm.fir_design_helper as fir_d
from scipy import signal, constants, integrate
from concurrent.futures import ThreadPoolExecutor

# from bokeh.models import GMapOptions, ColumnDataSource, ColorBar, LinearColorMapper, FixedTicker

"""
INITIALIZATIONS I: Collections & Utilities
"""
distns, pls = [], []
pi, c = np.pi, constants.speed_of_light
deg2rad, rad2deg = lambda x: x * (pi / 180.0), lambda x: x * (180.0 / pi)
linear_1, linear_2 = lambda x: 10 ** (x / 10.0), lambda x: 10 ** (x / 20.0)
decibel_1, decibel_2 = lambda x: 10.0 * np.log10(x), lambda x: 20.0 * np.log10(x)
gps_events, tx_imu_traces, rx_imu_traces, pdp_segments, pods, meas_pwrs = [], [], [], [], [], []

"""
INITIALIZATIONS II: Enumerations & Dataclasses
"""


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


"""
CONFIGURATIONS I: A few route-specific Bokeh & Plotly visualization options
                  Input & Output Dirs | GPS & IMU logs | Power delay profiles | Antenna pattern logs | Calibration logs
"""

''' urban-campus-I route (semi-autonomous) (1400 E St) '''
gps_dir = 'E:/SPAVE-28G/analyses/urban-campus-I/rx-realm/gps/'
comm_dir = 'E:/SPAVE-28G/analyses/urban-campus-I/rx-realm/pdp/'
rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-campus-I/rx-realm/imu/'
rx_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_campus_I_rx_df.csv'
pl_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_campus_I_pl_df.csv'
# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'urban-campus-I'
# rxp_ffe_png, pg_ffe_png, shadowing_png = 'uc_rxp_ffe.png', 'uc_pg_ffe.png', 'uc_shadowing.png'
tx_imu_dir, tx_imu_skip_step, sfe_norm_fit_num = 'E:/SPAVE-28G/analyses/urban-campus-I/tx-realm/imu/', 1, 1000
# pwr_png, pl_png, pl_dist_png = 'urban_campus_I_pwr.png', 'urban_campus_I_pl.png', 'urban_campus_I_pl_dist.png'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7626), longitude=Member(component=-111.8486))

''' urban-campus-II route (fully-autonomous) (President's Circle) '''
# gps_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/gps/'
# comm_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/pdp/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/imu/'
# rx_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_campus_II_rx_df.csv'
# pl_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_campus_II_pl_df.csv'
# map_width, map_height, map_zoom_level, map_title = 8400, 2800, 20, 'urban-campus-II'
# rxp_ffe_png, pg_ffe_png, shadowing_png = 'ucc_rxp_ffe.png', 'ucc_pg_ffe.png', 'ucc_shadowing.png'
# tx_imu_dir, tx_imu_skip_step, sfe_norm_fit_num = 'E:/SPAVE-28G/analyses/urban-campus-II/tx-realm/imu/', 5, 1000
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7650), longitude=Member(component=-111.8550))
# pwr_png, pl_png, pl_dist_png = 'urban_campus_II_pwr.png', 'urban_campus_II_pl.png', 'urban_campus_II_pl_dist.png'

''' urban-campus-III route (fully-autonomous) (100 S St) '''
# gps_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/gps/'
# comm_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/pdp/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/imu/'
# rx_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_campus_III_rx_df.csv'
# pl_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_campus_III_pl_df.csv'
# map_width, map_height, map_zoom_level, map_title = 5600, 2800, 21, 'urban-campus-III'
# rxp_ffe_png, pg_ffe_png, shadowing_png = 'uccc_rxp_ffe.png', 'uccc_pg_ffe.png', 'uccc_shadowing.png'
# tx_imu_dir, tx_imu_skip_step, sfe_norm_fit_num = 'E:/SPAVE-28G/analyses/urban-campus-III/tx-realm/imu/', 5, 1000
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7651), longitude=Member(component=-111.8500))
# pwr_png, pl_png, pl_dist_png = 'urban_campus_III_pwr.png', 'urban_campus_III_pl.png', 'urban_campus_III_pl_dist.png'

''' urban-garage route (semi-autonomous) (NW Garage on 1460 E St) '''
# gps_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/gps/'
# comm_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/pdp/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/imu/'
# rx_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_garage_rx_df.csv'
# pl_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_garage_pl_df.csv'
# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'urban-garage'
# rxp_ffe_png, pg_ffe_png, shadowing_png = 'ug_rxp_ffe.png', 'ug_pg_ffe.png', 'ug_shadowing.png'
# pwr_png, pl_png, pl_dist_png = 'urban_garage_pwr.png', 'urban_garage_pl.png', 'urban_garage_pl_dist.png'
# tx_imu_dir, tx_imu_skip_step, sfe_norm_fit_num = 'E:/SPAVE-28G/analyses/urban-garage/tx-realm/imu/', 1, 1000
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7670), longitude=Member(component=-111.8480))

''' urban-stadium route (fully-autonomous) (E South Campus Dr) '''
# gps_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/gps/'
# comm_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/pdp/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/imu/'
# rx_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_stadium_rx_df.csv'
# pl_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_stadium_pl_df.csv'
# map_width, map_height, map_zoom_level, map_title = 5500, 3500, 20, 'urban-stadium'
# rxp_ffe_png, pg_ffe_png, shadowing_png = 'us_rxp_ffe.png', 'us_pg_ffe.png', 'us_shadowing.png'
# pwr_png, pl_png, pl_dist_png = 'urban_stadium_pwr.png', 'urban_stadium_pl.png', 'urban_stadium_pl_dist.png'
# tx_imu_dir, tx_imu_skip_step, sfe_norm_fit_num = 'E:/SPAVE-28G/analyses/urban-stadium/tx-realm/imu/', 5, 1000
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7670), longitude=Member(component=-111.8480))

''' suburban-fraternities route (fully-autonomous) (S Wolcott St) '''
# gps_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/gps/'
# comm_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/pdp/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/imu/'
# rx_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/suburban_frats_rx_df.csv'
# pl_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/suburban_frats_pl_df.csv'
# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'suburban-fraternities'
# rxp_ffe_png, pg_ffe_png, shadowing_png = 'sf_rxp_ffe.png', 'sf_pg_ffe.png', 'sf_shadowing.png'
# pwr_png, pl_png, pl_dist_png = 'suburban_frats_pwr.png', 'suburban_frats_pl.png', 'suburban_frats_pl_dist.png'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7670), longitude=Member(component=-111.8480))
# tx_imu_dir, tx_imu_skip_step, sfe_norm_fit_num = 'E:/SPAVE-28G/analyses/suburban-fraternities/tx-realm/imu/', 1, 1000

''' urban-vegetation route (fully-autonomous) (Olpin Union Bldg) '''
# gps_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/gps/'
# comm_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/pdp/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/imu/'
# rx_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_vegetation_rx_df.csv'
# pl_df_op_file = 'E:/Workspace/SPAVE-28G/test/analyses/urban_vegetation_pl_df.csv'
# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'urban-vegetation'
# rxp_ffe_png, pg_ffe_png, shadowing_png = 'uv_rxp_ffe.png', 'uv_pg_ffe.png', 'uv_shadowing.png'
# tx_imu_dir, tx_imu_skip_step, sfe_norm_fit_num = 'E:/SPAVE-28G/analyses/urban-vegetation/tx-realm/imu/', 1, 1000
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7655), longitude=Member(component=-111.8479))
# pwr_png, pl_png, pl_dist_png = 'urban_vegetation_pwr.png', 'urban_vegetation_pl.png', 'urban_vegetation_pl_dist.png'

''' Generic configurations '''
# output_dir = 'E:/Workspace/SPAVE-28G/test/analyses/'
ant_log_file = 'E:/SPAVE-28G/analyses/antenna_pattern.mat'
pdp_samples_file, start_timestamp_file, parsed_metadata_file = 'samples.log', 'timestamp.log', 'parsed_metadata.log'
att_indices, cali_metadata_file_left, cali_metadata_file_right = list(range(0, 30, 2)), 'u76_', '_parsed_metadata.log'
cali_dir, cali_samples_file_left, cali_samples_file_right = 'E:/SPAVE-28G/analyses/calibration/', 'u76_', '_samples.log'

"""
CONFIGURATIONS II: Data post-processing parameters | Time-windowing | Pre-filtering | Noise elimination
"""
datetime_format, ne_amp_threshold = '%Y-%m-%d %H:%M:%S.%f', 0.05
h_avg, w_avg, rx_usrp_gain, sample_rate, invalid_min_magnitude = 21.3, 15.4, 76.0, 2e6, 1e5
carrier_freq, max_ant_gain, angle_res_ext, tx_pwr, uconv_gain, dconv_gain = 28e9, 22.0, 5.0, 23.0, 13.4, 13.4
time_windowing_config = {'window_multiplier': 2.0, 'truncation_length': int(2e5), 'truncation_multiplier': 4.0}
calc_pwrs = [-39.6, -42.1, -44.6, -47.1, -49.6, -52.1, -54.6, -57.1, -59.6, -62.1, -64.6, -67.1, -69.6, -72.1, -74.6]
prefilter_config = {'passband_freq': 60e3, 'stopband_freq': 65e3, 'passband_ripple': 0.01, 'stopband_attenuation': 80.0}

"""
CONFIGURATIONS III: LLA-to-UTM converter | Additional Bokeh & Plotly visualization options
"""
max_workers, sg_wsize, sg_poly_order = 4096, 53, 3
lla_utm_proj = Proj(proj='utm', zone=32, ellps='WGS84')
# color_bar_width, color_bar_height, color_bar_orientation = 100, 3400, 'vertical'
# rx_offset, pl_offset, rx_tick_num, pl_tick_num, rx_tilt, pl_tilt = 1.0, 1.0, 5, 5, -45, -45
# color_bar_layout_location, color_palette, color_bar_label_size = 'right', 'Magma256', '75px'
plotly.tools.set_credentials_file(username='bkeshava_bkeshav1', api_key='hspqOdIFQcnHdlL7MGch')
# tx_pin_size, tx_pin_alpha, tx_pin_color, rx_pins_size, rx_pins_alpha = 80, 1.0, 'blue', 40, 1.0
# google_maps_api_key, map_type, timeout = 'AIzaSyCQq7tZREFvb8G1NbirMweUKv_TTp4aUUA', 'hybrid', 12000


"""
CONFIGURATIONS IV: Tx location fixed on the rooftop of the William Browning Building in SLC, UT
"""
tx = GPSEvent(latitude=Member(component=40.766173670),
              longitude=Member(component=-111.847939330), altitude_ellipsoid=Member(component=1459.1210))

"""
CORE ROUTINES
"""


# Pack a dictionary into a dataclass instance
def ddc_transform(d: Dict, dc: dataclass) -> dataclass:
    d_l, d_f = {}, {f.name: f.type for f in dataclasses.fields(dc)}

    for k, v in d.items():
        d_l[k] = (lambda: v, lambda: ddc_transform(v, Member))[d_f[k] == Member]()

    return dc(**d_l)


# Parse the provided file as a dataclass instance and store it in the given collection
def parse(d: List, dc: dataclass, fn: str) -> None:
    with open(fn) as f:
        d.append(ddc_transform(json.load(f), dc))


# Yaw angle getter (deg)
def yaw(m: IMUTrace) -> float:
    return m.yaw_angle


# Pitch angle getter (deg)
def pitch(m: IMUTrace) -> float:
    return m.pitch_angle


# Latitude getter (deg)
def latitude(y: GPSEvent) -> float:
    return y.latitude.component


# Longitude getter (deg)
def longitude(y: GPSEvent) -> float:
    return y.longitude.component


# Altitude getter (deg)
def altitude(y: GPSEvent) -> float:
    return y.altitude_ellipsoid.component


# Tx-Rx 2D distance (m)
def distance_2d(y: GPSEvent) -> float:
    coords_rx = (latitude(y), longitude(y))
    coords_tx = (latitude(tx), longitude(tx))
    return distance.distance(coords_tx, coords_rx).m


# Tx-Rx 3D distance (m)
def distance_3d(y: GPSEvent) -> float:
    alt_rx, alt_tx = altitude(y), altitude(tx)
    return np.sqrt(np.square(distance_2d(y)) + np.square(alt_tx - alt_rx))


# USGS EPQS: Tx/Rx elevation (m)
def elevation(y: GPSEvent) -> float:
    elev_url, elev_val = '', 0.0
    lat, lon, alt = latitude(y), longitude(y), altitude(y)
    base_epqs_url = ('https://epqs.nationalmap.gov/v1/json?'
                     'x={}&y={}&units=Meters&wkid=4326&includeDate=False')

    while True:
        try:
            elev_url = base_epqs_url.format(lon, lat)
            elev_val = abs(alt - float(requests.get(elev_url).json()['value']))
        except KeyError as ke:
            print('SPAVE-28G | Consolidated Processing I | KeyError caught while getting elevation data '
                  'from URL: {} | Retrying... | Traceback: {}'.format(elev_url, traceback.print_tb(ke.__traceback__)))
            continue  # Retry querying the USGS EPQS URL for accurate elevation data...
        except JSONDecodeError as jde_:
            print('SPAVE-28G | Consolidated Processing I | JSONDecodeError caught while getting elevation data '
                  'from URL: {} | Retrying... | Traceback: {}'.format(elev_url, traceback.print_tb(jde_.__traceback__)))
            continue  # Retry querying the USGS EPQS URL for accurate elevation data...
        except Exception as e_:
            print('SPAVE-28G | Consolidated Processing I | Exception caught while getting elevation data '
                  'from URL: {} | Retrying... | Traceback: {}'.format(elev_url, traceback.print_tb(e_.__traceback__)))
            continue  # Retry querying the USGS EPQS URL for accurate elevation data...
        break

    return elev_val


# Cartesian coordinates to Spherical coordinates (x, y, z) -> (r, phi, theta) radians
def cart2sph(x: float, y: float, z: float) -> Tuple:
    return np.sqrt((x ** 2) + (y ** 2) + (z ** 2)), np.arctan2(y, x), np.arctan2(z, np.sqrt((x ** 2) + (y ** 2)))


# Spherical coordinates to Cartesian coordinates -> (r, phi, theta) radians -> (x, y, z)
def sph2cart(r: float, phi: float, theta: float) -> Tuple:
    return r * np.sin(theta) * np.cos(phi), r * np.sin(theta) * np.sin(phi), r * np.cos(theta)


# Process the power-delay-profiles recorded at the receiver
def process_rx_samples(x: np.array) -> Tuple:
    fs, ne_th = sample_rate, ne_amp_threshold
    f_pass, f_stop, d_pass, d_stop = prefilter_config.values()
    t_win_mul, t_trunc_len, t_trunc_mul = time_windowing_config.values()

    # Frequency Manipulation: Pre-filtering via a Low Pass Filter (LPF)
    b = fir_d.fir_remez_lpf(fs=fs, f_pass=f_pass, f_stop=f_stop, d_pass=d_pass, d_stop=d_stop)
    samps = signal.lfilter(b=b, a=1, x=x, axis=0)

    # Temporal Manipulation I: Temporal truncation
    samps = samps[t_trunc_len:] if samps.shape[0] > (t_trunc_mul * t_trunc_len) else samps

    # Temporal Manipulation II: Time-windowing
    window_size, n_samples = int(fs * t_win_mul), samps.shape[0]
    samps = samps[int(0.5 * window_size):int(1.5 * window_size)] if n_samples > 2 * window_size else samps

    # Noise Elimination: The peak search method is 'TallEnoughAbs'
    ne_samps = np.squeeze(samps[np.array(np.where(np.abs(samps) > ne_th * np.max(np.abs(samps))), dtype=int)])

    return ne_samps.shape[0], ne_samps


# Rx power computation (dB)
def compute_rx_power(n: int, x: np.array, is_calibration=False) -> float:
    fs = sample_rate

    # PSD Evaluation: Received signal power computation (calibration or campaign)
    pwr_values = np.square(np.abs(np.fft.fft(x))) / n
    freq_values = np.fft.fftfreq(n, (1 / fs))
    indices = np.argsort(freq_values)

    # Trapezoidal numerical integration to compute signal power at the Rx from the organized PSD data
    computed_rx_power = integrate.trapz(y=pwr_values[indices], x=freq_values[indices])

    if is_calibration:
        return decibel_1(computed_rx_power) - rx_usrp_gain if (computed_rx_power != 0.0) else -np.inf
    else:
        return cali_fit(decibel_1(computed_rx_power) - rx_usrp_gain) if (computed_rx_power != 0.0) else -np.inf


# Rx power getter (dB)
def rx_power(y: Pod) -> float:
    return y.rx_power


# Antenna gain computation (dB)
def compute_antenna_gain(y: GPSEvent, m: IMUTrace, is_tx=True) -> float:
    az0, el0 = deg2rad(yaw(m)), deg2rad(pitch(m))
    rx_lat, rx_lon, rx_alt = latitude(y), longitude(y), altitude(y)
    tx_lat, tx_lon, tx_alt = latitude(tx), longitude(tx), altitude(tx)

    rx_e, rx_n = lla_utm_proj(rx_lon, rx_lat)
    tx_e, tx_n = lla_utm_proj(tx_lon, tx_lat)

    if is_tx:
        x0, y0, z0, x, y, z = tx_n, -tx_e, tx_alt, rx_n, -rx_e, rx_alt
    else:
        x0, y0, z0, x, y, z = rx_n, -rx_e, rx_alt, tx_n, -tx_e, tx_alt

    r1, az1, el1 = cart2sph(x - x0, y - y0, z - z0)

    r21, r22, r23 = 0.0, 1.0, 0.0
    r11, r12, r13 = sph2cart(0.0, 1.0, -el0)
    r31, r32, r33 = sph2cart(0.0, 1.0, -el0 + (pi / 2.0))

    x1, y1, z1 = sph2cart(r1, az1 - az0, el1)
    f_arr = np.matmul(np.array([x1, y1, z1]), np.array([[r11, r12, r13], [r21, r22, r23], [r31, r32, r33]]))

    az, el = rad2deg(f_arr[0]), rad2deg(f_arr[2])
    xs, ys, zs = sph2cart(1.0, deg2rad(az), deg2rad(el))
    azs0 = np.mod(rad2deg(np.arctan2(np.sign(ys) * np.sqrt(1 - np.square(xs)), xs)), 360.0)
    els0 = np.mod(rad2deg(np.arctan2(np.sign(zs) * np.sqrt(1 - np.square(xs)), xs)), 360.0)

    az_amps_norm_db = az_amps_db - max(az_amps_db) + max_ant_gain
    el_amps_norm_db = el_amps_db - max(el_amps_db) + max_ant_gain
    az_amps_norm = np.array([linear_1(db) for db in az_amps_norm_db])
    el_amps_norm = np.array([linear_1(db) for db in el_amps_norm_db])

    az_amps0 = interp1d(az_angles, az_amps_norm)(azs0)
    el_amps0 = interp1d(el_angles, el_amps_norm)(els0)

    return decibel_1(((az_amps0 * abs(ys)) + (el_amps0 * abs(zs))) / (abs(ys) + abs(zs)))


# Tx antenna gain getter (dB)
def tx_ant_gain(y: Pod) -> float:
    return y.tx_ant_gain


# Rx antenna gain getter (dB)
def rx_ant_gain(y: Pod) -> float:
    return y.rx_ant_gain


# SPAVE-28G/Odin: Compute pathlosses evaluated from the collected measurements (dB)
def pathloss_spave28g_odin(t_gain: float, r_gain: float, rx_pwr: float) -> float:
    return tx_pwr + t_gain + uconv_gain + r_gain + dconv_gain - rx_pwr


# See PL-Models-II: 3GPP TR38.901 UMa | [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=7999294] (dB)
def pathloss_uma_3gpp_tr38901(y: GPSEvent) -> float:
    pl_los, pl_nlos = 0.0, 0.0
    h_ue, h_bs, f_c = elevation(y), elevation(tx), carrier_freq

    f_c_, h_ue_, h_bs_ = f_c / 1e9, h_ue - 1.0, h_bs - 1.0
    d_bp, d_2d, d_3d = 4 * h_ue_ * h_bs_ * f_c_ * (1e9 / c), distance_2d(y), distance_3d(y)

    if 10.0 <= d_2d <= d_bp:
        pl_los = 28.0 + (22.0 * np.log10(d_3d)) + (20.0 * np.log10(f_c_))
    elif d_bp <= d_2d <= 5e3:
        pl_los = 28.0 + (40.0 * np.log10(d_3d)) + (20.0 * np.log10(f_c_)) - (9.0 * np.log10(np.square(d_bp) +
                                                                                            np.square(h_ue - h_bs)))

    if 10.0 < d_2d < 5e3:
        pl_nlos = 13.54 + (39.08 * np.log10(d_3d)) + (20.0 * np.log10(f_c_)) - (0.6 * (h_ue - 1.5))

    return max(pl_los, pl_nlos) if pl_los != 0.0 or pl_nlos != 0.0 else np.nan


# See PL-Models-II: ITU-R M.2135 UMa | [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=7999294] (dB)
def pathloss_uma_itur_m2135(y: GPSEvent) -> float:
    pl_los, pl_nlos = 0.0, 0.0
    h, w, h_ue, h_bs, f_c = h_avg, w_avg, elevation(y), elevation(tx), carrier_freq
    d_bp, d_2d, d_3d = 2 * pi * h_ue * h_bs * (f_c / c), distance_2d(y), distance_3d(y)

    if 10.0 < d_2d < d_bp:
        pl_los = (np.log10(d_3d) * min(0.03 * (h ** 1.72), 10.0)) + \
                 (20.0 * np.log10(40.0 * pi * d_3d * (f_c / 3e9))) + \
                 (0.002 * np.log10(h) * d_3d) - min(0.044 * (h ** 1.72), 14.77)
    elif d_bp < d_2d < 10e3:
        pl_los = (np.log10(d_3d) * min(0.03 * (h ** 1.72), 10.0)) + \
                 (20.0 * np.log10(40.0 * pi * d_3d * (f_c / 3e9))) + \
                 (0.002 * np.log10(h) * d_3d) - min(0.044 * (h ** 1.72), 14.77) + (40.0 * np.log10(d_3d / d_bp))

    if 10.0 < d_2d < 5e3:
        pl_nlos = 161.04 - (7.1 * np.log10(w)) + (7.5 * np.log10(h)) - \
                  ((24.37 - (3.7 * np.square(h / h_bs))) * np.log10(h_bs)) + \
                  ((43.42 - (3.1 * np.log10(h_bs))) * (np.log10(d_3d) - 3.0)) + \
                  (20.0 * np.log10(f_c / 1e9)) - ((3.2 * np.square(np.log10(11.75 * h_ue))) - 4.97)

    return max(pl_los, pl_nlos) if pl_los != 0.0 or pl_nlos != 0.0 else np.nan


# See PL-Models-II: METIS Street-Canyon UMi | [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=7999294] (dB)
def pathloss_umi_metis(y: GPSEvent) -> float:
    pl_los, pl_nlos = 0.0, 0.0
    h_ue, h_bs, f_c = elevation(y), elevation(tx), carrier_freq
    h_ue_, h_bs_, f_c_, d_2d, d_3d = h_ue - 1.0, h_bs - 1.0, f_c / 1e9, distance_2d(y), distance_3d(y)

    pl_0 = -1.38 * np.log10(f_c_) + 3.34
    d_bp = 0.87 * np.exp(-np.log10(f_c_) / 0.65) * ((4.0 * h_ue_ * h_bs_) / (c / f_c))

    pl_1 = (22.0 * np.log10(d_3d)) + 28.0 + (20.0 * np.log10(f_c_)) + pl_0
    pl_1_func = lambda _d: (22.0 * np.log10(_d)) + 28.0 + (20.0 * np.log10(f_c_)) + pl_0
    pl_2 = (40.0 * np.log10(d_3d)) + 7.8 - (18.0 * np.log10(h_bs * h_ue)) + (2.0 * np.log10(f_c_)) + pl_1_func(d_bp)

    if 10.0 < d_2d < 5e3:
        if 10.0 < d_3d <= d_bp:
            pl_los = pl_1
        elif d_bp < d_3d <= 5e2:
            pl_los = pl_2

    if 10.0 < d_2d < 2e3:
        pl_nlos = (36.7 * np.log10(d_3d)) + 23.15 + (26.0 * np.log10(f_c_)) - (0.3 * h_ue)

    return max(pl_los, pl_nlos) if pl_los != 0.0 or pl_nlos != 0.0 else np.nan


# See PL-Models-II: mmMAGIC Street-Canyon UMi | [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=7999294] (dB)
def pathloss_umi_mm_magic(y: GPSEvent) -> float:
    f_c_, d_3d = carrier_freq / 1e9, distance_3d(y)
    pl_los = (19.2 * np.log10(d_3d)) + 32.9 + (20.8 * np.log10(f_c_))
    pl_nlos = (45.0 * np.log10(d_3d)) + 31.0 + (20.0 * np.log10(f_c_))
    return max(pl_los, pl_nlos) if pl_los != 0.0 or pl_nlos != 0.0 else np.nan


# SPAVE-28G/Odin: Pathloss getter (dB)
def pathloss(y: Pod) -> float:
    return y.pathloss[0]


# Fit a linear curve to pathloss data for empirical shadow fading evaluations
def linear_fit(x: np.array, y: np.array, x_log: bool = True) -> Tuple[float]:
    return np.polyfit(np.log10(x), y, deg=1) if x_log else np.polyfit(x, y, deg=1)


# Get the y_arr corresponding to the x_arr based on the provided linear model
def get_linear_fit_values(slope: float, intercept: float, x_arr: np.array, x_log: bool = True) -> np.array:
    if x_log:
        model = lambda _x: slope * np.log10(_x) + intercept
    else:
        model = lambda _x: slope * _x + intercept

    model_func = np.vectorize(model)
    return model_func(x_arr)


"""
CORE OPERATIONS I: Calibration
"""

# Evaluate parsed_metadata | Extract power-delay profile samples | Compute received power in this calibration paradigm
for att_val in att_indices:
    cali_samples_file = ''.join([cali_dir, cali_samples_file_left, 'a', str(att_val), cali_samples_file_right])
    with open(''.join([cali_dir, cali_metadata_file_left, 'a', str(att_val), cali_metadata_file_right])) as file:
        for line_num, line in enumerate(file):
            if (line_num - 11) % 18 == 0:
                num_samples = int(re.search(r'\d+', line)[0])
                cali_samples = np.fromfile(cali_samples_file,
                                           count=num_samples, dtype=np.csingle)
                num_samples, processed_rx_samples = process_rx_samples(cali_samples)
                meas_pwrs.append(compute_rx_power(num_samples, processed_rx_samples, True))
                break

# 1D interpolation of the measured and calculated powers for curve fitting
calc_pwrs_ = np.arange(start=0.0, stop=-82.0, step=-2.0)
meas_pwrs_ = interp1d(calc_pwrs, meas_pwrs, fill_value='extrapolate')(calc_pwrs_)
cali_fit = lambda mpwr: calc_pwrs_[min([_ for _ in range(len(meas_pwrs_))], key=lambda x: abs(mpwr - meas_pwrs_[x]))]

"""
CORE OPERATIONS II: Antenna patterns
"""

log = scipy.io.loadmat(ant_log_file)
az_log, el_log = log['pat28GAzNorm'], log['pat28GElNorm']
az_angles, az_amps = np.squeeze(az_log['azs'][0][0]), np.squeeze(az_log['amps'][0][0])
el_angles, el_amps = np.squeeze(el_log['els'][0][0]), np.squeeze(el_log['amps'][0][0])

az_amps_db, el_amps_db = decibel_1(az_amps), decibel_1(el_amps)

"""
CORE OPERATIONS III: Antenna gains, Received powers, and Pathloss computations
"""

# Extract gps_events (Rx only | Tx fixed on rooftop | V2I)
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(len(os.listdir(gps_dir))):
        filename = 'gps_event_{}.json'.format(i + 1)

        # noinspection PyBroadException
        try:
            parse(gps_events, GPSEvent, ''.join([gps_dir, filename]))
        except JSONDecodeError as jde:
            print('SPAVE-28G | Consolidated Processing I | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing I | Exception caught while parsing {}.'.format(filename))
            continue  # Ignore the Exception on this file | Move onto the next file...

# Extract Tx imu_traces
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(0, len(os.listdir(tx_imu_dir)), tx_imu_skip_step):
        filename = 'imu_trace_{}.json'.format(i + 1)

        # noinspection PyBroadException
        try:
            parse(tx_imu_traces, IMUTrace, ''.join([tx_imu_dir, filename]))
        except JSONDecodeError as jde:
            print('SPAVE-28G | Consolidated Processing I | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing I | Exception caught while parsing {}.'.format(filename))
            continue  # Ignore the Exception on this file | Move onto the next file...

# Extract Rx imu_traces
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(len(os.listdir(rx_imu_dir))):
        filename = 'imu_trace_{}.json'.format(i + 1)

        # noinspection PyBroadException
        try:
            parse(rx_imu_traces, IMUTrace, ''.join([rx_imu_dir, filename]))
        except JSONDecodeError as jde:
            print('SPAVE-28G | Consolidated Processing I | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing I | Exception caught while parsing {}.'.format(filename))
            continue  # Ignore the Exception on this file | Move onto the next file...

# Extract timestamp_0 (start_timestamp)
with open(''.join([comm_dir, start_timestamp_file])) as file:
    elements = file.readline().split()
    timestamp_0 = datetime.datetime.strptime(''.join([elements[2], ' ', elements[3]]), datetime_format)

''' Evaluate parsed_metadata | Extract power-delay profile samples '''

segment_done = False
timestamp_ref = datetime.datetime.strptime(gps_events[0].timestamp, datetime_format)

pdp_samples_file = ''.join([comm_dir, pdp_samples_file])
with open(''.join([comm_dir, parsed_metadata_file])) as file:
    for line_num, line in enumerate(file):
        if line_num % 18 == 0:
            seq_number = int(re.search(r'\d+', line)[0])
        elif (line_num - 3) % 18 == 0:
            # noinspection RegExpAnonymousGroup
            timestamp = timestamp_0 + datetime.timedelta(seconds=float(re.search(r'[+-]?\d+(\.\d+)?', line)[0]))
        elif (line_num - 11) % 18 == 0 and timestamp >= timestamp_ref:
            num_samples = int(re.search(r'\d+', line)[0])
            segment_done = True
        else:
            pass

        if segment_done:
            segment_done = False
            raw_rx_samples = np.fromfile(pdp_samples_file,
                                         offset=seq_number * num_samples, count=num_samples, dtype=np.csingle)

            if len(raw_rx_samples) == 0 or \
                    np.isnan(raw_rx_samples).any() or np.abs(np.min(raw_rx_samples)) > invalid_min_magnitude:
                continue

            num_samples, processed_rx_samples = process_rx_samples(raw_rx_samples)

            pdp_segments.append(PDPSegment(seq_number=seq_number + 1,
                                           timestamp=str(timestamp), num_samples=num_samples,
                                           raw_rx_samples=raw_rx_samples, processed_rx_samples=processed_rx_samples))

''' Match gps_event, imu_trace, and pdp_segment timestamps across both the Tx and the Rx realms '''

for gps_event in gps_events:
    seq_number, timestamp = gps_event.seq_number, gps_event.timestamp
    pdp_segment = min(pdp_segments, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                      datetime.datetime.strptime(x.timestamp, datetime_format)))
    tx_imu_trace = min(tx_imu_traces, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                        datetime.datetime.strptime(x.timestamp, datetime_format)))
    rx_imu_trace = min(rx_imu_traces, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                        datetime.datetime.strptime(x.timestamp, datetime_format)))

    tx_ant_gain = compute_antenna_gain(gps_event, tx_imu_trace)
    rx_ant_gain = compute_antenna_gain(gps_event, rx_imu_trace, False)
    rx_power_val = compute_rx_power(pdp_segment.num_samples, pdp_segment.processed_rx_samples)

    if rx_power_val == -np.inf:
        continue

    pathlosses = [pathloss_spave28g_odin(tx_ant_gain, rx_ant_gain, rx_power_val), pathloss_uma_3gpp_tr38901(gps_event),
                  pathloss_uma_itur_m2135(gps_event), pathloss_umi_mm_magic(gps_event), pathloss_umi_metis(gps_event)]

    pods.append(Pod(seq_number=seq_number, timestamp=timestamp,
                    gps_event=gps_event, pdp_segment=pdp_segment,
                    tx_imu_trace=tx_imu_trace, rx_imu_trace=rx_imu_trace, rx_power=rx_power_val,
                    tx_ant_gain=tx_ant_gain, rx_ant_gain=rx_ant_gain, elevation=elevation(gps_event),
                    distance_2d=distance_2d(gps_event), distance_3d=distance_3d(gps_event), pathloss=pathlosses))

"""
CORE VISUALIZATIONS I: 2D Azimuth & Elevation Antenna Patterns
"""

'''
ap2daz_url = plotly.plotly.plot(go.Figure(data=[go.Scatterpolar(r=az_amps_db, theta=az_angles, mode='lines+markers')]))
ap2del_url = plotly.plotly.plot(go.Figure(data=[go.Scatterpolar(r=el_amps_db, theta=el_angles, mode='lines+markers')]))

print('SPAVE-28G | Consolidated Processing I | 2D Azimuth Antenna Radiation Pattern Fig: {}.'.format(ap2daz_url))
print('SPAVE-28G | Consolidated Processing I | 2D Elevation Antenna Radiation Pattern Fig: {}.'.format(ap2del_url))
'''

"""
CORE VISUALIZATIONS II: Received power maps & Pathloss maps
"""

# rx_google_maps_options = GMapOptions(map_type=map_type, zoom=map_zoom_level, tilt=rx_tilt,
#                                      lat=latitude(map_central), lng=longitude(map_central))
# pl_google_maps_options = GMapOptions(map_type=map_type, zoom=map_zoom_level, tilt=pl_tilt,
#                                      lat=latitude(map_central), lng=longitude(map_central))

pl_kw, rx_kw, lat_kw, lon_kw = 'pathloss', 'rx-power', 'latitude', 'longitude'

rx_df = pd.DataFrame(data=[[latitude(x.gps_event),
                            longitude(x.gps_event), rx_power(x)] for x in pods], columns=[lat_kw, lon_kw, rx_kw])
pl_df = pd.DataFrame(data=[[latitude(x.gps_event),
                            longitude(x.gps_event), pathloss(x)] for x in pods], columns=[lat_kw, lon_kw, pl_kw])

rx_df.to_csv(rx_df_op_file)
pl_df.to_csv(pl_df_op_file)

# rx_color_mapper = LinearColorMapper(high=rx_df[rx_kw].min() - rx_offset,
#                                     low=rx_df[rx_kw].max() + rx_offset, palette=color_palette)
# pl_color_mapper = LinearColorMapper(low=pl_df[pl_kw].min() - pl_offset,
#                                     high=pl_df[pl_kw].max() + pl_offset, palette=color_palette)

# rx_ticks = [_x for _x in np.linspace(rx_df[rx_kw].min(), rx_df[rx_kw].max(), rx_tick_num)]
# pl_ticks = [_x for _x in np.linspace(pl_df[pl_kw].min(), pl_df[pl_kw].max(), pl_tick_num)]

# rx_color_bar = ColorBar(width=color_bar_width, height=color_bar_height,
#                         major_label_text_font_size=color_bar_label_size, ticker=FixedTicker(ticks=rx_ticks),
#                         color_mapper=rx_color_mapper, label_standoff=len(rx_ticks), orientation=color_bar_orientation)
# pl_color_bar = ColorBar(width=color_bar_width, height=color_bar_height,
#                         major_label_text_font_size=color_bar_label_size, ticker=FixedTicker(ticks=pl_ticks),
#                         color_mapper=pl_color_mapper, label_standoff=len(pl_ticks), orientation=color_bar_orientation)

# rx_figure = gmap(google_maps_api_key, rx_google_maps_options, width=map_width, height=map_height)
# pl_figure = gmap(google_maps_api_key, pl_google_maps_options, width=map_width, height=map_height)

# rx_figure.toolbar.logo = None
# rx_figure.toolbar_location = None
# rx_figure.xaxis.major_tick_line_color = None
# rx_figure.xaxis.minor_tick_line_color = None
# rx_figure.yaxis.major_tick_line_color = None
# rx_figure.yaxis.minor_tick_line_color = None
# rx_figure.xaxis.major_label_text_font_size = '0pt'
# rx_figure.yaxis.major_label_text_font_size = '0pt'

# pl_figure.toolbar.logo = None
# pl_figure.toolbar_location = None
# pl_figure.xaxis.major_tick_line_color = None
# pl_figure.xaxis.minor_tick_line_color = None
# pl_figure.yaxis.major_tick_line_color = None
# pl_figure.yaxis.minor_tick_line_color = None
# pl_figure.xaxis.major_label_text_font_size = '0pt'
# pl_figure.yaxis.major_label_text_font_size = '0pt'

# rx_figure.add_layout(rx_color_bar)
# pl_figure.add_layout(pl_color_bar)

# rx_figure.diamond('lon', 'lat', size=tx_pin_size, fill_color=tx_pin_color, fill_alpha=tx_pin_alpha,
#                   source=ColumnDataSource(data=dict(lat=[latitude(tx)], lon=[longitude(tx)])))
# pl_figure.diamond('lon', 'lat', size=tx_pin_size, fill_color=tx_pin_color, fill_alpha=tx_pin_alpha,
#                   source=ColumnDataSource(data=dict(lat=[latitude(tx)], lon=[longitude(tx)])))

# rx_figure.circle(lon_kw, lat_kw, size=rx_pins_size, alpha=rx_pins_alpha,
#                  color={'field': rx_kw, 'transform': rx_color_mapper}, source=ColumnDataSource(rx_df))
# pl_figure.circle(lon_kw, lat_kw, size=rx_pins_size, alpha=rx_pins_alpha,
#                  color={'field': pl_kw, 'transform': pl_color_mapper}, source=ColumnDataSource(pl_df))

# rx_fig_loc = ''.join([output_dir, pwr_png])
# pl_fig_loc = ''.join([output_dir, pl_png])

# export_png(rx_figure, filename=rx_fig_loc, width=map_width + 100, height=map_height + 100, timeout=timeout)
# export_png(pl_figure, filename=pl_fig_loc, width=map_width + 100, height=map_height + 100, timeout=timeout)

# print('SPAVE-28G | Consolidated Processing I | Received power map: {}.'.format(rx_fig_loc))
# print('SPAVE-28G | Consolidated Processing I | Pathloss map: {}.'.format(pl_fig_loc))

"""
CORE VISUALIZATIONS III: Pathloss v Distance curves
"""

pld_traces, pld_layout = [], dict(title='Pathloss v Distance',
                                  yaxis=dict(title='Pathloss (in dB)'), xaxis=dict(title='Tx-Rx Distance (in m)'))

for pl_pod in sorted(pods, key=lambda pod: pod.distance_3d):
    pls.append(pl_pod.pathloss)
    distns.append(pl_pod.distance_3d)

y_vals = np.array(pls)
x_vals = np.array(distns)
[pld_traces.append(go.Scatter(x=x_vals, mode='lines+markers',
                              name='PL Model: {}'.format(app.name),
                              y=signal.savgol_filter(y_vals[:, app.value],
                                                     sg_wsize, sg_poly_order))) for app in PathlossApproaches]

pld_url = plotly.plotly.plot(dict(data=pld_traces, layout=pld_layout))
print('SPAVE-28G | Consolidated Processing I | Pathloss v Distance Plot: {}.'.format(pld_url))
# pld_url = plotly.plotly.plot(dict(data=pld_traces, layout=pld_layout), filename=pl_dist_png)


"""
CORE VISUALIZATIONS IV: Path gain v Time for fast-fading evaluations
"""

ffe_pods = sorted(pods, key=lambda _pod: datetime.datetime.strptime(_pod.timestamp, datetime_format))
pod_zero = ffe_pods[0]

ffes = []
for _ffe_pod in ffe_pods:
    rxpower, pathgain = _ffe_pod.rx_power, -1 * _ffe_pod.pathloss[0]
    time_prog = (datetime.datetime.strptime(pod_zero.timestamp, datetime_format) -
                 datetime.datetime.strptime(_ffe_pod.timestamp, datetime_format)).seconds
    ffes.append((time_prog, rxpower, pathgain))

rxp_ffe_data = go.Scatter(x=[_ffes[0] for _ffes in ffes], mode='lines+markers',
                          y=signal.savgol_filter([_ffes[1] for _ffes in ffes], sg_wsize, sg_poly_order))
rxp_ffe_layout = dict(title='Rx Power v Time', yaxis=dict(title='Rx Power (in dB)'), xaxis=dict(title='Time (in s)'))

pg_ffe_data = go.Scatter(x=[_ffes[0] for _ffes in ffes], mode='lines+markers',
                         y=signal.savgol_filter([_ffes[2] for _ffes in ffes], sg_wsize, sg_poly_order))
pg_ffe_layout = dict(title='Pathgain v Time', yaxis=dict(title='Pathgain (in dB)'), xaxis=dict(title='Time (in s)'))

rxp_ffe_url = plotly.plotly.plot(dict(data=[rxp_ffe_data], layout=rxp_ffe_layout))
print('SPAVE-28G | Consolidated Processing I | Rx Power v Time Plot: {}.'.format(rxp_ffe_url))
# rxp_ffe_url = plotly.plotly.plot(dict(data=[rxp_ffe_data], layout=rxp_ffe_layout), filename=rxp_ffe_png)

pg_ffe_url = plotly.plotly.plot(dict(data=[pg_ffe_data], layout=pg_ffe_layout))
print('SPAVE-28G | Consolidated Processing I | Pathgain v Time Plot: {}.'.format(pg_ffe_url))
# pg_ffe_url = plotly.plotly.plot(dict(data=[pg_ffe_data], layout=pg_ffe_layout), filename=pg_ffe_png)

"""
CORE VISUALIZATIONS V: Empirical shadow fading evaluations
"""

route_distns, route_meas_pls = np.array(distns), np.array(pls)[:, 0]
fitted_model = linear_fit(route_distns, route_meas_pls)

route_fitted_pls = get_linear_fit_values(fitted_model[0], fitted_model[1], route_distns)
sfe_vals = np.subtract(route_meas_pls, route_fitted_pls)

# TO-DO: You may need to shift the mean to zero before fitting a Gaussian to the data...

mu, std = norm.fit(sfe_vals)
norm_x = np.linspace(np.min(sfe_vals), np.max(sfe_vals), sfe_norm_fit_num)

sfe_data = [go.Histogram(x=sfe_vals, histnorm='probability'),
            go.Scatter(x=norm_x, y=norm.pdf(norm_x, mu, std), mode='lines+markers')]

sfe_layout = dict(title='Shadowing Plot', xaxis=dict(title='Probability'), yaxis=dict(title='Shadow Fading (in dB)'))
sfe_url = plotly.plotly.plot(dict(data=[sfe_data], layout=sfe_layout))

print('SPAVE-28G | Consolidated Processing I | Mean: {} | STD: {} | Shadow Fading Plot: {}.'.format(mu, std, sfe_url))
# sfe_url = plotly.plotly.plot(dict(data=[sfe_data], layout=sfe_layout), filename=shadowing_png)
