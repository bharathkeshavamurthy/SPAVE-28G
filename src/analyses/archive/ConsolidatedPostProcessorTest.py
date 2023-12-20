"""
This script encapsulates the operations involved in estimating the propagation parameters associated with the various
Multi-Path Components (MPCs) in our 28 GHz outdoor measurement campaign on the POWDER testbed. It also describes the
visualizations of the RMS delay- & direction-spread characteristics obtained from this 28 GHz V2X channel modeling.
Furthermore, it includes spatial decoherence analyses w.r.t Tx-Rx distance, alignment, and relative velocity.

Additionally, as a part of our evaluations, we incorporate visualizations of the Power Delay Doppler Profiles (PDDPs),
the Power Delay Angular Profiles (PDAPs), the normalized Doppler spectrum, and the multi-path cluster characteristics.

Lastly, we analyze these results for the Saleh-Valenzuela (SV), Quasi-Deterministic (QD), and stochastic
channel models to empirically validate the correctness of such widely-used mmWave channel models <PENDING>.

Reference Papers:

@INPROCEEDINGS{SAGE,
  title={A sliding-correlator-based SAGE algorithm for mmWave wideband channel parameter estimation},
  author={Yin, Xuefeng and He, Yongyu and Song, Zinuo and Kim, Myung-Don and Chung, Hyun Kyu},
  booktitle={The 8th European Conference on Antennas and Propagation (EuCAP 2014)},
  year={2014}, pages={625-629}, doi={10.1109/EuCAP.2014.6901837}}.

@ARTICLE{Spatial-Consistency-I,
  title={Statistical channel impulse response models for factory and open plan building radio system design},
  author={Rappaport, T.S. and Seidel, S.Y. and Takamizawa, K.},
  journal={IEEE Transactions on Communications},
  pages={794-807}, doi={10.1109/26.87142},
  year={1991}, volume={39}, number={5}}.

@INPROCEEDINGS{Spatial-Consistency-II,
  author={Sun, Shu and Yan, Hangsong and MacCartney, George R. and Rappaport, Theodore S.},
  title={Millimeter wave small-scale spatial statistics in an urban microcell scenario},
  booktitle={2017 IEEE Int. Conf. on Commun. (ICC)},
  doi={10.1109/ICC.2017.7996408},
  pages={1-7}, year={2017}}.

@INPROCEEDINGS{PDDPs,
  title={28-GHz High-Speed Train Measurements and Propagation Characteristics Analysis},
  booktitle={2020 14th European Conference on Antennas and Propagation (EuCAP)},
  author={Park, Jae-Joon and Lee, Juyul and Kim, Kyung-Won and Kim, Myung-Don},
  year={2020}, pages={1-5}, doi={10.23919/EuCAP48036.2020.9135221}}.

@ARTICLE{PDAPs,
  title={Measurement-Based 5G mmWave Propagation Characterization in Vegetated Suburban Macrocell Environments},
  author={Zhang, Peize and Yang, Bensheng and Yi, Cheng and Wang, Haiming and You, Xiaohu},
  journal={IEEE Transactions on Antennas and Propagation},
  year={2020}, volume={68}, number={7}, pages={5556-5567},
  doi={10.1109/TAP.2020.2975365}}.

@ARTICLE{Channel-Models-I,
  author={Gustafson, Carl and Haneda, Katsuyuki and Wyne, Shurjeel and Tufvesson, Fredrik},
  title={On mm-Wave Multipath Clustering and Channel Modeling},
  journal={IEEE Transactions on Antennas and Propagation},
  year={2014}, volume={62}, number={3}, pages={1445-1455},
  doi={10.1109/TAP.2013.2295836}}

@INPROCEEDINGS{Channel-Models-II,
  author={Gustafson, Carl and Tufvesson, Fredrik and Wyne, Shurjeel and Haneda, Katsuyuki and Molisch, Andreas F.},
  title={Directional Analysis of Measured 60 GHz Indoor Radio Channels Using SAGE},
  booktitle={2011 IEEE 73rd Vehicular Technology Conference (VTC Spring)},
  year={2011}, volume={}, number={}, pages={1-5},
  doi={10.1109/VETECS.2011.5956639}}

@INPROCEEDINGS{Channel-Models-III,
  author={Lecci, Mattia and Polese, Michele and Lai, Chiehping and Wang, Jian and Gentile, Camillo and Golmie, et al.},
  title={Quasi-Deterministic Channel Model for mmWaves: Mathematical Formalization and Validation},
  booktitle={GLOBECOM 2020 - 2020 IEEE Global Communications Conference},
  year={2020}, pages={1-6}, doi={10.1109/GLOBECOM42002.2020.9322374}}

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

"""
INITIAL SETUP
"""

import warnings

# Ignore the Plotly Utils warning...
warnings.filterwarnings('ignore')

"""
IMPORTS
"""
import os
import re
import json
import plotly
import datetime
import scipy.io
import functools
# import requests
# import traceback
import dataclasses
import numpy as np
from enum import Enum
from pyproj import Proj
# from geopy import distance
from itertools import pairwise
import plotly.graph_objs as go
from json import JSONDecodeError
from scipy import signal, constants
from typing import Tuple, List, Dict
from scipy.interpolate import interp1d
from dataclasses import dataclass, field
import sk_dsp_comm.fir_design_helper as fir_d
# from itertools import pairwise, combinations
from concurrent.futures import ThreadPoolExecutor

"""
INITIALIZATIONS I: Collections & Utilities
"""
pi, c = np.pi, constants.c
# pi, c, distns, alignments, velocities = np.pi, constants.c, [], [], []
deg2rad, rad2deg = lambda x: x * (pi / 180.0), lambda x: x * (180.0 / pi)
decibel_1, linear_1 = lambda x: 10 * np.log10(x), lambda x: 10 ** (x / 10.0)
# linear_1, linear_2 = lambda x: 10 ** (x / 10.0), lambda x: 10 ** (x / 20.0)
rx_gps_events, tx_imu_traces, rx_imu_traces, pdp_segments, pods = [], [], [], [], []
# decibel_1, decibel_2, gamma = lambda x: 10 * np.log10(x), lambda x: 20 * np.log10(x), lambda fc, fc_: fc / (fc - fc_)

"""
INITIALIZATIONS II: Enumerations & Dataclasses (Inputs)
"""


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


"""
CONFIGURATIONS: A few route-specific Plotly visualization options
                Input & Output Dirs | GPS & IMU logs | Power delay profiles
"""

''' urban-campus-I route (semi-autonomous) (1400 E St) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-campus-I/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-campus-I/rx-realm/gps/'
# rx_imu_dir = 'E`:/SPAVE-28G/analyses/urban-campus-I/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-campus-I/tx-realm/imu/', 1
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'uc_rms_delay_spread.png', 'uc_aoa_rms_dir_spread.png'
# pdaps_png, pddps_png, doppler_spectrum_png = 'uc_pdaps.png', 'uc_pddps.png', 'uc_doppler_spectrum.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'uc_sc_dist.png', 'uc_sc_alignment.png', 'uc_sc_vel.png'
# pwrs_png, decay_chars_png, inter_arr_times_png = 'uc_pwrs.png', 'uc_decay_chars.png', 'uc_inter_arr_times.png'

''' urban-campus-II route (fully-autonomous) (President's Circle) '''
comm_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/pdp/'
rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/gps/'
rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/imu/'
tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-campus-II/tx-realm/imu/', 10
rms_delay_spread_png, aoa_rms_dir_spread_png = 'ucc_rms_delay_spread.png', 'ucc_aoa_rms_dir_spread.png'
pdaps_png, pddps_png, doppler_spectrum_png = 'ucc_pdaps.png', 'ucc_pddps.png', 'ucc_doppler_spectrum.png'
pwrs_png, decay_chars_png, inter_arr_times_png = 'ucc_pwrs.png', 'ucc_decay_chars.png', 'ucc_inter_arr_times.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'ucc_sc_dist.png', 'ucc_sc_alignment.png', 'ucc_sc_vel.png'

''' urban-campus-III route (fully-autonomous) (100 S St) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-campus-III/tx-realm/imu/', 5
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'uccc_rms_delay_spread.png', 'uccc_aoa_rms_dir_spread.png'
# pdaps_png, pddps_png, doppler_spectrum_png = 'uccc_pdaps.png', 'uccc_pddps.png', 'uccc_doppler_spectrum.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'uccc_sc_dist.png', 'uccc_sc_alignment.png', 'uccc_sc_vel.png'
# pwrs_png, decay_chars_png, inter_arr_times_png = 'uccc_pwrs.png', 'uccc_decay_chars.png', 'uccc_inter_arr_times.png'

''' urban-garage route (fully-autonomous) (NW Garage on 1460 E St) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-garage/tx-realm/imu/', 1
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'ug_rms_delay_spread.png', 'ug_aoa_rms_dir_spread.png'
# pdaps_png, pddps_png, doppler_spectrum_png = 'ug_pdaps.png', 'ug_pddps.png', 'ug_doppler_spectrum.png'
# pwrs_png, decay_chars_png, inter_arr_times_png = 'ug_pwrs.png', 'ug_decay_chars.png', 'ug_inter_arr_times.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'ug_sc_distance.png', 'ug_sc_alignment.png', 'ug_sc_velocity.png'

''' urban-stadium route (fully-autonomous) (E South Campus Dr) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-stadium/tx-realm/imu/', 5
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'us_rms_delay_spread.png', 'us_aoa_rms_dir_spread.png'
# pdaps_png, pddps_png, doppler_spectrum_png = 'us_pdaps.png', 'us_pddps.png', 'us_doppler_spectrum.png'
# pwrs_png, decay_chars_png, inter_arr_times_png = 'us_pwrs.png', 'us_decay_chars.png', 'us_inter_arr_times.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'us_sc_distance.png', 'us_sc_alignment.png', 'us_sc_velocity.png'

''' suburban-fraternities route (fully-autonomous) (S Wolcott St) '''
# comm_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/suburban-fraternities/tx-realm/imu/', 1
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'sf_rms_delay_spread.png', 'sf_aoa_rms_dir_spread.png'
# pdaps_png, pddps_png, doppler_spectrum_png = 'sf_pdaps.png', 'sf_pddps.png', 'sf_doppler_spectrum.png'
# pwrs_png, decay_chars_png, inter_arr_times_png = 'sf_pwrs.png', 'sf_decay_chars.png', 'sf_inter_arr_times.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'sf_sc_distance.png', 'sf_sc_alignment.png', 'sf_sc_velocity.png'

''' urban-vegetation route (fully-autonomous) (Olpin Union Bldg) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-vegetation/tx-realm/imu/', 1
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'uv_rms_delay_spread.png', 'uv_aoa_rms_dir_spread.png'
# pdaps_png, pddps_png, doppler_spectrum_png = 'uv_pdaps.png', 'uv_pddps.png', 'uv_doppler_spectrum.png'
# pwrs_png, decay_chars_png, inter_arr_times_png = 'uv_pwrs.png', 'uv_decay_chars.png', 'uv_inter_arr_times.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'uv_sc_distance.png', 'uv_sc_alignment.png', 'uv_sc_velocity.png'

''' Tx location fixed on the rooftop of the William Browning Building in SLC, UT '''
tx_gps_event = GPSEvent(latitude=Member(component=40.766173670),
                        longitude=Member(component=-111.847939330), altitude_ellipsoid=Member(component=1459.1210))

''' Generic configurations '''
tx_fc, rx_fc, wlength = 400e6, 399.95e6, c / 28e9
# output_dir = 'E:/Workspace/SPAVE-28G/test/analyses/'
lla_utm_proj = Proj(proj='utm', zone=32, ellps='WGS84')
mpc_delay_bins = np.arange(start=0, stop=400e-9, step=20e-9)
ne_amp_threshold, max_workers, sg_wsize, sg_poly_order = 0.05, 4096, 53, 3
min_threshold, sample_rate, datetime_format = 1e5, 2e6, '%Y-%m-%d %H:%M:%S.%f'
# d_max, d_step, a_max, a_step, v_max, v_step = 500.0, 1.0, 10.0, 0.05, 10.0, 0.1
delay_tol, doppler_tol, att_tol, aoa_az_tol, aoa_el_tol = 20e-9, 1e3, 0.5, 0.5, 0.5
n_sigma, max_ant_gain, max_mpcs, pn_v0, pn_l, pn_m = 0.015, 22.0, 20, 0.5, 11, 2047
ant_log_file, pn_reps = 'E:/SPAVE-28G/analyses/antenna_pattern.mat', int(pn_m / pn_l)
plotly.tools.set_credentials_file(username='total.academe', api_key='Xt5ic4JRgdvH8YuKmjEF')
tau_min, tau_max, nu_min, nu_max, phi_min, phi_max, the_min, the_max = 0, 400e-9, -5e3, 5e3, -pi, pi, -pi, pi
time_windowing_config = {'window_multiplier': 2.0, 'truncation_length': int(2e5), 'truncation_multiplier': 4.0}
pdp_samples_file, start_timestamp_file, parsed_metadata_file = 'samples.log', 'timestamp.log', 'parsed_metadata.log'
assert len(mpc_delay_bins) == max_mpcs, 'The max number of allowed MPCs must be equal to the delay bin quantization!'
# sc_dist_step, sc_align_step, sc_vel_step, mpc_delay_bins = 1.0, 0.05, 1.0, np.arange(start=0, stop=1e-6, step=1e-9)
prefilter_config = {'passband_freq': 60e3, 'stopband_freq': 65e3, 'passband_ripple': 0.01, 'stopband_attenuation': 80.0}

"""
INITIALIZATIONS III: Enumerations & Dataclasses (Temps | Outputs)
"""


@dataclass(order=True)
class MPCParameters:
    path_number: int = 0
    delay: float = 0.0  # s
    aoa_azimuth: float = 0.0  # rad
    aoa_elevation: float = 0.0  # rad
    doppler_shift: float = 0.0  # Hz
    profile_point_power: float = 0.0  # linear
    attenuation: float = complex(0.0, 0.0)  # linear (complex)


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
    # correlation_peak: float = 0.0  # linear


@dataclass(order=True)
class Pod:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    tx_gps_event: GPSEvent = GPSEvent()
    rx_gps_event: GPSEvent = GPSEvent()
    tx_imu_trace: IMUTrace = IMUTrace()
    rx_imu_trace: IMUTrace = IMUTrace()
    # tx_elevation: float = 0.0  # m
    # rx_elevation: float = 0.0  # m
    # tx_rx_alignment: float = 0.0  # deg
    # tx_rx_distance_2d: float = 0.0  # m
    # tx_rx_distance_3d: float = 0.0  # m
    pdp_segment: PDPSegment = PDPSegment()
    n_mpcs: int = max_mpcs
    mpc_parameters: List[MPCParameters] = field(default_factory=lambda: (MPCParameters() for _ in range(max_mpcs)))
    rms_delay_spread: float = 0.0  # s
    rms_aoa_dir_spread: float = 0.0  # no-units (normalized)


"""
CORE ROUTINES
"""


# Pack a dictionary into a dataclass instance
def ddc_transform(d: Dict, dc: dataclass) -> dataclass:
    d_l, d_f = {}, {f.name: f.type for f in dataclasses.fields(dc)}

    for k, v in d.items():
        # noinspection PyUnresolvedReferences
        d_l[k] = (lambda: v, lambda: ddc_transform(v, Member))[d_f[k] == Member]()

    return dc(**d_l)


# Parse the provided file and store it in the given collection
def parse(d: List, dc: dataclass, fn: str) -> None:
    with open(fn) as f:
        d.append(ddc_transform(json.load(f), dc))


"""
# Yaw angle getter (deg)
def yaw(m: IMUTrace) -> float:
    return m.yaw_angle
"""

"""
# Pitch angle getter (deg)
def pitch(m: IMUTrace) -> float:
    return m.pitch_angle
"""


# Latitude getter (deg)
def latitude(y: GPSEvent) -> float:
    return y.latitude.component


# Longitude getter (deg)
def longitude(y: GPSEvent) -> float:
    return y.longitude.component


# Altitude getter (m)
def altitude(y: GPSEvent) -> float:
    return y.altitude_ellipsoid.component


"""
# Tx-Rx 2D distance (m)
def tx_rx_distance_2d(tx: GPSEvent, rx: GPSEvent) -> float:
    coords_tx = (latitude(tx), longitude(tx))
    coords_rx = (latitude(rx), longitude(rx))
    return distance.distance(coords_tx, coords_rx).m
"""

"""
# Tx-Rx 3D distance (m)
def tx_rx_distance_3d(tx: GPSEvent, rx: GPSEvent) -> float:
    alt_tx, alt_rx = altitude(tx), altitude(rx)
    return np.sqrt(np.square(tx_rx_distance_2d(tx, rx)) + np.square(alt_tx - alt_rx))
"""

"""
# General 3D distance (m)
def distance_3d(y1: GPSEvent, y2: GPSEvent) -> float:
    coords_y1 = (latitude(y1), longitude(y1))
    coords_y2 = (latitude(y2), longitude(y2))
    alt_y1, alt_y2 = altitude(y1), altitude(y2)
    distance_2d = distance.distance(coords_y1, coords_y2).m
    return np.sqrt(np.square(distance_2d) + np.square(alt_y1 - alt_y2))
"""

"""
# Tx-Rx difference in alignment (deg)
def d_alignment(y1: GPSEvent, y2: GPSEvent, m: IMUTrace, is_tx=True) -> Tuple:
    y1_lat, y1_lon, y1_alt = latitude(y1), longitude(y1), altitude(y1)
    y2_lat, y2_lon, y2_alt = latitude(y2), longitude(y2), altitude(y2)

    '''
    Modeled from RxRealms.py - Utilities.py | principal_axes_positioning
    '''

    if is_tx:
        yaw_calc = rad2deg(np.arctan((y1_lat - y2_lat) / (np.cos(deg2rad(y1_lat)) * (y1_lon - y2_lon))))
    else:
        yaw_calc = rad2deg(np.arctan((y2_lat - y1_lat) / (np.cos(deg2rad(y2_lat)) * (y2_lon - y1_lon))))

    if y1_lat <= y2_lat:
        yaw_calc += 270.0 if yaw_calc >= 0.0 else 90.0
    else:
        yaw_calc += 90.0 if yaw_calc >= 0.0 else 270.0

    pitch_calc = rad2deg(np.arctan((y2_alt - y1_alt) / abs(np.cos(deg2rad(y1_lat)) * (y1_lon - y2_lon))))
    pitch_calc /= 2

    if is_tx:
        pitch_calc *= (pitch_calc * -30.0 if pitch_calc < 0.0 else 30.0) / (pitch_calc * pitch_calc)
    else:
        pitch_calc *= (pitch_calc * -5.0 if pitch_calc < 0.0 else 5.0) / (pitch_calc * pitch_calc)

    return abs(yaw(m) - yaw_calc), abs(pitch(m) - pitch_calc)
"""

"""
# Tx-Rx overall relative alignment accuracy (deg)
def tx_rx_alignment(tx: GPSEvent, rx: GPSEvent, m_tx: IMUTrace, m_rx: IMUTrace) -> float:
    m_tx_yaw_, m_tx_pitch_ = d_alignment(tx, rx, m_tx)
    m_rx_yaw_, m_rx_pitch_ = d_alignment(rx, tx, m_rx, False)
    return max(abs(180.0 - m_tx_yaw_ - m_rx_yaw_), abs(180.0 - m_tx_pitch_ - m_rx_pitch_))
"""

"""
# Tx-Rx relative velocity (ms-1) [N&W hemispheres only]
# TO-DO: Instead of using lat & long relative movements, use the Tx & Rx headings from the GPS logs.
def tx_rx_relative_velocity(tx: GPSEvent, rx_i: GPSEvent, rx_j: GPSEvent) -> float:
    rx_i_lat, rx_i_lon = latitude(rx_i), longitude(rx_i)
    rx_j_lat, rx_j_lon = latitude(rx_j), longitude(rx_j)
    rx_ds = distance.distance((rx_i_lat, rx_i_lon), (rx_j_lat, rx_j_lon)).m

    rx_i_dt = datetime.datetime.strptime(rx_i.timestamp, datetime_format)
    rx_j_dt = datetime.datetime.strptime(rx_j.timestamp, datetime_format)
    rx_dt = abs((rx_j_dt - rx_i_dt).seconds)

    if rx_dt == 0.0:
        return 0.0

    rx_v = rx_ds / rx_dt

    if tx_rx_distance_2d(tx, rx_i) > tx_rx_distance_2d(tx, rx_j):
        rx_v *= -1  # Multiply by -1 if the Rx is going towards the Tx...

    return rx_v
"""

"""
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
            print('SPAVE-28G | Consolidated Processing II | KeyError caught while getting elevation data '
                  'from URL: {} | Retrying... | Traceback: {}'.format(elev_url, traceback.print_tb(ke.__traceback__)))
            continue  # Retry querying the USGS EPQS URL for accurate elevation data...
        except JSONDecodeError as jde_:
            print('SPAVE-28G | Consolidated Processing II | JSONDecodeError caught while getting elevation data '
                  'from URL: {} | Retrying... | Traceback: {}'.format(elev_url, traceback.print_tb(jde_.__traceback__)))
            continue  # Retry querying the USGS EPQS URL for accurate elevation data...
        except Exception as e_:
            print('SPAVE-28G | Consolidated Processing II | Exception caught while getting elevation data '
                  'from URL: {} | Retrying... | Traceback: {}'.format(elev_url, traceback.print_tb(e_.__traceback__)))
            continue  # Retry querying the USGS EPQS URL for accurate elevation data...
        break

    return elev_val
"""


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


"""
# Correlation peak in the processed rx_samples (linear)
def correlation_peak(x: np.array) -> float:
    return np.max(np.abs(x))
"""


# RMS delay spread computation (std | s)
# See Visualizations-I: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6691924]
def rms_delay_spread(mpcs: List[MPCParameters]) -> float:
    num1, num2, den = [], [], []

    for mpc in mpcs:
        tau, p_tau = mpc.delay, mpc.profile_point_power
        num1.append(np.square(tau) * p_tau)
        num2.append(tau * p_tau)
        den.append(p_tau)

    num1_sum, num2_sum, den_sum = np.sum(num1), np.sum(num2), np.sum(den)

    return np.sqrt((num1_sum / den_sum) - np.square(num2_sum / den_sum))


# RMS AoA direction spread computation (std | no-units [normalized])
# See Visualizations-II: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=5956639]
def rms_aoa_direction_spread(mpcs: List[MPCParameters]) -> float:
    e_vecs, p_vec, mu_vec = [], [], []

    for _l_mpc in range(len(mpcs)):
        mpc = mpcs[_l_mpc]
        p = mpc.profile_point_power
        phi, theta = mpc.aoa_azimuth, mpc.aoa_elevation
        e_vecs.append(np.array([np.cos(phi) * np.sin(theta), np.sin(phi) * np.sin(theta), np.cos(theta)]))
        mu_vec.append(p * e_vecs[_l_mpc])
        p_vec.append(p)

    mu_omega = np.sum(mu_vec, axis=0)

    return np.sqrt(np.sum([np.square(np.linalg.norm(e_vecs[_l_mpc] -
                                                    mu_omega)) * p_vec[_l_mpc] for _l_mpc in range(len(mpcs))]))


# Empirical Cumulative Distribution Function (variant-I)
def ecdf(x: np.array) -> Tuple:
    x_, cts = np.unique(x, return_counts=True)

    cum_sum = np.cumsum(cts)
    return x_, cum_sum / cum_sum[-1]


# SAGE Algorithm: MPC parameters computation
# See: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=753729]
# Also see: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6901837]
def estimate_mpc_parameters(tx: GPSEvent, rx: GPSEvent, n: int, x: np.array) -> List:
    f_c, _f_c = tx_fc, rx_fc
    f_s, n_std = sample_rate, n_sigma
    first_iter, v_0, l_, k_ = False, pn_v0, pn_l, pn_reps

    y_f = np.fft.fft(x) / n
    fs = np.argsort(np.fft.fftfreq(n, (1 / f_s)))
    n_f = (n_std * np.random.randn(fs.shape[0], )).view(np.csingle)

    # Previous parameter estimates ...$[i-1]$
    nus = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    phis = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    taus = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    thetas = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    alphas = np.zeros(shape=(max_mpcs,), dtype=np.csingle)

    # Flip components in the rectangular chips of the PN-sequence $u(f)$
    def flip(a: int) -> int:
        assert a == 1 or a == -1
        return 1 if a == -1 else -1

    # Sinc function
    def sinc(z: float) -> float:
        return np.sin(pi * z) / (pi * z) if z != 0 else 1

    # Complex gaussian noise component post-convolution in the frequency domain $n'(f)$
    def noise_component() -> Tuple:
        f_shifts = []
        n_sum = complex(0.0, 0.0)
        f_idxs = [_ for _ in range(fs.shape[0])]
        for k in range(k_):
            for _k in range(k_):
                f_shift = ((f_c * k) + (_f_c * _k)) / l_
                idx_f = min(f_idxs, key=lambda idx: abs(f_shift - fs[idx]))

                pn_sum = complex(0.0, 0.0)
                for i_l in range(l_):
                    a_i = 1
                    pn_sum += ((2 * a_i) - 1) * complex(np.cos(-pi * ((_k * i) / l_)),
                                                        np.sin(-pi * ((_k * i) / l_)))
                    flip(a_i)

                f_shifts.append(f_shift)
                n_sum += pn_sum * n_f[idx_f] * sinc(k / l_) * complex(np.cos(-pi * (_k / l_)),
                                                                      np.sin(-pi * (_k / l_)))

        return f_shifts, (v_0 / l_) * n_sum

    # Baseband signal component post-convolution in the frequency domain $p(f; \tau_{l}, \nu_{l})$
    def signal_component(tau_l: float, nu_l: float) -> Tuple:
        f_shifts = []
        sig_sum = complex(0.0, 0.0)
        for k in range(k_):
            for _k in range(k_):
                f_shift = (((f_c * k) + (_f_c * _k)) / l_) + nu_l
                c_theta = -((2.0 * pi * f_c * (k / l_) * tau_l) + (pi * ((k + _k) / l_)))

                pn_sum = complex(0.0, 0.0)
                for i_l in range(l_):
                    a_i = 1
                    for _i_l in range(l_):
                        _a_i = 1
                        _c_theta = -(pi * (((k * i_l) + (_k * _i_l)) / l_))
                        pn_sum += ((2 * a_i) - 1) * ((2 * _a_i) - 1) * complex(np.cos(_c_theta), np.sin(_c_theta))
                        flip(_a_i)
                    flip(a_i)

                f_shifts.append(f_shift)
                sig_sum += pn_sum * sinc(k / l_) * sinc(_k / l_) * complex(np.cos(c_theta), np.sin(c_theta))

        return f_shifts, np.square(v_0 / l_) * sig_sum

    # Current parameter estimates ...$[i]$
    nus_ = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    phis_ = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    taus_ = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    thetas_ = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    alphas_ = np.zeros(shape=(max_mpcs,), dtype=np.csingle)
    nu_, tau_, phi_, theta_, alpha_ = nus_[0], taus_[0], phis_[0], thetas_[0], alphas_[0]
    w_matrix = np.linalg.inv(np.diag(np.full(fs.shape[0], np.mean(np.square(np.abs(noise_component()[1]))))))

    # See [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=753729]
    # Computing the steering vector [Angle-of-Arrival (AoA | azimuth & elevation)] at the Rx within SAGE
    def compute_steering(phi_l: float, theta_l: float) -> complex:
        az0, el0 = deg2rad(phi_l), deg2rad(theta_l)
        tx_lat, tx_lon, tx_alt = latitude(tx), longitude(tx), altitude(tx)
        rx_lat, rx_lon, rx_alt = latitude(rx), longitude(rx), altitude(rx)

        tx_e, tx_n = lla_utm_proj(tx_lon, tx_lat)
        rx_e, rx_n = lla_utm_proj(rx_lon, rx_lat)

        x0, y0, z0, x_, y_, z_ = rx_n, -rx_e, rx_alt, tx_n, -tx_e, tx_alt

        r1, az1, el1 = cart2sph(x_ - x0, y_ - y0, z_ - z0)

        r21, r22, r23 = 0.0, 1.0, 0.0
        r11, r12, r13 = sph2cart(0.0, 1.0, -el0)
        r31, r32, r33 = sph2cart(0.0, 1.0, -el0 + (pi / 2.0))

        x1, y1, z1 = sph2cart(r1, az1 - az0, el1)
        f_arr = np.matmul(np.array([x1, y1, z1]), np.array([[r11, r12, r13], [r21, r22, r23], [r31, r32, r33]]))

        az, el = rad2deg(f_arr[0]), rad2deg(f_arr[2])
        xs, ys, zs = sph2cart(1.0, deg2rad(az), deg2rad(el))
        az_amps_db, el_amps_db = decibel_1(az_amps), decibel_1(el_amps)
        azs0 = np.mod(rad2deg(np.arctan2(np.sign(ys) * np.sqrt(1 - np.square(xs)), xs)), 360.0)
        els0 = np.mod(rad2deg(np.arctan2(np.sign(zs) * np.sqrt(1 - np.square(xs)), xs)), 360.0)

        az_amps_norm_db = az_amps_db - max(az_amps_db) + max_ant_gain
        el_amps_norm_db = el_amps_db - max(el_amps_db) + max_ant_gain
        az_amps_norm = np.array([linear_1(db) for db in az_amps_norm_db])
        el_amps_norm = np.array([linear_1(db) for db in el_amps_norm_db])

        az_amps0 = interp1d(az_angles, az_amps_norm)(azs0)
        el_amps0 = interp1d(el_angles, el_amps_norm)(els0)

        ld, pos_vec = wlength, np.array([xs, ys, zs])
        f_aoa = ((az_amps0 * abs(ys)) + (el_amps0 * abs(zs))) / (abs(ys) + abs(zs))
        e_vec = np.array([np.cos(phi_l) * np.sin(theta_l), np.sin(phi_l) * np.sin(theta_l), np.cos(theta_l)])

        c_theta = 2.0 * pi * (1 / ld) * np.dot(e_vec, pos_vec)
        return f_aoa * complex(np.cos(c_theta), np.sin(c_theta))

    # Find the profile point power associated with the MPCParameters index (linear)
    def profile_point_power(l_idx: int) -> float:
        steering_l = compute_steering(phis_[l_idx], thetas_[l_idx])
        tau_l, nu_l, alpha_l = taus_[l_idx], nus_[l_idx], alphas_[l_idx]
        return np.square(np.abs(alpha_l * steering_l * signal_component(tau_l, nu_l)[1]))

    # Expectation of the log-likelihood component: Use $[i-1]$ estimates, i.e., phis, thetas, alphas, taus, nus
    def estep(l_idx: int, yf_s: np.array) -> np.array:
        return yf_s - np.sum(np.array([compute_steering(phis[_l_mpc], thetas[_l_mpc]) *
                                       alphas[_l_mpc] * signal_component(taus[_l_mpc], nus[_l_mpc])[1]
                                       for _l_mpc in range(max_mpcs) if _l_mpc != l_idx], dtype=np.csingle))

    # Maximization step for the MPC delay & Doppler shift $\argmax_{\tau,\nu}\{\eta_(\tau,\nu)\} (s, Hz)
    def tau_nu_mstep(l_idx: int) -> Tuple:
        args, estep_comps, tau_step, nu_step = [], estep(l_idx, y_f), delay_tol, doppler_tol

        for tau_var in np.arange(start=tau_min, stop=tau_max, step=tau_step):
            for nu_var in np.arange(start=nu_min, stop=nu_max + nu_step, step=nu_step):
                sig_comp = signal_component(tau_var, nu_var)[1]
                sig_comps = np.array([sig_comp for _ in fs], dtype=np.csingle)

                numerator = np.square(np.abs(sig_comps.conj().T @ w_matrix @ estep_comps))
                denominator = np.abs(sig_comps.conj().T @ w_matrix @ sig_comps)

                args.append([tau_var, nu_var, numerator / denominator])

        max_idx = max([_ for _ in range(len(args))], key=lambda _idx: args[_idx][2])

        return args[max_idx][0], args[max_idx][1]

    # Maximization step for the MPC AoA azimuth and AoA elevation (rad, rad)
    def phi_theta_mstep(l_idx: int, tau_l: float, nu_l: float, alpha_l: complex) -> Tuple:
        phi_step, theta_step = aoa_az_tol, aoa_el_tol
        args, estep_comps, sig_comp = [], estep(l_idx, y_f), signal_component(tau_l, nu_l)[1]

        for phi_var in np.arange(start=phi_min, stop=phi_max + phi_step, step=phi_step):
            for theta_var in np.arange(start=the_min, stop=the_max + theta_step, step=theta_step):
                steering_comp = compute_steering(phi_var, theta_var)
                sig_comps = steering_comp * alpha_l * np.array([sig_comp for _ in fs], dtype=np.csingle)

                numerator = np.square(np.abs(sig_comps.conj().T @ w_matrix @ estep_comps))
                denominator = np.abs(sig_comps.conj().T @ w_matrix @ sig_comps)

                args.append([phi_var, theta_var, numerator / denominator])

        max_idx = max([_ for _ in range(len(args))], key=lambda _idx: args[_idx][2])

        return args[max_idx][0], args[max_idx][1]

    # Maximization step for the MPC complex attenuation (linear [complex])
    def alpha_mstep(l_idx: int, tau_l: float, nu_l: float, phi_l: float, theta_l: float) -> complex:
        estep_comps = estep(l_idx, y_f)
        sig_comp = signal_component(tau_l, nu_l)[1]
        steering_comp = compute_steering(phi_l, theta_l)
        sig_comps = steering_comp * np.array([sig_comp for _ in fs], dtype=np.csingle)

        numerator = functools.reduce(np.matmul, [sig_comps.conj().T, w_matrix, estep_comps])
        denominator = functools.reduce(np.matmul, [sig_comps.conj().T, w_matrix, sig_comps])

        return numerator / denominator if denominator != 0 else 0

    # Convergence check
    def is_converged(l_idx) -> bool:
        check = lambda param, param_, tol: np.abs(param - param_) > tol
        tau_tol, nu_tol, alpha_tol, phi_tol, theta_tol = delay_tol, doppler_tol, att_tol, aoa_az_tol, aoa_el_tol

        if first_iter or \
                check(nus[l_idx], nus_[l_idx], nu_tol) or \
                check(taus[l_idx], taus_[l_idx], tau_tol) or \
                check(alphas[l_idx], alphas_[l_idx], alpha_tol) or \
                check(phis[l_idx], phis_[l_idx], phi_tol) or check(thetas[l_idx], thetas_[l_idx], theta_tol):
            return False

        return True

    # SAGE wrapper
    for l_mpc in range(max_mpcs):
        first_iter = True

        while not is_converged(l_mpc):
            first_iter = False

            # Old assignments
            nus[l_mpc] = nu_
            taus[l_mpc] = tau_
            phis[l_mpc] = phi_
            thetas[l_mpc] = theta_
            alphas[l_mpc] = alpha_

            tau_, nu_ = tau_nu_mstep(l_mpc)
            taus_[l_mpc] = tau_  # Delay new assignment
            nus_[l_mpc] = nu_  # Doppler new assignment

            phi_, theta_ = phi_theta_mstep(l_mpc, tau_, nu_, alphas[l_mpc])
            thetas_[l_mpc] = theta_  # AoA El new assignment
            phis_[l_mpc] = phi_  # AoA Az new assignment

            alpha_ = alpha_mstep(l_mpc, tau_, nu_, phi_, theta_)
            alphas_[l_mpc] = alpha_  # Att new assignment

    return [MPCParameters(path_number=l_mpc, delay=taus_[l_mpc],
                          doppler_shift=nus_[l_mpc], attenuation=alphas_[l_mpc],
                          aoa_azimuth=phis_[l_mpc], aoa_elevation=thetas_[l_mpc],
                          profile_point_power=profile_point_power(l_mpc)) for l_mpc in range(max_mpcs)]


"""
CORE OPERATIONS: Parsing the GPS, IMU, and PDP logs | SAGE estimation | Spatial consistency analyses
"""

# Antenna patterns
# noinspection PyUnresolvedReferences
log = scipy.io.loadmat(ant_log_file)
az_log, el_log = log['pat28GAzNorm'], log['pat28GElNorm']
az_angles, az_amps = np.squeeze(az_log['azs'][0][0]), np.squeeze(az_log['amps'][0][0])
el_angles, el_amps = np.squeeze(el_log['els'][0][0]), np.squeeze(el_log['amps'][0][0])

# Extract Rx gps_events (Tx fixed on rooftop | V2I)
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(len(os.listdir(rx_gps_dir))):
        filename = 'gps_event_{}.json'.format(i + 1)

        # noinspection PyBroadException
        try:
            parse(rx_gps_events, GPSEvent, ''.join([rx_gps_dir, filename]))
        except JSONDecodeError as jde:
            print('SPAVE-28G | Consolidated Processing II | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing II | Exception caught while parsing {}.'.format(filename))
            continue  # Ignore the Exception on this file | Move onto the next file...

# Extract Tx imu_traces
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(0, len(os.listdir(tx_imu_dir)), tx_imu_skip_step):
        filename = 'imu_trace_{}.json'.format(i + 1)

        # noinspection PyBroadException
        try:
            parse(tx_imu_traces, IMUTrace, ''.join([tx_imu_dir, filename]))
        except JSONDecodeError as jde:
            print('SPAVE-28G | Consolidated Processing II | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing II | Exception caught while parsing {}.'.format(filename))
            continue  # Ignore the Exception on this file | Move onto the next file...

# Extract Rx imu_traces
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(len(os.listdir(rx_imu_dir))):
        filename = 'imu_trace_{}.json'.format(i + 1)

        # noinspection PyBroadException
        try:
            parse(rx_imu_traces, IMUTrace, ''.join([rx_imu_dir, filename]))
        except JSONDecodeError as jde:
            print('SPAVE-28G | Consolidated Processing II | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing II | Exception caught while parsing {}.'.format(filename))
            continue  # Ignore the Exception on this file | Move onto the next file...

# Extract timestamp_0 (start_timestamp)
with open(''.join([comm_dir, start_timestamp_file])) as file:
    elements = file.readline().split()
    timestamp_0 = datetime.datetime.strptime(''.join([elements[2], ' ', elements[3]]), datetime_format)

''' Evaluate parsed_metadata | Extract power-delay profile samples '''

segment_done, pdp_samples_file = False, ''.join([comm_dir, pdp_samples_file])
timestamp_ref = datetime.datetime.strptime(rx_gps_events[0].timestamp, datetime_format)

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
                    np.isnan(raw_rx_samples).any() or np.abs(np.min(raw_rx_samples)) > min_threshold:
                continue

            num_samples, processed_rx_samples = process_rx_samples(raw_rx_samples)

            pdp_segments.append(PDPSegment(num_samples=num_samples,
                                           seq_number=seq_number + 1, timestamp=str(timestamp),
                                           raw_rx_samples=raw_rx_samples, processed_rx_samples=processed_rx_samples))

            # pdp_segments.append(PDPSegment(num_samples=num_samples,
            #                                seq_number=seq_number + 1, timestamp=str(timestamp),
            #                                correlation_peak=correlation_peak(processed_rx_samples),
            #                                raw_rx_samples=raw_rx_samples, processed_rx_samples=processed_rx_samples))

''' Match gps_event, imu_trace, and pdp_segment timestamps across both the Tx and the Rx realms '''

for seqnum in range(1, len(rx_gps_events)):
    rx_gps_event = rx_gps_events[seqnum]
    seq_number, timestamp = rx_gps_event.seq_number, rx_gps_event.timestamp

    pdp_segment = min(pdp_segments, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                      datetime.datetime.strptime(x.timestamp, datetime_format)))
    tx_imu_trace = min(tx_imu_traces, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                        datetime.datetime.strptime(x.timestamp, datetime_format)))
    rx_imu_trace = min(rx_imu_traces, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                        datetime.datetime.strptime(x.timestamp, datetime_format)))

    mpc_parameters = estimate_mpc_parameters(tx_gps_event, rx_gps_event,
                                             pdp_segment.num_samples, pdp_segment.processed_rx_samples)

    pods.append(Pod(seq_number=seq_number, timestamp=timestamp,
                    tx_gps_event=tx_gps_event, tx_imu_trace=IMUTrace(),
                    rx_gps_event=rx_gps_event, rx_imu_trace=IMUTrace(),
                    mpc_parameters=mpc_parameters, rms_delay_spread=rms_delay_spread(mpc_parameters),
                    rms_aoa_dir_spread=rms_aoa_direction_spread(mpc_parameters), pdp_segment=pdp_segment))

    # pods.append(Pod(seq_number=seq_number, timestamp=timestamp,
    #                 tx_gps_event=tx_gps_event, tx_imu_trace=IMUTrace(),
    #                 rx_gps_event=rx_gps_event, rx_imu_trace=IMUTrace(),
    #                 rms_aoa_dir_spread=rms_aoa_direction_spread(mpc_parameters),
    #                 tx_rx_distance_2d=tx_rx_distance_2d(tx_gps_event, rx_gps_event),
    #                 tx_elevation=elevation(tx_gps_event), rx_elevation=elevation(rx_gps_event),
    #                 mpc_parameters=mpc_parameters, rms_delay_spread=rms_delay_spread(mpc_parameters),
    #                 tx_rx_alignment=tx_rx_alignment(tx_gps_event, rx_gps_event, IMUTrace(), IMUTrace()),
    #                 pdp_segment=pdp_segment, tx_rx_distance_3d=tx_rx_distance_3d(tx_gps_event, rx_gps_event)))

"""
CORE VISUALIZATIONS I: Spatial decoherence analyses
                       Refer to the math in the manuscript for the underlying modeling...
"""

"""
''' Delay bin quantization '''

tsorted_pods = sorted(pods, key=lambda _pod: _pod.seq_num)
mpc_amps = [{_db: 0.0 for _db in mpc_delay_bins} for _pod in tsorted_pods]

for db_idx, db_val in enumerate(mpc_delay_bins):

    for db_pod_idx, db_pod_val in enumerate(tsorted_pods):
        db_min, db_max = 0 if db_idx == 0 else db_idx - 1, db_val

        if db_min <= db_pod_val < db_max:
            mpc_amps[db_pod_idx][db_val] = np.sqrt(db_pod_val.profile_point_power)
"""

"""
''' Separation variables bin quantization '''

nnve_proj = lambda _x: _x if _x >= 0 else 0
rel_vel = lambda _pod: tx_rx_relative_velocity(_pod.tx_gps_event,
                                               tsorted_pods[_pod.seq_number - 2].rx_gps_event,
                                               _pod.rx_gps_event) if _pod.seq_number != 1 else 0

pod_vels = [rel_vel(_pod) for _pod in tsorted_pods]
pod_aligns = [_pod.tx_rx_alignment for _pod in tsorted_pods]
pod_distns = [_pod.tx_rx_distance_3d for _pod in tsorted_pods]

vel_bins = np.arange(start=min(pod_vels), stop=max(pod_vels) + sc_vel_step, step=sc_vel_step)
distn_bins = np.arange(start=min(pod_distns), stop=max(pod_distns) + sc_dist_step, step=sc_dist_step)
align_bins = np.arange(start=min(pod_aligns), stop=max(pod_aligns) + sc_align_step, step=sc_align_step)
"""

"""
''' SAC utilities '''


# Map distance to a distance bin
def find_distn_bin(distn: float) -> float:
    for dn_idx, dn_val in enumerate(distn_bins):
        dn_min, dn_max = 0 if dn_idx == 0 else dn_idx - 1, dn_val
        if dn_min <= distn < dn_max:
            return dn_val


# Map alignment to an alignment bin
def find_align_bin(align: float) -> float:
    for an_idx, an_val in enumerate(align_bins):
        an_min, an_max = 0 if an_idx == 0 else an_idx - 1, an_val
        if an_min <= align < an_max:
            return an_val


# Map velocity to a velocity bin
def find_vel_bin(vel: float) -> float:
    for vl_idx, vl_val in enumerate(vel_bins):
        vl_min, vl_max = 0 if vl_idx == 0 else vl_idx - 1, vl_val
        if vl_min <= vel < vl_max:
            return vl_val


# Find sample mean of MPC amplitudes
def amp_samp_mean(d_bin: float, distn_bin: float, align_bin: float, vel_bin: float) -> float:
    vel_bin_min, vel_bin_max = nnve_proj(vel_bin - sc_vel_step), vel_bin
    distn_bin_min, distn_bin_max = nnve_proj(distn_bin - sc_dist_step), distn_bin
    align_bin_min, align_bin_max = nnve_proj(align_bin - sc_align_step), align_bin

    return np.mean([mpc_amps[_idx][d_bin] for _idx, _pod in enumerate(tsorted_pods)
                    if (distn_bin_min <= _pod.tx_rx_distance_3d < distn_bin_max)
                    and (align_bin_min <= _pod.tx_rx_alignment < align_bin_max)
                    and (vel_bin_min <= rel_vel(_pod) <= vel_bin_max)])


inner_sums_den = []
# Common SAC denominator block
for ts_idx, ts_pod in enumerate(tsorted_pods):
    inner_sum_den, vl_bin_den = 0.0, find_vel_bin(rel_vel(ts_pod))
    dn_bin_den, an_bin_den = find_distn_bin(ts_pod.tx_rx_distance_3d), find_align_bin(ts_pod.tx_rx_alignment)

    for db_val_den in mpc_delay_bins:
        inner_sum_den += np.square(mpc_amps[ts_idx][db_val_den] - amp_samp_mean(db_val_den, dn_bin_den,
                                                                                an_bin_den, vl_bin_den))

    inner_sums_den.append(inner_sum_den)

s_coeff_den = np.mean(inner_sums_den)


# SAC routine
def s_coeff(eval_set: np.array) -> float:
    inner_sums_num = []

    for e_idx, e_pair in enumerate(eval_set):
        pod_0, pod_1 = e_pair

        inner_sum_num, vl_bin_num_0, vl_bin_num_1 = 0.0, find_vel_bin(rel_vel(pod_0)), find_vel_bin(rel_vel(pod_1))
        dn_bin_num_0, an_bin_num_0 = find_distn_bin(pod_0.tx_rx_distance_3d), find_align_bin(pod_0.tx_rx_alignment)
        dn_bin_num_1, an_bin_num_1 = find_distn_bin(pod_1.tx_rx_distance_3d), find_align_bin(pod_1.tx_rx_alignment)

        for db_val_num in mpc_delay_bins:
            comp_1 = mpc_amps[pod_0.seq_number - 1][db_val_num] - amp_samp_mean(db_val_num, dn_bin_num_0,
                                                                                an_bin_num_0, vl_bin_num_0)

            comp_2 = mpc_amps[pod_1.seq_number - 1][db_val_num] - amp_samp_mean(db_val_num, dn_bin_num_1,
                                                                                an_bin_num_1, vl_bin_num_1)

            inner_sum_num += comp_1 * comp_2

        inner_sums_num.append(inner_sum_num)

    s_coeff_num = np.mean(inner_sums_num)

    return s_coeff_num / s_coeff_den
"""

"""
''' Evaluation conditions '''


# Distance evaluation condition
def distn_sep_fn(pod_x: Pod, pod_y: Pod, dn_: float) -> bool:
    pod_x_ = tsorted_pods[pod_x.seq_number - 2]
    pod_y_ = tsorted_pods[pod_y.seq_number - 2]

    bool_1 = pod_x.tx_rx_alignment == pod_y.tx_rx_alignment
    bool_2 = pod_x.seq_number != 1 and pod_y.seq_number != 1
    bool_3 = distance_3d(pod_x.rx_gps_event, pod_y.rx_gps_event) == dn_
    vel_x = tx_rx_relative_velocity(pod_x.tx_gps_event, pod_x_.rx_gps_event, pod_x.rx_gps_event)
    vel_y = tx_rx_relative_velocity(pod_y.tx_gps_event, pod_y_.rx_gps_event, pod_y.rx_gps_event)

    return bool_1 and bool_2 and bool_3 and vel_x == vel_y


# Alignment evaluation condition
def align_sep_fn(pod_x: Pod, pod_y: Pod, an_: float) -> bool:
    pod_x_ = tsorted_pods[pod_x.seq_number - 2]
    pod_y_ = tsorted_pods[pod_y.seq_number - 2]

    bool_1 = pod_x.seq_number != 1 and pod_y.seq_number != 1
    bool_2 = pod_x.tx_rx_distance_3d == pod_y.tx_rx_distance_3d
    bool_3 = abs(pod_x.tx_rx_alignment - pod_y.tx_rx_alignment) == an_
    vel_x = tx_rx_relative_velocity(pod_x.tx_gps_event, pod_x_.rx_gps_event, pod_x.rx_gps_event)
    vel_y = tx_rx_relative_velocity(pod_y.tx_gps_event, pod_y_.rx_gps_event, pod_y.rx_gps_event)

    return bool_1 and bool_2 and bool_3 and vel_x == vel_y


# Velocity evaluation condition
def vel_sep_fn(pod_x: Pod, pod_y: Pod, vl_: float) -> bool:
    pod_x_ = tsorted_pods[pod_x.seq_number - 2]
    pod_y_ = tsorted_pods[pod_y.seq_number - 2]

    bool_1 = pod_x.tx_rx_alignment == pod_y.tx_rx_alignment
    bool_2 = pod_x.seq_number != 1 and pod_y.seq_number != 1
    bool_3 = pod_x.tx_rx_distance_3d == pod_y.tx_rx_distance_3d
    vel_x = tx_rx_relative_velocity(pod_x.tx_gps_event, pod_x_.rx_gps_event, pod_x.rx_gps_event)
    vel_y = tx_rx_relative_velocity(pod_y.tx_gps_event, pod_y_.rx_gps_event, pod_y.rx_gps_event)

    return bool_1 and bool_2 and bool_3 and abs(vel_x - vel_y) == vl_
"""

"""
''' Computing SACs for the separation variables '''

for dn in np.arange(start=0.0, stop=d_max, step=d_step):
    distn_sep_set = np.array([_pair for _pair in combinations(tsorted_pods, 2) if distn_sep_fn(_pair[0], _pair[1], dn)])
    distns.append((dn, s_coeff(distn_sep_set)))

for an in np.arange(start=0.0, stop=a_max, step=a_step):
    align_sep_set = np.array([_pair for _pair in combinations(tsorted_pods, 2) if align_sep_fn(_pair[0], _pair[1], an)])
    alignments.append((an, s_coeff(align_sep_set)))

for vl in np.arange(start=0.0, stop=v_max, step=v_step):
    vel_sep_set = np.array([_pair for _pair in combinations(tsorted_pods, 2) if vel_sep_fn(_pair[0], _pair[1], vl)])
    velocities.append((vl, s_coeff(vel_sep_set)))

''' Visualizing the SACs for the separation variables '''

scd_layout = dict(xaxis=dict(title='Tx-Rx Distance (in m)'),
                  title='Spatial Consistency Analysis vis-à-vis Distance',
                  yaxis=dict(title='Spatial Autocorrelation Coefficient'))
scd_trace = go.Scatter(x=[distn[0] for distn in distns], mode='lines+markers',
                       y=signal.savgol_filter([distn[1] for distn in distns], sg_wsize, sg_poly_order))

scv_layout = dict(xaxis=dict(title='Tx-Rx Relative Velocity (in m/s)'),
                  title='Spatial Consistency Analysis vis-à-vis Velocity',
                  yaxis=dict(title='Spatial Autocorrelation Coefficient'))
scv_trace = go.Scatter(x=[vel[0] for vel in velocities], mode='lines+markers',
                       y=signal.savgol_filter([veloc[1] for veloc in velocities], sg_wsize, sg_poly_order))

sca_layout = dict(xaxis=dict(title='Tx-Rx Relative Alignment Accuracy (in deg)'),
                  yaxis=dict(title='Spatial (Angular) Autocorrelation Coefficient'),
                  title='Spatial (Angular) Consistency Analysis vis-à-vis Alignment')
sca_trace = go.Scatter(x=[alignment[0] for alignment in alignments], mode='lines+markers',
                       y=signal.savgol_filter([alignment[1] for alignment in alignments], sg_wsize, sg_poly_order))

scd_url = plotly.plotly.plot(dict(data=[scd_trace], layout=scd_layout), filename=sc_distance_png)
scv_url = plotly.plotly.plot(dict(data=[scv_trace], layout=scv_layout), filename=sc_velocity_png)
sca_url = plotly.plotly.plot(dict(data=[sca_trace], layout=sca_layout), filename=sc_alignment_png)

print('SPAVE-28G | Consolidated Processing II | Spatial Consistency Analysis vis-à-vis Distance: {}.'.format(scd_url))
print('SPAVE-28G | Consolidated Processing II | Spatial Consistency Analysis vis-à-vis Velocity: {}.'.format(scv_url))
print('SPAVE-28G | Consolidated Processing II | Spatial Consistency Analysis vis-à-vis Alignment: {}.'.format(sca_url))
"""

"""
CORE VISUALIZATIONS II: RMS delay spread and RMS direction spread
"""

''' Computing ECDFs '''

rms_aoa_dir_spreads = np.array([pod.rms_aoa_dir_spread for pod in pods])
rms_delay_spreads = np.array([pod.rms_delay_spread / 1e-9 for pod in pods])

rms_delay_spread_x, rms_delay_spread_ecdf = ecdf(rms_delay_spreads)
rms_aoa_dir_spread_x, rms_aoa_dir_spread_ecdf = ecdf(rms_aoa_dir_spreads)

''' Visualizing ECDFs '''

rms_ds_layout = dict(yaxis=dict(title='CDF Probability'),
                     xaxis=dict(title='RMS Delay Spread in ns'),
                     title='RMS Delay Spreads Cumulative Distribution Function')
rms_aoa_dirs_layout = dict(yaxis=dict(title='CDF Probability'),
                           xaxis=dict(title='RMS AoA Direction Spread'),
                           title='RMS AoA Direction Spreads Cumulative Distribution Function')

rms_ds_trace = go.Scatter(x=rms_delay_spread_x, mode='lines+markers',
                          y=signal.savgol_filter(rms_delay_spread_ecdf, sg_wsize, sg_poly_order))
rms_aoa_dirs_trace = go.Scatter(x=rms_aoa_dir_spread_x, mode='lines+markers',
                                y=signal.savgol_filter(rms_aoa_dir_spread_ecdf, sg_wsize, sg_poly_order))

rms_aoa_dirs_url = plotly.plotly.plot(dict(data=[rms_aoa_dirs_trace],
                                           layout=rms_aoa_dirs_layout), filename=aoa_rms_dir_spread_png)
rms_ds_url = plotly.plotly.plot(dict(data=[rms_ds_trace], layout=rms_ds_layout), filename=rms_delay_spread_png)

print('SPAVE-28G | Consolidated Processing II | RMS Delay Spread CDF: {}.'.format(rms_ds_url))
print('SPAVE-28G | Consolidated Processing II | RMS AoA Direction Spread CDF: {}.'.format(rms_aoa_dirs_url))

"""
CORE VISUALIZATIONS III: Cluster inter-arrival times, Cluster decay characteristics, and Cluster peak-power distribution
                         TODO: Comparison with SV, QDC, and stochastic channel models via Kolmogorov-Smirnov statistic

TODO: We may need to focus on a representative position along the route here, instead of flattening or sorting through.
"""

''' Computing ECDFs and Preparing decay characteristics '''

decay_chars = np.array([sorted([[_mpc.delay / 1e-9, decibel_1(_mpc.profile_point_power)]
                                for _mpc in _pod.mpc_parameters], key=lambda _v: _v[0]) for _pod in pods])

pwrs = np.array([sorted([decibel_1(_mpc.profile_point_power) for _mpc in _pod.mpc_parameters]) for _pod in pods])
pwrs_x, pwrs_ecdf = ecdf(pwrs.flatten())

inter_arr_times = np.array([[_y - _x for _x, _y in
                             pairwise(sorted([_mpc.delay / 1e-9 for _mpc in _pod.mpc_parameters]))] for _pod in pods])
inter_arr_times_x, inter_arr_times_ecdf = ecdf(inter_arr_times.flatten())

''' Visualizing ECDFs and Decay characteristics '''

decay_chars_layout = dict(yaxis=dict(title='Cluster Peak-Power in dB'),
                          xaxis=dict(title='Delay in ns'), title='Cluster Decay Characteristics')
inter_arr_times_layout = dict(yaxis=dict(title='CDF Probability'),
                              xaxis=dict(title='Cluster Inter-Arrival Time in ns'),
                              title='Cluster Inter-Arrival Times Cumulative Distribution Function')
pwrs_layout = dict(yaxis=dict(title='CDF Probability'),
                   xaxis=dict(title='Cluster Peak-Power in dB'), title='Cluster Peak-Power Distribution')

pwrs_trace = go.Scatter(x=pwrs_x, mode='lines+markers',
                        y=signal.savgol_filter(pwrs_ecdf, sg_wsize, sg_poly_order))
decay_chars_trace = go.Scatter(x=[__v[0] for _v in decay_chars for __v in _v],
                               y=signal.savgol_filter([__v[1] for _v in decay_chars for __v in _v],
                                                      sg_wsize, sg_poly_order), mode='lines+markers')
inter_arr_times_trace = go.Scatter(x=inter_arr_times_x, mode='lines+markers',
                                   y=signal.savgol_filter(inter_arr_times_ecdf, sg_wsize, sg_poly_order))

pwrs_url = plotly.plotly.plot(dict(data=[pwrs_trace], layout=pwrs_layout), filename=pwrs_png)
decay_chars_url = plotly.plotly.plot(dict(data=[decay_chars_trace],
                                          layout=decay_chars_layout), filename=decay_chars_png)
inter_arr_times_url = plotly.plotly.plot(dict(data=[inter_arr_times_trace],
                                              layout=inter_arr_times_layout), filename=inter_arr_times_png)

print('SPAVE-28G | Consolidated Processing II | Cluster Peak-Power Distribution: {}.'.format(pwrs_url))
print('SPAVE-28G | Consolidated Processing II | Cluster Decay Characteristics: {}.'.format(decay_chars_url))
print('SPAVE-28G | Consolidated Processing II | Cluster Inter-Arrival Times CDF: {}.'.format(inter_arr_times_url))

"""
CORE VISUALIZATIONS-IV: Power Delay Angular Profiles (PDAPs), Power Delay Doppler Profiles (PDDPs), and Doppler spectrum

TODO: We may need to focus on a representative position along the route here, instead of flattening or sorting through.
"""

''' Preparing PDAPs and PDDPs '''

pdaps = np.array([sorted([[_mpc.delay / 1e-9, rad2deg(_mpc.aoa_azimuth), decibel_1(_mpc.profile_point_power)]
                          for _mpc in _pod.mpc_parameters], key=lambda _v: _v[0]) for _pod in pods])
pddps = np.array([sorted([[_mpc.delay / 1e-9, _mpc.doppler_shift / 1e3, decibel_1(_mpc.profile_point_power)]
                          for _mpc in _pod.mpc_parameters], key=lambda _v: _v[0]) for _pod in pods])

''' Visualizing PDAPs, PDDPs, and Doppler spectrum '''

doppler_spectrum_layout = dict(yaxis=dict(title='Doppler frequency shift in kHz'),
                               xaxis=dict(title='Power in dB'), title='Doppler Spectrum')
pdaps_layout = dict(yaxis=dict(title='AoA in degrees'), xaxis=dict(title='Delay in ns'),
                    zaxis=dict(title='Power in dB'), title='Power Delay Angular Profiles')
pddps_layout = dict(zaxis=dict(title='Power in dB'), title='Power Delay Doppler Profiles',
                    yaxis=dict(title='Doppler frequency shift in kHz'), xaxis=dict(title='Delay in ns'))

doppler_spectrum_trace = go.Scatter(x=[__v[1] for _v in pdaps for __v in _v],
                                    y=[__v[2] for _v in pdaps for __v in _v], mode='lines+markers')
pdaps_trace = go.Heatmap(x=[__v[0] for _v in pdaps for __v in _v],
                         y=[__v[1] for _v in pdaps for __v in _v],
                         z=[__v[2] for _v in pdaps for __v in _v], mode='lines+markers', colorscale='Viridis')
pddps_trace = go.Heatmap(x=[__v[0] for _v in pddps for __v in _v],
                         y=[__v[1] for _v in pddps for __v in _v],
                         z=[__v[2] for _v in pdaps for __v in _v], mode='lines+markers', colorscale='Viridis')

pdaps_url = plotly.plotly.plot(dict(data=[pdaps_trace], layout=pdaps_layout), filename=pdaps_png)
pddps_url = plotly.plotly.plot(dict(data=[pddps_trace], layout=pddps_layout), filename=pdaps_png)
doppler_spectrum_url = plotly.plotly.plot(dict(data=[doppler_spectrum_trace],
                                               layout=doppler_spectrum_layout), filename=doppler_spectrum_png)

print('SPAVE-28G | Consolidated Processing II | Doppler Spectrum {}.'.format(doppler_spectrum_url))
print('SPAVE-28G | Consolidated Processing II | Power Delay Angular Profiles: {}.'.format(pdaps_url))
print('SPAVE-28G | Consolidated Processing II | Power Delay Doppler Profiles: {}.'.format(pddps_url))
