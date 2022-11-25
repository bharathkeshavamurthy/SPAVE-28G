"""
This script encapsulates the operations involved in estimating the propagation parameters associated with the various
Multi-Path Components (MPCs) in our 28 GHz outdoor measurement campaign on the POWDER testbed; next, it details the
visualizations of the RMS delay- & direction-spread characteristics obtained from this 28 GHz V2X channel modeling;
and finally, it includes spatial decoherence analyses w.r.t distance, alignment, and velocity (later: in IEEE TAP).

The constituent algorithm is Space-Alternating Generalized Expectation-Maximization (SAGE), derived from:

@INPROCEEDINGS{SAGE,
  title={A sliding-correlator-based SAGE algorithm for mmWave wideband channel parameter estimation},
  author={Yin, Xuefeng and He, Yongyu and Song, Zinuo and Kim, Myung-Don and Chung, Hyun Kyu},
  booktitle={The 8th European Conference on Antennas and Propagation (EuCAP 2014)},
  year={2014}, pages={625-629}, doi={10.1109/EuCAP.2014.6901837}}.

The plots output from this script include RMS delay-spread CDF, AoA RMS direction-spread CDF, and spatial decoherence
characteristics under Tx-Rx distance, Tx-Rx alignment accuracy, and Tx-Rx relative velocity (later: in IEEE TAP)
variations -- namely, evaluating the spatial autocorrelation coefficient value (corr coefficient equation).

Reference Papers:

@ARTICLE{Visualizations-I,
  author={Gustafson, Carl and Haneda, Katsuyuki and Wyne, Shurjeel and Tufvesson, Fredrik},
  title={On mm-Wave Multipath Clustering and Channel Modeling},
  journal={IEEE Transactions on Antennas and Propagation},
  year={2014}, volume={62}, number={3}, pages={1445-1455},
  doi={10.1109/TAP.2013.2295836}}

@INPROCEEDINGS{Visualizations-II,
  author={Gustafson, Carl and Tufvesson, Fredrik and Wyne, Shurjeel and Haneda, Katsuyuki and Molisch, Andreas F.},
  title={Directional Analysis of Measured 60 GHz Indoor Radio Channels Using SAGE},
  booktitle={2011 IEEE 73rd Vehicular Technology Conference (VTC Spring)},
  year={2011}, volume={}, number={}, pages={1-5},
  doi={10.1109/VETECS.2011.5956639}}

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2022. All Rights Reserved.
"""

import os
import re
import json
import plotly
import requests
import datetime
import scipy.io
import functools
import dataclasses
import numpy as np
import cvxpy as cp
from enum import Enum
from pyproj import Proj
from geopy import distance
import plotly.graph_objs as go
from scipy import signal, constants
from typing import Tuple, List, Dict
from scipy.interpolate import interp1d
from dataclasses import dataclass, field
import sk_dsp_comm.fir_design_helper as fir_d
from concurrent.futures import ThreadPoolExecutor

"""
INITIALIZATIONS-I: Collections & Utilities
"""
pi, c, distns, alignments, velocities = np.pi, constants.c, [], [], []
deg2rad, rad2deg = lambda x: x * (pi / 180.0), lambda x: x * (180.0 / pi)
linear_1, linear_2 = lambda x: 10 ** (x / 10.0), lambda x: 10 ** (x / 20.0)
rx_gps_events, tx_imu_traces, rx_imu_traces, pdp_segments, pods = [], [], [], [], []
decibel_1, decibel_2, gamma = lambda x: 10 * np.log10(x), lambda x: 20 * np.log10(x), lambda fc, fc_: fc / (fc - fc_)

"""
INITIALIZATIONS-II: Enumerations & Dataclasses (Inputs)
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
CONFIGURATIONS: Input & Output Dirs | GPS & IMU logs | Power delay profiles
"""

''' urban-campus-II route '''
comm_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/pdp/'
rx_gps_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/gps/'
tx_imu_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/tx-realm/imu/'
rx_imu_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/imu/'
rms_delay_spread_png, aoa_rms_dir_spread_png = 'uc_rms_delay_spread.png', 'uc_aoa_rms_dir_spread.png'
sc_distance_png, sc_alignment_png, sc_velocity_png = 'uc_sc_distance.png', 'uc_sc_alignment.png', 'uc_sc_velocity.png'

''' urban-vegetation route '''
# comm_dir = 'D:/SPAVE-28G/analyses/urban-vegetation/rx-realm/pdp/'
# rx_gps_dir = 'D:/SPAVE-28G/analyses/urban-vegetation/rx-realm/gps/'
# tx_imu_dir = 'D:/SPAVE-28G/analyses/urban-vegetation/tx-realm/imu/'
# rx_imu_dir = 'D:/SPAVE-28G/analyses/urban-vegetation/rx-realm/imu/'
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'uv_rms_delay_spread.png', 'uv_aoa_rms_dir_spread.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'uv_sc_distance.png', 'uv_sc_alignment.png', 'uv_sc_velocity.png'

''' suburban-fraternities route '''
# comm_dir = 'D:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/pdp/'
# rx_gps_dir = 'D:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/gps/'
# tx_imu_dir = 'D:/SPAVE-28G/analyses/suburban-fraternities/tx-realm/imu/'
# rx_imu_dir = 'D:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/imu/'
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'sf_rms_delay_spread.png', 'sf_aoa_rms_dir_spread.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'sf_sc_distance.png', 'sf_sc_alignment.png', 'sf_sc_velocity.png'

''' Tx GPSEvent (Rooftop-mounted | William Browning Building) '''
tx_gps_event = GPSEvent(latitude=Member(component=40.766173670),
                        longitude=Member(component=-111.847939330), altitude_ellipsoid=Member(component=1459.1210))

''' Generic configurations '''
max_workers, sg_wsize, sg_poly_order = 1024, 53, 3
lla_utm_proj = Proj(proj='utm', zone=32, ellps='WGS84')
ant_log_file = 'D:/SPAVE-28G/analyses/antenna_pattern.mat'
output_dir = 'C:/Users/kesha/Workspaces/SPAVE-28G/test/analyses/'
time_windowing_config = {'multiplier': 0.5, 'truncation_length': int(2e5)}
d_max, d_step, a_max, a_step, v_max, v_step = 350.0, 0.1, 5.0, 0.005, 25.0, 0.1
solver, max_iters, eps_abs, eps_rel, verbose = 'SCS', int(1e6), 1e-6, 1e-6, True
delay_tol, doppler_tol, att_tol, aoa_az_tol, aoa_el_tol = 1e-9, 1.0, 0.1, 0.1, 0.1
plotly.tools.set_credentials_file(username='bkeshav1', api_key='PUYaTVhV1Ok04I07S4lU')
noise_elimination_config = {'multiplier': 3.5, 'min_peak_index': 2000, 'num_samples_discard': 0,
                            'max_num_samples': int(5e5), 'relative_range': [0.875, 0.975], 'threshold_ratio': 0.9}
pdp_samples_file, start_timestamp_file, parsed_metadata_file = 'samples.log', 'timestamp.log', 'parsed_metadata.log'
tx_fc, rx_fc, pn_v0, pn_l, pn_m, wlength, n_sigma, max_ant_gain = 400e6, 399.95e6, 0.5, 11, 2047, c / 28e9, 0.015, 22.0
min_threshold, sample_rate, datetime_format, pn_reps, max_mpcs = 1e5, 2e6, '%Y-%m-%d %H:%M:%S.%f', int(pn_m / pn_l), 1
prefilter_config = {'passband_freq': 60e3, 'stopband_freq': 65e3, 'passband_ripple': 0.01, 'stopband_attenuation': 80.0}

"""
INITIALIZATIONS-III: Enumerations & Dataclasses (Temps | Outputs)
"""


@dataclass(order=True)
class MPCParameters:
    path_number: int = 0
    delay: float = 0.0  # s
    aoa_azimuth: float = 0.0  # rad
    aoa_elevation: float = 0.0  # rad
    doppler_shift: float = 0.0  # Hz
    profile_point_power: float = 0.0  # linear
    attenuation: float = complex(0.0, 0.0)


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
    raw_rx_samples: np.array = np.array([], dtype=np.csingle)
    processed_rx_samples: np.array = np.array([], dtype=np.csingle)
    correlation_peak: float = 0.0  # linear


@dataclass(order=True)
class Pod:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    tx_gps_event: GPSEvent = GPSEvent()
    rx_gps_event: GPSEvent = GPSEvent()
    tx_imu_trace: IMUTrace = IMUTrace()
    rx_imu_trace: IMUTrace = IMUTrace()
    tx_elevation: float = 0.0  # m
    rx_elevation: float = 0.0  # m
    tx_rx_alignment: float = 0.0  # deg
    tx_rx_distance_2d: float = 0.0  # m
    tx_rx_distance_3d: float = 0.0  # m
    pdp_segment: PDPSegment = PDPSegment()
    n_mpcs: int = max_mpcs
    mpc_parameters: List[MPCParameters] = field(default_factory=lambda: (MPCParameters() for _ in range(max_mpcs)))
    rms_delay_spread: float = 0.0
    rms_aoa_dir_spread: float = 0.0


"""
CORE ROUTINES
"""


# Pack a dictionary into a dataclass instance
def ddc_transform(d: Dict, dc: dataclass) -> dataclass:
    d_l, d_f = {}, {f.name: f.type for f in dataclasses.fields(dc)}

    for k, v in d.items():
        d_l[k] = (lambda: v, lambda: ddc_transform(v, Member))[d_f[k] == Member]()

    return dc(**d_l)


# Parse the provided file and store it in the given collection
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


# Altitude getter (m)
def altitude(y: GPSEvent) -> float:
    return y.altitude_ellipsoid.component


# Tx-Rx 2D distance (m)
def tx_rx_distance_2d(tx: GPSEvent, rx: GPSEvent) -> float:
    coords_tx = (latitude(tx), longitude(tx))
    coords_rx = (latitude(rx), longitude(rx))
    return distance.distance(coords_tx, coords_rx).m


# Tx-Rx 3D distance (m)
def tx_rx_distance_3d(tx: GPSEvent, rx: GPSEvent) -> float:
    alt_tx, alt_rx = altitude(tx), altitude(rx)
    return np.sqrt(np.square(tx_rx_distance_2d(tx, rx)) + np.square(alt_tx - alt_rx))


# General 3D distance (m)
def distance_3d(y1: GPSEvent, y2: GPSEvent) -> float:
    coords_y1 = (latitude(y1), longitude(y1))
    coords_y2 = (latitude(y2), longitude(y2))
    alt_y1, alt_y2 = altitude(y1), altitude(y2)
    distance_2d = distance.distance(coords_y1, coords_y2).m
    return np.sqrt(np.square(distance_2d) + np.square(alt_y1 - alt_y2))


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


# Tx-Rx overall relative alignment accuracy (deg)
def tx_rx_alignment(tx: GPSEvent, rx: GPSEvent, m_tx: IMUTrace, m_rx: IMUTrace) -> float:
    m_tx_yaw_, m_tx_pitch_ = d_alignment(tx, rx, m_tx)
    m_rx_yaw_, m_rx_pitch_ = d_alignment(rx, tx, m_rx, False)
    return max(abs(180.0 - m_tx_yaw_ - m_rx_yaw_), abs(180.0 - m_tx_pitch_ - m_rx_pitch_))


# Tx-Rx relative velocity (ms-1)
def tx_rx_relative_velocity(tx_i: GPSEvent, tx_j: GPSEvent, rx_i: GPSEvent, rx_j: GPSEvent) -> float:
    tx_i_lat, tx_i_lon = latitude(tx_i), longitude(tx_i)
    tx_j_lat, tx_j_lon = latitude(tx_j), longitude(tx_j)
    rx_i_lat, rx_i_lon = latitude(rx_i), longitude(rx_i)
    rx_j_lat, rx_j_lon = latitude(rx_j), longitude(rx_j)
    tx_i_dt = datetime.datetime.strptime(tx_i.timestamp, datetime_format)
    tx_j_dt = datetime.datetime.strptime(tx_j.timestamp, datetime_format)
    rx_i_dt = datetime.datetime.strptime(rx_i.timestamp, datetime_format)
    rx_j_dt = datetime.datetime.strptime(rx_j.timestamp, datetime_format)
    tx_v = (distance.distance((tx_i_lat, tx_i_lon), (tx_j_lat, tx_j_lon)).m / (tx_j_dt - tx_i_dt).microseconds) * 1e6
    rx_v = (distance.distance((rx_i_lat, rx_i_lon), (rx_j_lat, rx_j_lon)).m / (rx_j_dt - rx_i_dt).microseconds) * 1e6

    if ((tx_j_lat > tx_i_lat or tx_j_lon < tx_i_lon) and (rx_j_lat > rx_i_lat or rx_j_lon < rx_i_lon)) or \
            ((tx_j_lat < tx_i_lat or tx_j_lon > tx_i_lon) and (rx_j_lat < rx_i_lat or rx_j_lon > rx_i_lon)):
        return abs(tx_v - rx_v)
    else:
        return abs(tx_v + rx_v)


# USGS EPQS: Tx/Rx elevation (m)
def elevation(y: GPSEvent) -> float:
    lat, lon, alt = latitude(y), longitude(y), altitude(y)
    base_epqs_url = 'https://nationalmap.gov/epqs/pqs.php?x={}&y={}&output=json&units=Meters'
    epqs_kw, eq_kw, e_kw = 'USGS_Elevation_Point_Query_Service', 'Elevation_Query', 'Elevation'
    return abs(alt - requests.get(base_epqs_url.format(lon, lat)).json()[epqs_kw][eq_kw][e_kw])


# Cartesian coordinates to Spherical coordinates (x, y, z) -> (r, phi, theta) radians
def cart2sph(x: float, y: float, z: float) -> Tuple:
    return np.sqrt((x ** 2) + (y ** 2) + (z ** 2)), np.arctan2(y, x), np.arctan2(z, np.sqrt((x ** 2) + (y ** 2)))


# Spherical coordinates to Cartesian coordinates -> (r, phi, theta) radians -> (x, y, z)
def sph2cart(r: float, phi: float, theta: float) -> Tuple:
    return r * np.sin(theta) * np.cos(phi), r * np.sin(theta) * np.sin(phi), r * np.cos(theta)


# Process the power-delay-profiles recorded at the receiver
def process_rx_samples(x: np.array) -> Tuple:
    fs = sample_rate
    t_mul, t_len = time_windowing_config.values()
    f_pass, f_stop, d_pass, d_stop = prefilter_config.values()
    ne_mul, min_peak_idx, n_min, n_max, rel_range, amp_threshold = noise_elimination_config.values()

    # Frequency Manipulation: Pre-filtering via a Low Pass Filter (LPF)
    b = fir_d.fir_remez_lpf(fs=fs, f_pass=f_pass, f_stop=f_stop, d_pass=d_pass, d_stop=d_stop)
    samps = signal.lfilter(b=b, a=1, x=x, axis=0)

    # Temporal Manipulation: Initial temporal truncation | Time-windowing
    samps = samps[t_len:] if samps.shape[0] > (4 * t_len) else samps
    window_size, n_samples = int(fs * t_mul), samps.shape[0]
    if n_samples > window_size:
        n_samples = window_size
        samps = samps[int(0.5 * n_samples) + (np.array([-1, 1]) * int(window_size / 2))]

    # Noise Elimination: The peak search method is 'TallEnoughAbs' | Thresholded at (ne_mul * sigma) + mu
    samps_ = samps[n_min:n_max] if n_samples > n_max else samps[n_min:]
    a_samps = np.abs(samps_)[min_peak_idx:]
    samps_ = samps_[((np.where(a_samps > amp_threshold * max(a_samps))[0][0] +
                      min_peak_idx - 1) * np.array(rel_range)).astype(dtype=int)]
    th_min, th_max = np.array([-1, 1]) * ne_mul * np.std(samps_) + np.mean(samps_)
    thresholder = np.vectorize(lambda s: 0 + 0j if (s > th_min) and (s < th_max) else s)

    return n_samples, thresholder(samps)


# Correlation peak in the processed rx_samples (linear)
def correlation_peak(n: int, x: np.array) -> float:
    return max(np.abs(np.fft.fft(x)) / n)


# RMS delay spread computation (std)
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


# RMS direction spread computation (std)
# See Visualizations-II: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=5956639]
def rms_direction_spread(mpcs: List[MPCParameters]) -> float:
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


# Spatial autocorrelation coefficient computation
# See [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=7996408]
# Increasing Tx-Rx distance, Tx-Rx alignment accuracy, and Tx-Rx relative velocity (TAP) | corr: prev | corr_: curr
def s_coeff(pdp: PDPSegment, pdp_: PDPSegment) -> float:
    n, n_ = pdp.num_samples, pdp_.num_samples
    samps, samps_ = pdp.processed_rx_samples, pdp_.processed_rx_samples

    s, s_ = np.abs(np.fft.fft(samps)) / n, np.abs(np.fft.fft(samps_)) / n_
    a, a_ = np.mean(s), np.mean(s_)
    ln = min(n, n_)

    s_mod, s_mod_ = (s - a)[:ln], (s_ - a_)[:ln]
    num = np.mean(s_mod * s_mod_)
    den = np.sqrt(np.mean(np.square(s_mod)) * np.mean(np.square(s_mod_)))

    return np.clip(num / den, -1.0, 1.0) if den != 0.0 else np.nan


# Empirical Cumulative Distribution Function
def ecdf(x: np.array) -> Tuple:
    x_, ct = np.unique(x, return_counts=True)
    cum_sum = np.cumsum(x_)
    return x_, cum_sum / cum_sum[-1]


# SAGE Algorithm: MPC parameters computation
# See: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=753729]
# Also see: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6901837]
def estimate_mpc_parameters(tx: GPSEvent, rx: GPSEvent, n: int, x: np.array) -> List:
    f_c, _f_c = tx_fc, rx_fc
    f_s, n_std = sample_rate, n_sigma
    v_0, l_, k_ = pn_v0, pn_l, pn_reps

    y_f = np.fft.fft(x) / n
    fs = np.argsort(np.fft.fftfreq(n, (1 / f_s)))
    n_f = (n_std * np.random.randn(fs.shape[0], )).view(np.csingle)

    # Previous parameter estimates ...$[i-1]$
    nus = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    phis = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    taus = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    thetas = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    alphas = np.zeros(shape=(max_mpcs,), dtype=np.csingle)

    '''
    TODO: Bring in an object-oriented programming methodology here wherein we have a configurable set of MPCParameters, 
          possibly defined by enumerations, and each enum member has a class definition with its relevant routines such 
          as convergence_check, getters/setters, L1/L2 norms, etc. This allows for easy encapsulation across the script.
    '''

    # Flip components in the rectangular chips of the PN-sequence $u(f)$
    def flip(a: int) -> int:
        assert a == 1 or a == -1
        return 1 if a == -1 else -1

    # Sinc function
    def sinc(z: float) -> float:
        return np.sin(pi * z) / (pi * z)

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
                    pn_sum += ((2 * a_i) - 1) * complex(np.cos(-pi * ((_k * i) / l_)), np.sin(-pi * ((_k * i) / l_)))
                    flip(a_i)

                f_shifts.append(f_shift)
                n_sum += pn_sum * n_f[idx_f] * sinc(k / l_) * complex(np.cos(-pi * (_k / l_)), np.sin(-pi * (_k / l_)))

        return f_shifts, (v_0 / l_) * n_sum

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

    # Find the profile point power associated with the MPCParameters index
    def profile_point_power(l_idx: int) -> float:
        steering_l = compute_steering(phis_[l_idx], thetas_[l_idx])
        tau_l, nu_l, alpha_l = taus_[l_idx], nus_[l_idx], alphas_[l_idx]
        return np.square(np.abs(alpha_l * steering_l * signal_component(tau_l, nu_l)[1]))

    # Expectation of the log-likelihood component: Use $[i-1]$ estimates, i.e., phis, thetas, alphas, taus, nus
    def estep(l_idx: int, yf_s: np.array) -> np.array:
        return yf_s - np.sum(np.array([compute_steering(phis[_l_mpc], thetas[_l_mpc]) *
                                       alphas[_l_mpc] * signal_component(taus[_l_mpc], nus[_l_mpc])[1]
                                       for _l_mpc in range(max_mpcs) if _l_mpc != l_idx], dtype=np.csingle))

    # Maximization step for the MPC delay & Doppler shift $\argmax_{\tau,\nu}\{\eta_(\tau,\nu)\}
    def tau_nu_mstep(l_idx: int) -> Tuple:
        tau_var, nu_var = cp.Variable(value=0.0), cp.Variable(value=0.0)

        estep_comps = estep(l_idx, y_f)
        sig_comp = signal_component(tau_var.value, nu_var.value)[1]
        sig_comps = np.array([sig_comp for _ in fs], dtype=np.csingle)

        numerator = cp.square(cp.abs(sig_comps.conj().T @ w_matrix @ estep_comps))
        denominator = cp.abs(sig_comps.conj().T @ w_matrix @ sig_comps)
        objective = numerator / denominator

        # noinspection PyTypeChecker
        problem = cp.Problem(objective=objective,
                             constraints=[1e-9 <= tau_var <= 1e-6, 0.0 <= nu_var <= 1e3])
        problem.solve(solver=solver, max_iters=max_iters, eps_abs=eps_abs, eps_rel=eps_rel, verbose=verbose)

        return tau_var.value, nu_var.value

    # Maximization step for the MPC complex attenuation
    def alpha_mstep(l_idx: int, tau_l: float, nu_l: float, phi_l: float, theta_l: float) -> complex:
        estep_comps = estep(l_idx, y_f)
        sig_comp = signal_component(tau_l, nu_l)[1]
        steering_comp = compute_steering(phi_l, theta_l)
        sig_comps = steering_comp * np.array([sig_comp for _ in fs], dtype=np.csingle)

        numerator = functools.reduce(np.matmul, [sig_comps.conj().T, w_matrix, estep_comps])
        denominator = functools.reduce(np.matmul, [sig_comps.conj().T, w_matrix, sig_comps])

        return numerator / denominator

    # Maximization step for the MPC AoA azimuth and AoA elevation
    def phi_theta_mstep(l_idx: int, tau_l: float, nu_l: float, alpha_l: complex) -> Tuple:
        phi_var, theta_var = cp.Variable(value=0.0), cp.Variable(value=0.0)

        estep_comps = estep(l_idx, y_f)
        sig_comp = signal_component(tau_l, nu_l)[1]
        steering_comp = compute_steering(phi_var.value, theta_var.value)

        sig_comps = steering_comp * alpha_l * np.array([sig_comp for _ in fs], dtype=np.csingle)
        numerator = cp.square(cp.abs(sig_comps.conj().T @ w_matrix @ estep_comps))
        denominator = cp.abs(sig_comps.conj().T @ w_matrix @ sig_comps)
        objective = numerator / denominator

        # noinspection PyTypeChecker
        problem = cp.Problem(objective=objective,
                             constraints=[-pi <= phi_var <= pi, -(pi / 2.0) <= theta_var <= (pi / 2.0)])
        problem.solve(solver=solver, max_iters=max_iters, eps_abs=eps_abs, eps_rel=eps_rel, verbose=verbose)

        return phi_var.value, theta_var.value

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

    # Current parameter estimates ...$[i]$
    nus_ = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    phis_ = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    taus_ = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    thetas_ = np.zeros(shape=(max_mpcs,), dtype=np.float64)
    alphas_ = np.zeros(shape=(max_mpcs,), dtype=np.csingle)
    nu_, tau_, phi_, theta_, alpha_ = nus_[0], taus_[0], phis_[0], thetas_[0], alphas_[0]
    w_matrix = np.linalg.inv(np.diag(np.full(fs.shape[0], np.mean(np.square(np.abs(noise_component()[1]))))))

    # SAGE wrapper
    for l_mpc in range(max_mpcs):
        first_iter = True

        while not is_converged(l_mpc):
            first_iter = False

            nus[l_mpc] = nu_
            taus[l_mpc] = tau_
            phis[l_mpc] = phi_
            thetas[l_mpc] = theta_
            alphas[l_mpc] = alpha_

            tau_, nu_ = tau_nu_mstep(l_mpc)
            taus_[l_mpc] = tau_
            nus_[l_mpc] = nu_

            phi_, theta_ = phi_theta_mstep(l_mpc, tau_, nu_, alphas[l_mpc])
            thetas_[l_mpc] = theta_
            phis_[l_mpc] = phi_

            alpha_ = alpha_mstep(l_mpc, tau_, nu_, phi_, theta_)
            alphas_[l_mpc] = alpha_

    return [MPCParameters(path_number=l_mpc, delay=taus_[l_mpc],
                          doppler_shift=nus_[l_mpc], attenuation=alphas_[l_mpc],
                          aoa_azimuth=phis_[l_mpc], aoa_elevation=thetas_[l_mpc],
                          profile_point_power=profile_point_power(l_mpc)) for l_mpc in range(max_mpcs)]


"""
CORE OPERATIONS: Parsing the GPS, IMU, and PDP logs | SAGE estimation | Spatial consistency analyses
"""

# Antenna pattern
log = scipy.io.loadmat(ant_log_file)
az_log, el_log = log['pat28GAzNorm'], log['pat28GElNorm']
az_angles, az_amps = np.squeeze(az_log['azs'][0][0]), np.squeeze(az_log['amps'][0][0])
el_angles, el_amps = np.squeeze(el_log['els'][0][0]), np.squeeze(el_log['amps'][0][0])

# Extract Rx gps_events
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(1, len(os.listdir(rx_gps_dir))):
        filename = 'gps_event_{}.json'.format(i)
        parse(rx_gps_events, GPSEvent, ''.join([rx_gps_dir, filename]))

# Extract Tx imu_traces
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(1, len(os.listdir(tx_imu_dir))):
        filename = 'imu_trace_{}.json'.format(i)
        parse(tx_imu_traces, IMUTrace, ''.join([tx_imu_dir, filename]))

# Extract Rx imu_traces
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(1, len(os.listdir(rx_imu_dir))):
        filename = 'imu_trace_{}.json'.format(i)
        parse(rx_imu_traces, IMUTrace, ''.join([rx_imu_dir, filename]))

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

            if np.isnan(raw_rx_samples).any() or np.abs(np.min(raw_rx_samples)) > min_threshold:
                continue

            num_samples, processed_rx_samples = process_rx_samples(raw_rx_samples)
            pdp_segments.append(PDPSegment(num_samples=num_samples,
                                           seq_number=seq_number + 1, timestamp=str(timestamp),
                                           correlation_peak=correlation_peak(num_samples, processed_rx_samples),
                                           raw_rx_samples=raw_rx_samples, processed_rx_samples=processed_rx_samples))

''' Match gps_event, imu_trace, and pdp_segment timestamps across both the Tx and the Rx realms'''

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
                    rms_aoa_dir_spread=rms_direction_spread(mpc_parameters),
                    tx_rx_distance_2d=tx_rx_distance_2d(tx_gps_event, rx_gps_event),
                    tx_elevation=elevation(tx_gps_event), rx_elevation=elevation(rx_gps_event),
                    mpc_parameters=mpc_parameters, rms_delay_spread=rms_delay_spread(mpc_parameters),
                    tx_rx_alignment=tx_rx_alignment(tx_gps_event, rx_gps_event, IMUTrace(), IMUTrace()),
                    pdp_segment=pdp_segment, tx_rx_distance_3d=tx_rx_distance_3d(tx_gps_event, rx_gps_event)))

"""
CORE VISUALIZATIONS-I: Spatial decoherence analyses 
"""

idxs = [_i for _i in range(len(pods))]
pod = max(pods, key=lambda _pod: _pod.pdp_segment.correlation_peak)

for dn in np.arange(start=0.0, stop=d_max, step=d_step):
    i_ = min(idxs, key=lambda idx: abs(pod.tx_rx_alignment - pods[idx].tx_rx_alignment) +
                                   abs(dn - distance_3d(pod.rx_gps_event, pods[idx].rx_gps_event)))
    distns.append((dn, s_coeff(pod.pdp_segment, pods[i_].pdp_segment)))

for an in np.arange(start=0.0, stop=a_max, step=a_step):
    i_ = min(idxs, key=lambda idx: abs(distance_3d(pod.rx_gps_event, pods[idx].rx_gps_event)) +
                                   abs(an - abs(pod.tx_rx_alignment - pods[idx].tx_rx_alignment)))
    alignments.append((an, s_coeff(pod.pdp_segment, pods[i_].pdp_segment)))

for vn in np.arange(start=0.0, stop=v_max, step=v_step):
    i_ = min(idxs, key=lambda: abs(vn - tx_rx_relative_velocity(pod.tx_gps_event, pods[i_].tx_gps_event,
                                                                pod.rx_gps_event, pods[i_].rx_gps_event)))
    velocities.append((vn, s_coeff(pod.pdp_segment, pods[i_].pdp_segment)))

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

sca_layout = dict(yaxis=dict(title='Spatial Autocorrelation Coefficient'),
                  title='Spatial Consistency Analysis vis-à-vis Alignment',
                  xaxis=dict(title='Tx-Rx Relative Alignment Accuracy (in deg)'))
sca_trace = go.Scatter(x=[alignment[0] for alignment in alignments], mode='lines+markers',
                       y=signal.savgol_filter([alignment[1] for alignment in alignments], sg_wsize, sg_poly_order))

scd_url = plotly.plotly.plot(dict(data=[scd_trace], layout=scd_layout), filename=sc_distance_png)
scv_url = plotly.plotly.plot(dict(data=[scv_trace], layout=scv_layout), filename=sc_velocity_png)
sca_url = plotly.plotly.plot(dict(data=[sca_trace], layout=sca_layout), filename=sc_alignment_png)
print('SPAVE-28G | Consolidated Processing II | Spatial Consistency Analysis vis-à-vis Distance: {}'.format(scd_url))
print('SPAVE-28G | Consolidated Processing II | Spatial Consistency Analysis vis-à-vis Velocity: {}'.format(scv_url))
print('SPAVE-28G | Consolidated Processing II | Spatial Consistency Analysis vis-à-vis Alignment: {}'.format(sca_url))

"""
CORE VISUALIZATIONS-II: RMS delay spread and RMS direction spread
"""

rms_delay_spreads = np.array([pod.rms_delay_spread for pod in pods])
rms_aoa_dir_spreads = np.array([pod.rms_aoa_dir_spread for pod in pods])

rms_delay_spread_x, rms_delay_spread_ecdf = ecdf(rms_delay_spreads)
rms_aoa_dir_spread_x, rms_aoa_dir_spread_ecdf = ecdf(rms_aoa_dir_spreads)

rms_ds_layout = dict(yaxis=dict(title='CDF Probability'),
                     xaxis=dict(title='RMS Delay Spreads (x) in ns'),
                     title='RMS Delay Spread Cumulative Distribution Function')
rms_aoa_dirs_layout = dict(yaxis=dict(title='CDF Probability'),
                           xaxis=dict(title='RMS AoA Direction Spreads (x) in deg'),
                           title='RMS AoA Direction Spread Cumulative Distribution Function')

rms_ds_trace = go.Scatter(x=rms_delay_spread_x, y=rms_delay_spread_ecdf, mode='lines+markers')
rms_aoa_dirs_trace = go.Scatter(x=rms_aoa_dir_spread_x, y=rms_aoa_dir_spread_ecdf, mode='lines+markers')

rms_aoa_dirs_url = plotly.plotly.plot(dict(data=[rms_aoa_dirs_trace],
                                           layout=rms_aoa_dirs_layout), filename=aoa_rms_dir_spread_png)
rms_ds_url = plotly.plotly.plot(dict(data=[rms_ds_trace], layout=rms_ds_layout), filename=rms_delay_spread_png)

print('SPAVE-28G | Consolidated Processing II | RMS Delay Spread CDF: {}'.format(rms_ds_url))
print('SPAVE-28G | Consolidated Processing II | RMS AoA Direction Spread CDF: {}'.format(rms_aoa_dirs_url))
