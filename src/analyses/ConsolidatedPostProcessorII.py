"""
This script encapsulates the operations involved in estimating the propagation parameters associated with the various
Multi-Path Components (MPCs) in our 28-GHz outdoor measurement campaign on the POWDER testbed; next, it details the
visualizations of the RMS delay- & direction-spread characteristics obtained from this 28-GHz V2X channel modeling;
and finally, it incorporates spatial consistency analyses vis-à-vis distance, alignment, and velocity.

The constituent algorithm is Space-Alternating Generalized Expectation-Maximization (SAGE), derived from:

@INPROCEEDINGS{SAGE,
  title={A sliding-correlator-based SAGE algorithm for Mm-wave wideband channel parameter estimation},
  author={Yin, Xuefeng and He, Yongyu and Song, Zinuo and Kim, Myung-Don and Chung, Hyun Kyu},
  booktitle={The 8th European Conference on Antennas and Propagation (EuCAP 2014)},
  year={2014}, pages={625-629}, doi={10.1109/EuCAP.2014.6901837}}.

The plots resulting from this script include RMS delay-spread CDF, RMS direction-spread CDF, and spatial decoherence
characteristics under Tx-Rx distance, Tx-Rx alignment accuracy, and Tx-Rx relative velocity variations: the relative
drop in the time-dilated cross-correlation peak magnitudes under distance, alignment, and velocity variations.

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
import dataclasses
import numpy as np
from enum import Enum
from pyproj import Proj
from scipy import signal
from geopy import distance
from scipy import constants
from typing import Tuple, Dict
import plotly.graph_objs as go
from collections import namedtuple
from scipy.interpolate import interp1d
from dataclasses import dataclass, field
import sk_dsp_comm.fir_design_helper as fir_d

"""
INITIALIZATIONS-I: Collections & Utilities
"""
pi, c, distns, alignments, velocities = np.pi, constants.c, [], [], []
deg2rad, rad2deg = lambda x: x * (pi / 180.0), lambda x: x * (180.0 / pi)
linear_1, linear_2 = lambda x: 10 ** (x / 10.0), lambda x: 10 ** (x / 20.0)
rx_gps_events, tx_imu_traces, rx_imu_traces, pdp_segments, pods = [], [], [], [], []
pattern = namedtuple('pattern', ['angles', 'amplitudes', 'amplitudes_db', 'angles_hpbw', 'powers_hpbw', 'hpbw'])
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
    latitude: Member = Member()
    longitude: Member = Member()
    altitude_ellipsoid: Member = Member()
    altitude_msl: Member = Member()
    speed: Member = Member()
    heading: Member = Member()
    horizontal_acc: Member = Member()
    vertical_acc: Member = Member()
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
    horizontal_accuracy: Member = Member()
    vertical_accuracy: Member = Member()
    ultra_core_length: int = 16
    core_length: int = 37
    total_length: int = 39


@dataclass(order=True)
class IMUTrace:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    yaw_angle: float = 0.0
    pitch_angle: float = 0.0


"""
CONFIGURATIONS: Input & Output Dirs | GPS logs | Power delay profiles
"""

''' urban-campus-II route '''
rms_delay_spread_png = 'uc_rms_delay_spread.png'
ant_log_file = 'D:/SPAVE-28G/analyses/antenna_pattern.mat'
comm_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/pdp/'
output_dir = 'C:/Users/kesha/Workspaces/SPAVE-28G/test/analyses/'
rx_gps_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/gps/'
tx_imu_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/tx-realm/imu/'
rx_imu_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/imu/'
aod_rms_dir_spread_png, aoa_rms_dir_spread_png = 'uc_aod_rms_dir_spread.png', 'uc_aoa_rms_dir_spread.png'
sc_distance_png, sc_alignment_png, sc_velocity_png = 'uc_sc_distance.png', 'uc_sc_alignment.png', 'uc_sc_velocity.png'

''' urban-vegetation route '''
# rms_delay_spread_png = 'uv_rms_delay_spread.png'
# comm_dir = 'D:/SPAVE-28G/analyses/urban-vegetation/rx-realm/pdp/'
# output_dir = 'C:/Users/kesha/Workspaces/SPAVE-28G/test/analyses/'
# rx_gps_dir = 'D:/SPAVE-28G/analyses/urban-vegetation/rx-realm/gps/'
# tx_imu_dir = 'D:/SPAVE-28G/analyses/urban-vegetation/tx-realm/imu/'
# rx_imu_dir = 'D:/SPAVE-28G/analyses/urban-vegetation/rx-realm/imu/'
# aod_rms_dir_spread_png, aoa_rms_dir_spread_png = 'uv_aod_rms_dir_spread.png', 'uv_aoa_rms_dir_spread.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'uv_sc_distance.png', 'uv_sc_alignment.png', 'uv_sc_velocity.png'

''' suburban-fraternities route '''
# rms_delay_spread_png = 'sf_rms_delay_spread.png'
# comm_dir = 'D:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/pdp/'
# output_dir = 'C:/Users/kesha/Workspaces/SPAVE-28G/test/analyses/'
# rx_gps_dir = 'D:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/gps/'
# tx_imu_dir = 'D:/SPAVE-28G/analyses/suburban-fraternities/tx-realm/imu/'
# rx_imu_dir = 'D:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/imu/'
# aod_rms_dir_spread_png, aoa_rms_dir_spread_png = 'sf_aod_rms_dir_spread.png', 'sf_aoa_rms_dir_spread.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'sf_sc_distance.png', 'sf_sc_alignment.png', 'sf_sc_velocity.png'

''' Tx GPSEvent (Rooftop-mounted | William Browning Building) '''
tx_gps_event = GPSEvent(latitude=Member(component=40.766173670),
                        longitude=Member(component=-111.847939330), altitude_ellipsoid=Member(component=1459.1210))

''' Generic configurations '''
lla_utm_proj = Proj(proj='utm', zone=32, ellps='WGS84')
time_windowing_config = {'multiplier': 0.5, 'truncation_length': 200000}
plotly.tools.set_credentials_file(username='bkeshav1', api_key='CLTFaBmP0KN7xw1fUheu')
noise_elimination_config = {'multiplier': 3.5, 'min_peak_index': 2000, 'num_samples_discard': 0,
                            'max_num_samples': 500000, 'relative_range': [0.875, 0.975], 'threshold_ratio': 0.9}
pdp_samples_file, start_timestamp_file, parsed_metadata_file = 'samples.log', 'timestamp.log', 'parsed_metadata.log'
tx_fc, rx_fc, pn_v0, pn_l, pn_m, wlength, n_sigma, max_ant_gain = 400e6, 399.95e6, 0.5, 11, 2047, c / 28e9, 0.015, 22.0
prefilter_config = {'passband_freq': 60e3, 'stopband_freq': 65e3, 'passband_ripple': 0.01, 'stopband_attenuation': 80.0}
min_threshold, sample_rate, datetime_format, pn_reps, max_mpcs = 1e5, 2e6, '%Y-%m-%d %H:%M:%S.%f', int(pn_m / pn_l), 100

"""
INITIALIZATIONS-III: Enumerations & Dataclasses (Temps | Outputs)
"""


@dataclass(order=True)
class MPCParameters:
    path_number: int = 0
    attenuation: float = 0.0
    delay: float = 0.0
    aoa_azimuth: float = 0.0
    aoa_elevation: float = 0.0
    doppler_shift: float = 0.0
    profile_point_power: float = 0.0


@dataclass(order=True)
class PDPSegment:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    sample_rate: float = 2e6
    rx_freq: float = 2.5e9
    item_size: int = 8
    core_header_length: int = 149
    extra_header_length: int = 22
    header_length: int = 171
    num_samples: int = 1000000
    num_bytes: int = num_samples * item_size
    raw_rx_samples: np.array = np.array([], dtype=np.csingle)
    processed_rx_samples: np.array = np.array([], dtype=np.csingle)
    correlation_peak: float = 0.0


@dataclass(order=True)
class Pod:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    tx_gps_event: GPSEvent = GPSEvent()
    rx_gps_event: GPSEvent = GPSEvent()
    tx_imu_trace: IMUTrace = IMUTrace()
    rx_imu_trace: IMUTrace = IMUTrace()
    tx_elevation: float = 0.0
    rx_elevation: float = 0.0
    tx_rx_alignment: float = 0.0
    tx_rx_distance_2d: float = 0.0
    tx_rx_distance_3d: float = 0.0
    pdp_segment: PDPSegment = PDPSegment()
    n_mpcs: int = max_mpcs
    mpc_parameters: Tuple[MPCParameters] = field(default_factory=lambda: (MPCParameters() for _ in range(max_mpcs)))
    rms_delay_spread: float = 0.0
    rms_aoa_dir_spread: float = 0.0


"""
CORE ROUTINES
"""


def pack_dict_into_dataclass(dict_: Dict, dataclass_: dataclass) -> dataclass:
    loaded_dict = {}
    fields = {f.name: f.type for f in dataclasses.fields(dataclass_)}

    for k, v in dict_.items():
        loaded_dict[k] = (lambda: v, lambda: pack_dict_into_dataclass(v, Member))[fields[k] == Member]()

    return dataclass_(**loaded_dict)


def yaw(m: IMUTrace) -> float:
    return m.yaw_angle


def pitch(m: IMUTrace) -> float:
    return m.pitch_angle


def latitude(y: GPSEvent) -> float:
    return y.latitude.component


def longitude(y: GPSEvent) -> float:
    return y.longitude.component


def altitude(y: GPSEvent) -> float:
    return y.altitude_ellipsoid.component


def tx_rx_distance_2d(tx: GPSEvent, rx: GPSEvent) -> float:
    coords_tx = (latitude(tx), longitude(tx))
    coords_rx = (latitude(rx), longitude(rx))
    return distance.distance(coords_tx, coords_rx).m


def tx_rx_distance_3d(tx: GPSEvent, rx: GPSEvent) -> float:
    alt_tx, alt_rx = altitude(tx), altitude(rx)
    return np.sqrt(np.square(tx_rx_distance_2d(tx, rx)) + np.square(alt_tx - alt_rx))


def d_alignment(y1: GPSEvent, y2: GPSEvent, m: IMUTrace, is_tx=True) -> Tuple:
    y1_lat, y1_lon, y1_alt = latitude(y1), longitude(y1), altitude(y1)
    y2_lat, y2_lon, y2_alt = latitude(y2), longitude(y2), altitude(y2)

    if is_tx:
        yaw_calc = np.arctan((y1_lat - y2_lat) / (np.cos(y1_lat * (pi / 180.0)) * (y1_lon - y2_lon))) * (180.0 / pi)
    else:
        yaw_calc = np.arctan((y2_lat - y1_lat) / (np.cos(y2_lat * (pi / 180.0)) * (y2_lon - y1_lon))) * (180.0 / pi)

    if y1_lat <= y2_lat:
        yaw_calc += 270.0 if yaw_calc >= 0.0 else 90.0
    else:
        yaw_calc += 90.0 if yaw_calc >= 0.0 else 270.0

    pitch_calc = np.arctan((y2_alt - y1_alt) / np.abs(np.cos(y1_lat * (pi / 180.0)) * (y1_lon - y2_lon))) * (180.0 / pi)
    pitch_calc /= 2

    if is_tx:
        pitch_calc *= np.dot(pitch_calc,
                             (lambda: 30.0, lambda: -30.0)[pitch_calc < 0.0]()) / np.dot(pitch_calc, pitch_calc)
    else:
        pitch_calc *= np.dot(pitch_calc,
                             (lambda: 5.0, lambda: -5.0)[pitch_calc < 0.0]()) / np.dot(pitch_calc, pitch_calc)

    return np.abs(yaw(m) - yaw_calc), np.abs(pitch(m) - pitch_calc)


def tx_rx_alignment(tx: GPSEvent, rx: GPSEvent, m_tx: IMUTrace, m_rx: IMUTrace) -> float:
    m_tx_yaw_, m_tx_pitch_ = d_alignment(tx, rx, m_tx)
    m_rx_yaw_, m_rx_pitch_ = d_alignment(rx, tx, m_rx, False)
    return 0.5 * (np.abs(180.0 - m_tx_yaw_ - m_rx_yaw_) + np.abs(180.0 - m_tx_pitch_ - m_rx_pitch_))


def tx_rx_relative_velocity(tx_i: GPSEvent, tx_j: GPSEvent, rx_i: GPSEvent, rx_j: GPSEvent) -> float:
    tx_i_lat, tx_i_lon = latitude(tx_i), longitude(tx_i)
    tx_j_lat, tx_j_lon = latitude(tx_j), longitude(tx_j)
    rx_i_lat, rx_i_lon = latitude(rx_i), longitude(rx_i)
    rx_j_lat, rx_j_lon = latitude(rx_j), longitude(rx_j)
    tx_i_dt = datetime.datetime.strptime(tx_i.timestamp, datetime_format)
    tx_j_dt = datetime.datetime.strptime(tx_j.timestamp, datetime_format)
    rx_i_dt = datetime.datetime.strptime(rx_i.timestamp, datetime_format)
    rx_j_dt = datetime.datetime.strptime(rx_j.timestamp, datetime_format)
    tx_v = distance.distance((tx_i_lat, tx_i_lon), (tx_j_lat, tx_j_lon)).m / (tx_j_dt - tx_i_dt).total_seconds()
    rx_v = distance.distance((rx_i_lat, rx_i_lon), (rx_j_lat, rx_j_lon)).m / (rx_j_dt - rx_i_dt).total_seconds()

    if ((tx_j_lat > tx_i_lat or tx_j_lon < tx_i_lon) and (rx_j_lat > rx_i_lat or rx_j_lon < rx_i_lon)) or \
            ((tx_j_lat < tx_i_lat or tx_j_lon > tx_i_lon) and (rx_j_lat < rx_i_lat or rx_j_lon > rx_i_lon)):
        return abs(tx_v - rx_v)
    else:
        return abs(tx_v + rx_v)


def elevation(y: GPSEvent) -> float:
    lat, lon, alt = latitude(y), longitude(y), altitude(y)
    base_epqs_url = 'https://nationalmap.gov/epqs/pqs.php?x={}&y={}output=json&units=Meters'
    epqs_kw, eq_kw, e_kw = 'USGS_Elevation_Point_Query_Service', 'Elevation_Query', 'Elevation'
    return abs(alt - requests.get(base_epqs_url.format(lat, lon)).json()[epqs_kw][eq_kw][e_kw])


def cart2sph(x: float, y: float, z: float) -> Tuple:
    return np.sqrt((x ** 2) + (y ** 2) + (z ** 2)), np.arctan2(z, np.sqrt((x ** 2) + (y ** 2))), np.arctan2(y, x)


def sph2cart(r: float, phi: float, theta: float) -> Tuple:
    return r * np.sin(theta) * np.cos(phi), r * np.sin(theta) * np.sin(phi), r * np.cos(theta)


def process_rx_samples(x: np.array) -> Tuple:
    fs = sample_rate
    t_mul, t_len = time_windowing_config.values()
    f_pass, f_stop, d_pass, d_stop = prefilter_config.values()
    ne_mul, min_peak_idx, n_min, n_max, rel_range, amp_threshold = noise_elimination_config.values()

    # Frequency Manipulation: Pre-filtering via a Low Pass Filter (LPF) [FIR filtering via SciPy-Scikit-Remez]
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
    samps_ = samps_[((np.where(a_samps > amp_threshold * max(a_samps))[0][0] + min_peak_idx - 1) *
                     np.array(rel_range)).astype(dtype=int)]
    th_min, th_max = np.array([-1, 1]) * ne_mul * np.std(samps_) + np.mean(samps_)
    thresholder = np.vectorize(lambda s: 0 + 0j if (s > th_min) and (s < th_max) else s)

    return n_samples, thresholder(samps)


# Correlation peak in the processed rx_samples
def correlation_peak(n: int, x: np.array) -> float:
    return max(np.fft.fft(x) / n)


# See Visualizations-I: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6691924]
def rms_delay_spread(mpcs: Tuple[MPCParameters]) -> float:
    num, den = [], []

    for mpc in mpcs:
        tau, p_tau = mpc.delay, mpc.profile_point_power
        num.append(np.square(tau) * p_tau)
        den.append(p_tau)
    num_sum, den_sum = np.sum(num), np.sum(den)

    return np.sqrt((num_sum / den_sum) - np.square((num_sum / den_sum)))


# See Visualizations-II: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=5956639]
def rms_direction_spread(mpcs: Tuple[MPCParameters]) -> float:
    e_vecs, p_vec, mu_vec = [], [], []

    for i_mpc in range(len(mpcs)):
        mpc = mpcs[i_mpc]
        p = mpc.profile_point_power
        phi, theta = mpc.aoa_azimuth, mpc.aoa_elevation
        e_vecs.append(np.array([np.cos(phi) * np.sin(theta), np.sin(phi) * np.sin(theta), np.cos(theta)]))
        mu_vec.append(p * e_vecs[i_mpc])
        p_vec.append(p)

    mu_omega = np.sum(mu_vec)
    return np.sqrt(np.sum([np.square(np.linalg.norm(e_vecs[i_mpc] -
                                                    mu_omega)) * p_vec[i_mpc] for i_mpc in range(len(mpcs))]))


# Relative drop in correlation peak between the two 'consecutive' PDPSegments (...Pods)
#   Increasing Tx-Rx distance, Tx-Rx alignment accuracy, and Tx-Rx relative velocity | corr_: previous | corr: current
def relcorr_drop(pdp_: PDPSegment, pdp: PDPSegment) -> float:
    corr_, corr = pdp_.correlation_peak, pdp.correlation_peak
    return (corr - corr_) / corr_


# SAGE Algorithm
# See: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=753729]
# Also see: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6901837]
def estimate_mpc_parameters(tx: GPSEvent, rx: GPSEvent, n: int, x: np.array) -> Tuple:
    f_c, _f_c = tx_fc, rx_fc
    f_s, n_std = sample_rate, n_sigma
    v_0, l_, k_ = pn_v0, pn_l, pn_reps
    y_f = np.square(np.fft.fft(x)) / n
    fs = np.argsort(np.fft.fftfreq(n, (1 / f_s)))
    n_f = (n_std * np.random.randn(fs.shape[0], )).view(np.csingle)

    # Parameter initialization
    nus = np.array([0.0 for _ in range(max_mpcs)])
    phis = np.array([0.0 for _ in range(max_mpcs)])
    taus = np.array([0.0 for _ in range(max_mpcs)])
    thetas = np.array([0.0 for _ in range(max_mpcs)])
    alphas = np.array([complex(0.0, 0.0) for _ in range(max_mpcs)])

    def flip(a: int) -> int:
        return 1 if a == -1 else -1

    def sinc(z: float) -> float:
        return np.sin(pi * z) / (pi * z)

    # Baseband signal component post-convolution in the frequency domain
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

    # Complex gaussian noise component post-convolution in the frequency domain
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
                    pn_sum += ((2 * a_i) - 1) * complex(np.cos(pi * ((_k * i) / l_)), np.sin(pi * ((_k * i) / l_)))
                    flip(a_i)

                f_shifts.append(f_shift)
                n_sum += pn_sum * n_f[idx_f] * sinc(k / l_) * complex(np.cos(pi * (_k / l_)), np.sin(pi * (_k / l_)))

        return f_shifts, (v_0 / l_) * n_sum

    # See [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=753729]
    # Angle-of-Arrival (AoA | azimuth & elevation) pattern at the Rx for signal (p) computation within SAGE
    def compute_steering(phi_l: float, theta_l: float) -> complex:
        az0, el0 = deg2rad(phi_l), deg2rad(theta_l)
        tx_lat, tx_lon, tx_alt = latitude(tx), longitude(tx), altitude(tx)
        rx_lat, rx_lon, rx_alt = latitude(rx), longitude(rx), altitude(rx)

        tx_e, tx_n = lla_utm_proj(tx_lon, tx_lat)
        rx_e, rx_n = lla_utm_proj(rx_lon, rx_lat)

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

        ld, pos_vec = wlength, np.array([xs, ys, zs])
        f_aoa = ((az_amps0 * abs(ys)) + (el_amps0 * abs(zs))) / (abs(ys) + abs(zs))
        e_vec = np.array([np.cos(phi_l) * np.sin(theta_l), np.sin(phi_l) * np.sin(theta_l), np.cos(theta_l)])

        c_theta = 2.0 * pi * (1 / ld) * np.dot(e_vec, pos_vec)
        return f_aoa * complex(np.cos(c_theta), np.sin(c_theta))

    # Expectation of the log-likelihood component
    def estep_component(l_idx: int, yf_s: np.array) -> np.array:
        return yf_s - np.sum(np.array([compute_steering(phis[l_mpc], thetas[l_mpc]) *
                                       alphas[l_mpc] * signal_component(taus[l_mpc], nus[l_mpc])[1]
                                       for l_mpc in range(max_mpcs) if l_mpc != l_idx], dtype=np.csingle))


"""
CORE OPERATIONS: Parsing the GPS, IMU, and PDP logs | SAGE estimation | Spatial consistency analysis setup
"""

# Antenna Pattern
log = scipy.io.loadmat(ant_log_file)
az_log, el_log = log['pat28GAzNorm'], log['pat28GElNorm']
az_angles, az_amps, az_amps_db = az_log.azs, az_log.amps, decibel_2(az_log.amps)
el_angles, el_amps, el_amps_db = el_log.els, el_log.amps, decibel_2(el_log.amps)

# Extract Rx gps_events
for filename in os.listdir(rx_gps_dir):
    with open(''.join([rx_gps_dir, filename])) as file:
        rx_gps_events.append(pack_dict_into_dataclass(json.load(file), GPSEvent))

# Extract Tx imu_traces
for filename in os.listdir(tx_imu_dir):
    with open(''.join([tx_imu_dir, filename])) as file:
        tx_imu_traces.append(pack_dict_into_dataclass(json.load(file), IMUTrace))

# Extract Rx imu_traces
for filename in os.listdir(rx_imu_dir):
    with open(''.join([rx_imu_dir, filename])) as file:
        rx_imu_traces.append(pack_dict_into_dataclass(json.load(file), IMUTrace))

# Extract timestamp_0 (start_timestamp)
with open(''.join([comm_dir, start_timestamp_file])) as file:
    elements = file.readline().split()
    timestamp_0 = datetime.datetime.strptime(''.join([elements[2], ' ', elements[3]]), datetime_format)

# Evaluate parsed_metadata | Extract power-delay profile samples
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
            pdp_segments.append(PDPSegment(seq_number=seq_number + 1, timestamp=str(timestamp), num_samples=num_samples,
                                           raw_rx_samples=raw_rx_samples, processed_rx_samples=processed_rx_samples,
                                           correlation_peak=correlation_peak(num_samples, processed_rx_samples)))

''' Match gps_event, Tx/Rx imu_trace, and pdp_segment timestamps '''

for rx_gps_event in rx_gps_events:
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
                    rms_delay_spread=rms_delay_spread(mpc_parameters),
                    tx_gps_event=tx_gps_event, tx_imu_trace=tx_imu_trace,
                    rx_gps_event=rx_gps_event, rx_imu_trace=rx_imu_trace,
                    rms_aoa_dir_spread=rms_direction_spread(mpc_parameters),
                    tx_rx_distance_2d=tx_rx_distance_2d(tx_gps_event, rx_gps_event),
                    tx_elevation=elevation(tx_gps_event), rx_elevation=elevation(rx_gps_event),
                    tx_rx_alignment=tx_rx_alignment(tx_gps_event, rx_gps_event, tx_imu_trace, rx_imu_trace),
                    pdp_segment=pdp_segment, tx_rx_distance_3d=tx_rx_distance_3d(tx_gps_event, rx_gps_event)))

"""
CORE VISUALIZATIONS-I: Spatial decoherence analyses 
"""

pods_acc = sorted(pods, key=lambda pod: pod.tx_rx_alignment)
pods_dist = sorted(pods, key=lambda pod: pod.tx_rx_distance_3d)

tuples_vel = [(pods_dist[0], 0.0)]
for i in range(len(pods_dist) - 1):
    podd_, podd = pods_dist[i], pods_dist[i + 1]
    tuples_vel.append((podd, tx_rx_relative_velocity(podd_.tx_gps_event, podd.tx_gps_event,
                                                     podd_.rx_gps_event, podd.rx_gps_event)))

pods_vel = sorted(tuples_vel, key=lambda tuple_vel: tuple_vel[1])

for i in range(len(pods_dist) - 1):
    poda_, poda = pods_acc[i], pods_acc[i + 1]
    podv_, podv = pods_vel[i], pods_vel[i + 1]
    podd_, podd = pods_dist[i], pods_dist[i + 1]
    velocities.append((podv[1] - podv_[1], relcorr_drop(podv_[0].pdp_segment, podv[0].pdp_segment)))
    alignments.append((poda.tx_rx_alignment - poda_.tx_rx_alignment, relcorr_drop(poda_.pdp_segment, poda.pdp_segment)))
    distns.append((podd.tx_rx_distance_3d - podd_.tx_rx_distance_3d, relcorr_drop(podd_.pdp_segment, podd.pdp_segment)))

scd_layout = dict(xaxis=dict(title='Tx-Rx 3D Distance (in m)'),
                  title='Spatial Consistency Analysis vis-à-vis Distance',
                  yaxis=dict(title='Relative Drop in Correlation Peak Magnitude'))
scd_trace = go.Scatter(x=[distn[0] for distn in distns], y=[distn[1] for distn in distns], mode='lines+markers')

sca_layout = dict(xaxis=dict(title='Tx-Rx Relative Alignment Accuracy'),
                  title='Spatial Consistency Analysis vis-à-vis Alignment',
                  yaxis=dict(title='Relative Drop in Correlation Peak Magnitude'))
sca_trace = go.Scatter(x=[align[0] for align in alignments], y=[align[1] for align in alignments], mode='lines+markers')

scv_layout = dict(xaxis=dict(title='Tx-Rx Relative Velocity (in m/s)'),
                  title='Spatial Consistency Analysis vis-à-vis Velocity',
                  yaxis=dict(title='Relative Drop in Correlation Peak Magnitude'))
scv_trace = go.Scatter(x=[veloc[0] for veloc in velocities], y=[veloc[1] for veloc in velocities], mode='lines+markers')

scv_url = plotly.plotly.plot(dict(data=[scv_trace], layout=scv_layout), filename=sc_velocity_png)
scd_url = plotly.plotly.plot(dict(data=[scd_trace], layout=scd_layout), filename=sc_distance_png)
sca_url = plotly.plotly.plot(dict(data=[sca_trace], layout=sca_layout), filename=sc_alignment_png)
print('SPAVE-28G | Consolidated Processing-II | Spatial Consistency Analysis vis-à-vis Velocity: {}'.format(scv_url))
print('SPAVE-28G | Consolidated Processing-II | Spatial Consistency Analysis vis-à-vis Distance: {}'.format(scd_url))
print('SPAVE-28G | Consolidated Processing-II | Spatial Consistency Analysis vis-à-vis Alignment: {}'.format(sca_url))

"""
CORE VISUALIZATIONS-II: RMS delay spread and RMS direction spread
"""

rms_delay_spreads = np.array([pod.rms_delay_spread for pod in pods])
rms_aoa_dir_spreads = np.array([pod.rms_aoa_dir_spread for pod in pods])

rms_delay_spread_pdf = rms_delay_spreads / np.sum(rms_delay_spreads)
rms_aoa_dir_spread_pdf = rms_aoa_dir_spreads / np.sum(rms_aoa_dir_spreads)

rms_ds_layout = dict(xaxis=dict(title='RMS Delay Spreads (x)'),
                     title='RMS Delay Spread Cumulative Distribution Function',
                     yaxis=dict(title=r'CDF Probability \mathbb{P}(S(\tau){\leq}x)'))
rms_aoa_dirs_layout = dict(xaxis=dict(title='RMS AoA Direction Spreads (x)'),
                           title='RMS AoA Direction Spread Cumulative Distribution Function',
                           yaxis=dict(title=r'CDF Probability \mathbb{P}(\sigma_{\Omega}{\leq}x)'))

rms_ds_trace = go.Scatter(x=rms_delay_spread_pdf, y=np.cumsum(rms_delay_spread_pdf), mode='lines+markers')
rms_aoa_dirs_trace = go.Scatter(x=rms_aoa_dir_spread_pdf, y=np.cumsum(rms_aoa_dir_spread_pdf), mode='lines+markers')

rms_aoa_dirs_url = plotly.plotly.plot(dict(data=[rms_aoa_dirs_trace],
                                           layout=rms_aoa_dirs_layout), filename=aoa_rms_dir_spread_png)
rms_ds_url = plotly.plotly.plot(dict(data=[rms_ds_trace], layout=rms_ds_layout), filename=rms_delay_spread_png)

print('SPAVE-28G | Consolidated Processing-II | RMS Delay Spread CDF: {}'.format(rms_ds_url))
print('SPAVE-28G | Consolidated Processing-II | RMS AoA Direction Spread CDF: {}'.format(rms_aoa_dirs_url))
