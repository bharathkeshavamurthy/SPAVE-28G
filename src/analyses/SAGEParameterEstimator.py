"""
This script encapsulates the operations involved in estimating the propagation parameters associated with the various
Multi-Path Components (MPCs) in our 28-GHz outdoor measurement campaign on the POWDER testbed in Salt Lake City, UT.

The constituent algorithm is Space-Alternating Generalized Expectation-Maximization (SAGE), derived from the following
reference paper:

@INPROCEEDINGS{SAGE,
  author={Yin, Xuefeng and He, Yongyu and Song, Zinuo and Kim, Myung-Don and Chung, Hyun Kyu},
  booktitle={The 8th European Conference on Antennas and Propagation (EuCAP 2014)},
  title={A sliding-correlator-based SAGE algorithm for Mm-wave wideband channel parameter estimation},
  year={2014},
  pages={625-629},
  doi={10.1109/EuCAP.2014.6901837}}

The plots resulting from this analysis script include RMS delay-spread CDF, RMS direction-spread CDF, and spatial
decoherence characteristics under Tx-Rx distance and Tx-Rx alignment accuracy variations, i.e., the relative drop in
the observed time-dilated cross-correlation peak magnitudes (w.r.t previous 'x') under distance and alignment effects.

Author: Bharath Keshavamurthy <bkeshav1@asu.edu | bkeshava@purdue.edu>
Organization: School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ
              School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
Copyright (c) 2022. All Rights Reserved.
"""

# The imports
import os
import re
import json
import datetime
import dataclasses
import numpy as np
from enum import Enum
from scipy import signal
from geopy import distance
from typing import Dict, List
from dataclasses import dataclass, field
import sk_dsp_comm.fir_design_helper as fir_d

"""
INITIALIZATIONS-I: Collections & Utilities
"""
pi, rx_gps_events, tx_imu_traces, rx_imu_traces, pdp_segments, pods = np.pi, [], [], [], [], []
decibel_1, decibel_2, gamma = lambda x: 10 * np.log10(x), lambda x: 20 * np.log10(x), lambda fc, fc_: fc / (fc - fc_)

"""
INITIALIZATIONS-II: Enumerations & Dataclasses (Inputs)
"""


@dataclass(order=True)
class IMUTrace:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    yaw_angle: float = 0.0
    pitch_angle: float = 0.0


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


"""
CONFIGURATIONS: Input & Output Dirs | GPS logs | Power delay profiles
"""

comm_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/pdp/'
output_dir = 'C:/Users/kesha/Workspaces/SPAVE-28G/test/analyses/'
rx_gps_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/gps/'
tx_imu_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/tx-realm/imu/'
rx_imu_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/imu/'
sc_distance_png, sc_alignment_png = 'sc_distance.png', 'sc_alignment.png'
delay_spread_png, direction_spread_png = 'delay_spread.png', 'direction_spread.png'
pdp_samples_file, start_timestamp_file, parsed_metadata_file = 'samples.log', 'timestamp.log', 'parsed_metadata.log'

tx_gps_event = GPSEvent(latitude=Member(component=40.766173670),
                        longitude=Member(component=-111.847939330),
                        altitude_ellipsoid=Member(component=1459.1210))

time_windowing_config = {'multiplier': 0.5, 'truncation_length': 200000}
min_threshold, sample_rate, datetime_format = 1e5, 2e6, '%Y-%m-%d %H:%M:%S.%f'
tx_fc, rx_fc_, pn_v0, pn_l, pn_m, max_mpcs = 400e6, 399.95e6, 5.0, 2047, 11, 30
noise_elimination_config = {'multiplier': 3.5, 'min_peak_index': 2000, 'num_samples_discard': 0,
                            'max_num_samples': 500000, 'relative_range': [0.875, 0.975], 'threshold_ratio': 0.9}
prefilter_config = {'passband_freq': 60e3, 'stopband_freq': 65e3, 'passband_ripple': 0.01, 'stopband_attenuation': 80.0}

"""
INITIALIZATIONS-III: Enumerations & Dataclasses (Temps | Outputs)
"""


@dataclass(order=True)
class MPCParameters:
    path_number: int = 0
    attenuation: float = 0.0
    delay: float = 0.0
    aod_azimuth: float = 0.0
    aod_elevation: float = 0.0
    aoa_azimuth: float = 0.0
    aoa_elevation: float = 0.0
    doppler_shift: float = 0.0
    profile_point_power: float = 0.0
    rms_delay_spread: float = 0.0
    rms_tx_direction_spread: float = 0.0
    rms_rx_direction_spread: float = 0.0


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
    n_mpcs: int = max_mpcs
    mpc_parameters: List[MPCParameters] = field(default_factory=lambda: [MPCParameters() for _ in range(max_mpcs)])


@dataclass(order=True)
class Pod:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    tx_gps_event: GPSEvent = GPSEvent()
    rx_gps_event: GPSEvent = GPSEvent()
    tx_imu_trace: IMUTrace = IMUTrace()
    rx_imu_trace: IMUTrace = IMUTrace()
    pdp_segment: PDPSegment = PDPSegment()
    tx_rx_distance: float = 0.0
    tx_rx_alignment: float = 0.0


"""
CORE ROUTINES
"""


def yaw(m: IMUTrace) -> float:
    return m.yaw_angle


def pitch(m: IMUTrace) -> float:
    return m.pitch_angle


def pack_dict_into_dataclass(dict_: Dict, dataclass_: dataclass) -> dataclass:
    loaded_dict = {}
    fields = {f.name: f.type for f in dataclasses.fields(dataclass_)}
    for k, v in dict_.items():
        loaded_dict[k] = (lambda: v, lambda: pack_dict_into_dataclass(v, Member))[fields[k] == Member]()
    return dataclass_(**loaded_dict)


def latitude(y: GPSEvent) -> float:
    return y.latitude.component


def longitude(y: GPSEvent) -> float:
    return y.longitude.component


def altitude(y: GPSEvent) -> float:
    return y.altitude_ellipsoid.component


def tx_rx_distance(tx: GPSEvent, rx: GPSEvent) -> float:
    coords_1 = (latitude(tx), longitude(rx))
    coords_2 = (latitude(rx), longitude(rx))
    return distance.distance(coords_1, coords_2).m


def d_alignment(y1: GPSEvent, y2: GPSEvent, m: IMUTrace, is_tx=True) -> List[float]:
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

    return [np.abs(yaw(m) - yaw_calc), np.abs(pitch(m) - pitch_calc)]


def tx_rx_alignment(tx: GPSEvent, rx: GPSEvent, m_tx: IMUTrace, m_rx: IMUTrace) -> float:
    m_tx_yaw_, m_tx_pitch_ = d_alignment(tx, rx, m_tx)
    m_rx_yaw_, m_rx_pitch_ = d_alignment(rx, tx, m_rx, False)

    d_yaw = np.abs(180.0 - m_tx_yaw_ - m_rx_yaw_)
    d_pitch = np.abs(180.0 - m_tx_pitch_ - m_rx_pitch_)
    return 0.5 * (d_yaw + d_pitch)


def process_rx_samples(x: np.array) -> np.array:
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
    return thresholder(samps)


# See: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6691924]
def rms_delay_spread(pdp: PDPSegment) -> float:
    num, den = [], []
    for i_mpc in range(pdp.n_mpcs):
        mpc = pdp.mpc_parameters[i_mpc]
        tau, p_tau = mpc.delay, mpc.profile_point_power
        num.append(np.square(tau) * p_tau)
        den.append(p_tau)

    num_sum, den_sum = np.sum(num), np.sum(den)
    return np.sqrt((num_sum / den_sum) - np.square((num_sum / den_sum)))


# See: [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=5956639]
def rms_direction_spread(pdp: PDPSegment, is_aod=True) -> float:
    e_vecs, p_vec, mu_vec = [], [], []

    for i_mpc in range(pdp.n_mpcs):
        mpc = pdp.mpc_parameters[i_mpc]
        p = mpc.profile_point_power
        if is_aod:
            phi, theta = mpc.aod_azimuth, mpc.aod_elevation
        else:
            phi, theta = mpc.aoa_azimuth, mpc.aoa_elevation
        e_vecs.append(np.array([np.cos(phi) * np.sin(theta), np.sin(phi) * np.sin(theta), np.cos(theta)]))
        mu_vec.append(p * e_vecs[i_mpc])
        p_vec.append(p)

    mu_omega = np.sum(mu_vec)
    return np.sqrt(np.sum([np.square(np.linalg.norm(e_vecs[i_mpc] -
                                                    mu_omega)) * p_vec[i_mpc] for i_mpc in range(pdp.n_mpcs)]))


def estimate_mpc_parameters(rx_samples: np.array) -> List[MPCParameters]:
    """
    SAGE Algorithm
    """
    return [MPCParameters()]


"""
CORE OPERATIONS-I: Parsing the GPS, IMU, and PDP logs
"""

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
                                         offset=seq_number * num_samples,
                                         count=num_samples, dtype=np.csingle)

            if np.isnan(raw_rx_samples).any() or np.abs(np.min(raw_rx_samples)) > min_threshold:
                continue

            processed_rx_samples = process_rx_samples(raw_rx_samples)
            n_mpcs, mpc_parameters = estimate_mpc_parameters(processed_rx_samples)
            pdp_segments.append(PDPSegment(seq_number=seq_number + 1, timestamp=str(timestamp),
                                           num_samples=num_samples, raw_rx_samples=raw_rx_samples,
                                           processed_rx_samples=processed_rx_samples, n_mpcs=n_mpcs,
                                           correlation_peak=-np.inf, mpc_parameters=mpc_parameters))

# Match gps_event, Tx/Rx imu_trace, and pdp_segment timestamps
for rx_gps_event in rx_gps_events:
    seq_number, timestamp = rx_gps_event.seq_number, rx_gps_event.timestamp

    pdp_segment = min(pdp_segments, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                      datetime.datetime.strptime(x.timestamp, datetime_format)))

    tx_imu_trace = min(tx_imu_traces, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                        datetime.datetime.strptime(x.timestamp, datetime_format)))
    rx_imu_trace = min(rx_imu_traces, key=lambda x: abs(datetime.datetime.strptime(timestamp, datetime_format) -
                                                        datetime.datetime.strptime(x.timestamp, datetime_format)))

    pods.append(Pod(seq_number=seq_number, timestamp=timestamp,
                    tx_gps_event=tx_gps_event, tx_imu_trace=tx_imu_trace,
                    rx_gps_event=rx_gps_event, rx_imu_trace=rx_imu_trace,
                    pdp_segment=pdp_segment, tx_rx_distance=tx_rx_distance(tx_gps_event, rx_gps_event),
                    tx_rx_alignment=tx_rx_alignment(tx_gps_event, rx_gps_event, tx_imu_trace, rx_imu_trace)))
