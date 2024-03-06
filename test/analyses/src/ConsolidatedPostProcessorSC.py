"""
This script encapsulates the operations involved in our initial version of spatial decoherence analyses
with respect to Tx-Rx distance, Tx-Rx alignment accuracy, and Tx-Rx relative velocity.

<Older version of computing the spatial autocorrelation coefficient w.r.t distance, alignment, and velocity>

Reference Papers:

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

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

import os
import re
import json
import plotly
import datetime
import scipy.io
import dataclasses
import numpy as np
from enum import Enum
from scipy import signal
from geopy import distance
import plotly.graph_objs as go
from json import JSONDecodeError
from dataclasses import dataclass
from typing import Tuple, List, Dict
import sk_dsp_comm.fir_design_helper as fir_d
from concurrent.futures import ThreadPoolExecutor

"""
INITIALIZATIONS I: Collections & Utilities
"""
pi, distns, alignments, velocities = np.pi, [], [], []
deg2rad, rad2deg = lambda x: x * (pi / 180.0), lambda x: x * (180.0 / pi)
rx_gps_events, tx_imu_traces, rx_imu_traces, pdp_segments, pods = [], [], [], [], []

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
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-campus-I/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-campus-I/tx-realm/imu/', 1
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'uc_sc_dist.png', 'uc_sc_alignment.png', 'uc_sc_vel.png'

''' urban-campus-II route (fully-autonomous) (President's Circle) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-campus-II/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-campus-II/tx-realm/imu/', 5
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'ucc_sc_dist.png', 'ucc_sc_alignment.png', 'ucc_sc_vel.png'

''' urban-campus-III route (fully-autonomous) (100 S St) '''
comm_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/pdp/'
rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/gps/'
rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-campus-III/rx-realm/imu/'
tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-campus-III/tx-realm/imu/', 5
sc_distance_png, sc_alignment_png, sc_velocity_png = 'uccc_sc_dist.png', 'uccc_sc_alignment.png', 'uccc_sc_vel.png'

''' urban-garage route (fully-autonomous) (NW Garage on 1460 E St) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-garage/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-garage/tx-realm/imu/', 1
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'ug_sc_distance.png', 'ug_sc_alignment.png', 'ug_sc_velocity.png'

''' urban-stadium route (fully-autonomous) (E South Campus Dr) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-stadium/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-stadium/tx-realm/imu/', 5
# rms_delay_spread_png, aoa_rms_dir_spread_png = 'us_rms_delay_spread.png', 'us_aoa_rms_dir_spread.png'
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'us_sc_distance.png', 'us_sc_alignment.png', 'us_sc_velocity.png'

''' suburban-fraternities route (fully-autonomous) (S Wolcott St) '''
# comm_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/suburban-fraternities/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/suburban-fraternities/tx-realm/imu/', 1
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'sf_sc_distance.png', 'sf_sc_alignment.png', 'sf_sc_velocity.png'

''' urban-vegetation route (fully-autonomous) (Olpin Union Bldg) '''
# comm_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/pdp/'
# rx_gps_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/gps/'
# rx_imu_dir = 'E:/SPAVE-28G/analyses/urban-vegetation/rx-realm/imu/'
# tx_imu_dir, tx_imu_skip_step = 'E:/SPAVE-28G/analyses/urban-vegetation/tx-realm/imu/', 1
# sc_distance_png, sc_alignment_png, sc_velocity_png = 'uv_sc_distance.png', 'uv_sc_alignment.png', 'uv_sc_velocity.png'

''' Tx location fixed on the rooftop of the William Browning Building in SLC, UT '''
tx_gps_event = GPSEvent(latitude=Member(component=40.766173670),
                        longitude=Member(component=-111.847939330), altitude_ellipsoid=Member(component=1459.1210))

''' Generic configurations '''
ant_log_file = 'E:/SPAVE-28G/analyses/antenna_pattern.mat'
ne_amp_threshold, max_workers, sg_wsize, sg_poly_order = 0.05, 4096, 53, 3
min_threshold, sample_rate, datetime_format = 1e5, 2e6, '%Y-%m-%d %H:%M:%S.%f'
d_max, d_step, a_max, a_step, v_max, v_step = 500.0, 1.0, 10.0, 0.05, 10.0, 0.1
plotly.tools.set_credentials_file(username='total.academe', api_key='Xt5ic4JRgdvH8YuKmjEF')
time_windowing_config = {'window_multiplier': 2.0, 'truncation_length': int(2e5), 'truncation_multiplier': 4.0}
pdp_samples_file, start_timestamp_file, parsed_metadata_file = 'samples.log', 'timestamp.log', 'parsed_metadata.log'
prefilter_config = {'passband_freq': 60e3, 'stopband_freq': 65e3, 'passband_ripple': 0.01, 'stopband_attenuation': 80.0}

"""
INITIALIZATIONS III: Enumerations & Dataclasses (Temps | Outputs)
"""


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
    correlation_peak: float = 0.0  # linear


@dataclass(order=True)
class Pod:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    tx_gps_event: GPSEvent = GPSEvent()
    rx_gps_event: GPSEvent = GPSEvent()
    tx_imu_trace: IMUTrace = IMUTrace()
    rx_imu_trace: IMUTrace = IMUTrace()
    tx_rx_alignment: float = 0.0  # deg
    tx_rx_distance_2d: float = 0.0  # m
    tx_rx_distance_3d: float = 0.0  # m
    pdp_segment: PDPSegment = PDPSegment()


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


# Correlation peak in the processed rx_samples (linear)
def correlation_peak(x: np.array) -> float:
    return np.max(np.abs(x))


# Spatial autocorrelation coefficient computation
# See [https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=7996408]
# Increasing Tx-Rx distance, Tx-Rx alignment accuracy, and Tx-Rx relative velocity | corr: prev | corr_: curr
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


"""
CORE OPERATIONS: Parsing the GPS, IMU, and PDP logs | SAGE estimation | Spatial consistency analyses
"""

# noinspection PyUnresolvedReferences
log = scipy.io.loadmat(ant_log_file)  # Antenna patterns
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
            print('SPAVE-28G | Consolidated Processing SC | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing SC | Exception caught while parsing {}.'.format(filename))
            continue  # Ignore the Exception on this file | Move onto the next file...

# Extract Tx imu_traces
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(0, len(os.listdir(tx_imu_dir)), tx_imu_skip_step):
        filename = 'imu_trace_{}.json'.format(i + 1)

        # noinspection PyBroadException
        try:
            parse(tx_imu_traces, IMUTrace, ''.join([tx_imu_dir, filename]))
        except JSONDecodeError as jde:
            print('SPAVE-28G | Consolidated Processing SC | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing SC | Exception caught while parsing {}.'.format(filename))
            continue  # Ignore the Exception on this file | Move onto the next file...

# Extract Rx imu_traces
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(len(os.listdir(rx_imu_dir))):
        filename = 'imu_trace_{}.json'.format(i + 1)

        # noinspection PyBroadException
        try:
            parse(rx_imu_traces, IMUTrace, ''.join([rx_imu_dir, filename]))
        except JSONDecodeError as jde:
            print('SPAVE-28G | Consolidated Processing SC | JSONDecodeError caught while parsing {}.'.format(filename))
            continue  # Ignore the JSONDecodeError on this file | Move onto the next file...
        except Exception as e:
            print('SPAVE-28G | Consolidated Processing SC | Exception caught while parsing {}.'.format(filename))
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
                                           correlation_peak=correlation_peak(processed_rx_samples),
                                           raw_rx_samples=raw_rx_samples, processed_rx_samples=processed_rx_samples))

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

    pods.append(Pod(seq_number=seq_number, timestamp=timestamp,
                    tx_gps_event=tx_gps_event, tx_imu_trace=IMUTrace(),
                    rx_gps_event=rx_gps_event, rx_imu_trace=IMUTrace(),
                    tx_rx_distance_2d=tx_rx_distance_2d(tx_gps_event, rx_gps_event),
                    tx_rx_alignment=tx_rx_alignment(tx_gps_event, rx_gps_event, IMUTrace(), IMUTrace()),
                    pdp_segment=pdp_segment, tx_rx_distance_3d=tx_rx_distance_3d(tx_gps_event, rx_gps_event)))

"""
CORE VISUALIZATIONS: Spatial decoherence analyses
"""

idxs = [_i for _i in range(len(pods))]
pod = max(pods, key=lambda _pod: _pod.pdp_segment.correlation_peak)

for dn in np.arange(start=0.0, stop=d_max, step=d_step):
    '''
    # With Tx-Rx alignment more or less the same, compute s_coeff w.r.t "pod" for the pod with the dist closest to "dn"
    i_ = min(idxs, key=lambda idx: abs(pod.tx_rx_alignment - pods[idx].tx_rx_alignment) +
                                   abs(dn - distance_3d(pod.rx_gps_event, pods[idx].rx_gps_event)))
    '''
    i_ = min(idxs, key=lambda idx: abs(dn - distance_3d(pod.rx_gps_event, pods[idx].rx_gps_event)))
    distns.append((dn, s_coeff(pod.pdp_segment, pods[i_].pdp_segment)))

for an in np.arange(start=0.0, stop=a_max, step=a_step):
    '''
    # With Tx-Rx distance more or less the same, compute s_coeff w.r.t "pod" for the pod with alignment closest to "an"
    i_ = min(idxs, key=lambda idx: abs(distance_3d(pod.rx_gps_event, pods[idx].rx_gps_event)) +
                                   abs(an - abs(pod.tx_rx_alignment - pods[idx].tx_rx_alignment)))
    '''
    i_ = min(idxs, key=lambda idx: abs(an - abs(pod.tx_rx_alignment - pods[idx].tx_rx_alignment)))
    alignments.append((an, s_coeff(pod.pdp_segment, pods[i_].pdp_segment)))

for vn in np.arange(start=0.0, stop=v_max, step=v_step):
    '''
    # With Tx-Rx dist and align more or less the same, compute s_coeff w.r.t "pod" for the pod with vel closest to "vn"
    i_ = min(idxs, key=lambda idx: abs(pod.tx_rx_alignment - pods[idx].tx_rx_alignment) +
                                   abs(distance_3d(pod.rx_gps_event, pods[idx].rx_gps_event)) +
                                   abs(vn - tx_rx_relative_velocity(pod.tx_gps_event, pods[idx].tx_gps_event,
                                                                    pod.rx_gps_event, pods[idx].rx_gps_event)))
    '''
    i_ = min(idxs, key=lambda idx: abs(vn - tx_rx_relative_velocity(pod.tx_gps_event,
                                                                    pod.rx_gps_event, pods[idx].rx_gps_event)))
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

sca_layout = dict(xaxis=dict(title='Tx-Rx Relative Alignment Accuracy (in deg)'),
                  yaxis=dict(title='Spatial (Angular) Autocorrelation Coefficient'),
                  title='Spatial (Angular) Consistency Analysis vis-à-vis Alignment')
sca_trace = go.Scatter(x=[alignment[0] for alignment in alignments], mode='lines+markers',
                       y=signal.savgol_filter([alignment[1] for alignment in alignments], sg_wsize, sg_poly_order))

scd_url = plotly.plotly.plot(dict(data=[scd_trace], layout=scd_layout), filename=sc_distance_png)
scv_url = plotly.plotly.plot(dict(data=[scv_trace], layout=scv_layout), filename=sc_velocity_png)
sca_url = plotly.plotly.plot(dict(data=[sca_trace], layout=sca_layout), filename=sc_alignment_png)

print('SPAVE-28G | Consolidated Processing SC | Spatial Consistency Analysis vis-à-vis Distance: {}.'.format(scd_url))
print('SPAVE-28G | Consolidated Processing SC | Spatial Consistency Analysis vis-à-vis Velocity: {}.'.format(scv_url))
print('SPAVE-28G | Consolidated Processing SC | Spatial Consistency Analysis vis-à-vis Alignment: {}.'.format(sca_url))
