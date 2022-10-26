"""
This script encapsulates the operations involved in visualizing the antenna patterns (elevation & azimuth) and Rx power
maps of our mmWave (28 GHz) V2X propagation modeling activities on the POWDER testbed in Salt Lake City. Subsequently,
this script generates the path-loss maps of the routes traversed during this measurement campaign, along with plots
of path-loss/path-gain versus distance which will help us with next-generation mmWave V2V/V2I network design.

Also, we conduct comparisons against the ITU-R M.2135 and the 3GPP TR38.901 outdoor UMa large-scale path-loss standards.

Reference Papers:

@INPROCEEDINGS{PL-Models-I,
  author={Haneda, Katsuyuki and Zhang, Jianhua and Tan, Lei and Liu, Guangyi and Zheng, Yi and Asplund, Henrik, et al.},
  booktitle={2016 IEEE 83rd Vehicular Technology Conference (VTC Spring)},
  title={5G 3GPP-Like Channel Models for Outdoor Urban Microcellular and Macrocellular Environments},
  year={2016},
  volume={},
  number={},
  pages={1-7},
  doi={10.1109/VTCSpring.2016.7503971}}

@ARTICLE{PL-Models-II,
  author={Rappaport, Theodore S. and Xing, Yunchou and MacCartney, George R. and Molisch, Andreas F. et al.},
  journal={IEEE Transactions on Antennas and Propagation},
  title={Overview of mmWave Communications for 5G Wireless Networks—With a Focus on Propagation Models},
  year={2017},
  volume={65},
  number={12},
  pages={6213-6230},
  doi={10.1109/TAP.2017.2734243}}

Author: Bharath Keshavamurthy <bkeshav1@asu.edu | bkeshava@purdue.edu>
Organization: School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ
              School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
Copyright (c) 2022. All Rights Reserved.
"""

# The imports
import os
import re
import json
import plotly
import scipy.io
import datetime
import dataclasses
import numpy as np
import pandas as pd
from enum import Enum
from typing import Dict, Tuple
from bokeh.plotting import gmap
from bokeh.io import export_png
from dataclasses import dataclass
from bokeh.palettes import brewer
from collections import namedtuple
from scipy import signal, integrate
from scipy.interpolate import interp1d
import sk_dsp_comm.fir_design_helper as fir_d
from bokeh.models import GMapOptions, ColumnDataSource, ColorBar, LinearColorMapper

"""
INITIALIZATIONS-I: Collections & Utilities
"""
gps_events, pdp_segments, pods, calc_pwrs = [], [], [], []
decibel_1, decibel_2 = lambda x: 10 * np.log10(x), lambda x: 20 * np.log10(x)
pattern = namedtuple('pattern', ['angles', 'amplitudes', 'amplitudes_db', 'angles_hpbw', 'powers_hpbw', 'hpbw'])

"""
INITIALIZATIONS-II: Enumerations & Dataclasses
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
    rx_samples: np.array = np.array([], dtype=np.csingle)
    rx_power: float = 0.0


@dataclass(order=True)
class Pod:
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    gps_event: GPSEvent = GPSEvent()
    pdp_segment: PDPSegment = PDPSegment()


"""
CONFIGURATIONS-I: Input & Output Dirs | GPS logs | Power delay profiles | Antenna pattern logs
"""
ant_log_file = 'D:/SPAVE-28G/analyses/antenna_pattern.mat'
gps_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/gps'
comm_dir = 'D:/SPAVE-28G/analyses/urban-campus-II/rx-realm/pdp'
output_dir = 'C:/Users/kesha/Workspaces/SPAVE-28G/test/analyses/'
az_pat_png, el_pat_png, = 'az-antenna-pattern.png', 'el-antenna-pattern.png'
az_pat_3d_png, el_pat_3d_png = 'az-antenna-pattern-3d.png', 'el-antenna-pattern-3d.png'
rx_pwr_png, pathloss_png = 'urban-campus-II-pathloss.png', 'urban-campus-II-rx-power.png'
pdp_samples_file, start_timestamp_file, parsed_metadata_file = 'samples.log', 'timestamp.log', 'parsed_metadata.log'
att_indices, cali_metadata_file_left, cali_metadata_file_right = list(range(0, 30, 2)), 'u76_', '_parsed_metadata.log'
cali_dir, cali_samples_file_left, cali_samples_file_right = 'D:/SPAVE-28G/analyses/calibration', 'u76_', '_samples.log'

"""
CONFIGURATIONS-II: Data post-processing parameters | Time-windowing | Pre-filtering | Noise elimination
"""
max_ant_gain, angle_res_ext = 22.0, 5.0
rx_gain, sample_rate, invalid_min_magnitude = 76.0, 2e6, 1e5
time_windowing_config = {'multiplier': 0.5, 'truncation_length': 200000}
noise_elimination_config = {'multiplier': 3.5, 'min_peak_index': 2000, 'num_samples_discard': 0,
                            'max_num_samples': 500000, 'relative_range': [0.875, 0.975], 'threshold_ratio': 0.9}
# The reference received powers observed on a spectrum analyzer for calibration fit comparisons [at 76dB USRP gain]
meas_pwrs = [-39.6, -42.1, -44.6, -47.1, -49.6, -52.1, -54.6, -57.1, -59.6, -62.1, -64.6, -67.1, -69.6, -72.1, -74.6]
prefilter_config = {'passband_freq': 60e3, 'stopband_freq': 65e3, 'passband_ripple': 0.01, 'stopband_attenuation': 80.0}

"""
CONFIGURATIONS-III: Bokeh & Plotly visualization options | Antenna patterns | Rx power maps | Path-loss maps
"""
color_bar_layout_location, color_palette, color_palette_index = 'right', 'RdYlGn', 11
plotly.tools.set_credentials_file(username='bkeshav1', api_key='CLTFaBmP0KN7xw1fUheu')
map_width, map_height, map_zoom_level, map_title = 5500.0, 2800.0, 20, 'urban-campus-II'
tx_pin_size, tx_pin_alpha, tx_pin_color, rx_pins_size, rx_pins_alpha = 80, 1.0, 'red', 50, 1.0
google_maps_api_key, map_type, timeout = 'AIzaSyDzb5CB4L9l42MyvSmzvaSZ3bnRINIjpUk', 'hybrid', 300
color_bar_width, color_bar_height, color_bar_label_size, color_bar_orientation = 125, 2700, '125px', 'vertical'
map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7651), longitude=Member(component=-111.8500))
tx_location = GPSEvent(seq_number=-1, latitude=Member(component=40.76617367), longitude=Member(component=-111.84793933))

"""
CORE ROUTINES
"""


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


def rx_power(y: PDPSegment) -> float:
    return y.rx_power


def compute_rx_power(x: np.array) -> float:
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
    samps = thresholder(samps)

    # PSD Evaluation: Received signal power computation (calibration or campaign)
    pwr_values = np.square(np.abs(np.fft.fft(samps))) / n_samples
    freq_values = np.fft.fftfreq(n_samples, (1 / fs))
    indices = np.argsort(freq_values)

    # Trapezoidal numerical integration to compute signal power at the Rx from the organized PSD data
    computed_rx_power = integrate.trapz(y=pwr_values[indices], x=freq_values[indices])
    if computed_rx_power != 0.0:
        return 10 * np.log10(computed_rx_power) - rx_gain
    return -np.inf


def hpbw(angles: np.array, amps: np.array, max_gain: float) -> Tuple:
    angles_ = np.arange(start=0.0, stop=360.0, step=360.0 / (angle_res_ext * len(angles)))

    # 1D interpolation of the angles and amplitudes parsed from the antenna pattern logs
    amps_ = interp1d(angles, amps)(angles_)
    amps_th_idx = np.where(amps_ <= max_gain - 3.0)[0]

    # Threshold index window determination for HPBW computation
    low, high = amps_th_idx[0], amps_th_idx[-1]
    angles_hpbw, powers_hpbw = (angles_[low], angles_[high]), (amps_[low], amps_[high])
    return angles_hpbw, powers_hpbw, angles_hpbw[0] + 360.0 + angles_hpbw[1]


"""
CORE OPERATIONS-I: Calibration
"""

# Evaluate parsed_metadata | Extract power-delay profile samples | Compute received power in this calibration paradigm
for att_val in att_indices:
    with open(''.join([cali_dir, cali_metadata_file_left, 'a', att_val, cali_metadata_file_right])) as file:
        for line_num, line in enumerate(file):
            if (line_num - 11) % 18 == 0:
                num_samples = int(re.search(r'\d+', line)[0])
            cali_samples_file = ''.join([cali_dir, cali_samples_file_left, 'a', att_val, cali_samples_file_right])
            cali_samples = np.fromfile(cali_samples_file, count=num_samples, dtype=np.csingle)
            calc_pwrs.append(compute_rx_power(cali_samples))

# 1D interpolation of the measured and calculated powers for curve fitting
meas_pwrs_ = np.arange(start=0.0, stop=-80.0, step=-2.0)
calc_pwrs_ = interp1d(meas_pwrs, calc_pwrs)(meas_pwrs_)
cali_fit = lambda cpwr: meas_pwrs_[min([_ for _ in range(len(calc_pwrs_))], key=lambda x: abs(cpwr - calc_pwrs_[x]))]

"""
CORE OPERATIONS-II: Antenna patterns
"""

log = scipy.io.loadmat(ant_log_file)
az_log, el_log = log['pat28GAzNorm'], log['pat28GElNorm']

az_angles, az_amps, az_amps_db = az_log.azs, az_log.amps, decibel_2(az_log.amps)
az_angles_hpbw, az_powers_hpbw, az_hpbw = hpbw(az_angles, az_amps_db, max_ant_gain)
az_pattern = pattern(az_angles, az_amps, az_amps_db, az_angles_hpbw, az_powers_hpbw, az_hpbw)

el_angles, el_amps, el_amps_db = el_log.els, el_log.amps, decibel_2(el_log.amps)
el_angles_hpbw, el_powers_hpbw, el_hpbw = hpbw(el_angles, el_amps_db, max_ant_gain)
el_pattern = pattern(el_angles, el_amps, el_amps_db, el_angles_hpbw, el_powers_hpbw, el_hpbw)

"""
CORE OPERATIONS-III: Received power maps
"""

# Extract gps_event
for filename in os.listdir(gps_dir):
    with open(''.join([gps_dir, filename])) as file:
        gps_events.append(pack_dict_into_dataclass(json.load(file), GPSEvent))

# Extract timestamp_0 (start_timestamp)
with open(''.join([comm_dir, start_timestamp_file])) as file:
    elements = file.readline().split()
    timestamp_0 = datetime.datetime.strptime(''.join([elements[2], ' ', elements[3]]), '%Y-%m-%d %H:%M:%S.%f')

# Evaluate parsed_metadata | Extract power-delay profile samples
segment_done = False
timestamp_ref = datetime.datetime.strptime(gps_events[0].timestamp, '%Y-%m-%d %H:%M:%S.%f')
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

        if segment_done:
            segment_done = False
            rx_samples = np.fromfile(pdp_samples_file, offset=seq_number * num_samples,
                                     count=num_samples, dtype=np.csingle)
            if np.isnan(rx_samples).any() or np.abs(np.min(rx_samples)) > invalid_min_magnitude:
                continue
            pdp_segments.append(PDPSegment(seq_number=seq_number + 1, timestamp=str(timestamp), num_samples=num_samples,
                                           rx_samples=rx_samples, rx_power=compute_rx_power(rx_samples)))

# Match gps_event and pdp_segment timestamps
for gps_event in gps_events:
    seq_number, timestamp = gps_event.seq_number, gps_event.timestamp
    pdp_segment = min(pdp_segments, key=lambda x: abs(datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f') -
                                                      datetime.datetime.strptime(x.timestamp, '%Y-%m-%d %H:%M:%S.%f')))
    if pdp_segment.rx_power == -np.inf:
        continue
    pods.append(Pod(seq_number=seq_number, timestamp=timestamp, gps_event=gps_event, pdp_segment=pdp_segment))

# Visualization: Google Maps rendition of the received signal power levels along the specified route
dataframe = pd.DataFrame(data=[[latitude(x.gps_event), longitude(x.gps_event), rx_power(x.pdp_segment)]
                               for x in pods], columns=['latitude', 'longitude', 'rx-power'])
palette = brewer[color_palette][color_palette_index]
color_mapper = LinearColorMapper(palette=palette, low=dataframe['rx-power'].min(), high=dataframe['rx-power'].max())
color_bar = ColorBar(color_mapper=color_mapper, width=color_bar_width, height=color_bar_height,
                     major_label_text_font_size=color_bar_label_size, label_standoff=color_palette_index,
                     orientation=color_bar_orientation)
google_maps_options = GMapOptions(lat=map_central.latitude.component, lng=map_central.longitude.component,
                                  map_type=map_type, zoom=map_zoom_level)
figure = gmap(google_maps_api_key, google_maps_options, title=map_title, width=map_width, height=map_height)
figure.add_layout(color_bar, color_bar_layout_location)
figure_rx_points = figure.circle('longitude', 'latitude', size=rx_pins_size, alpha=rx_pins_alpha,
                                 color={'field': 'rx-power', 'transform': color_mapper},
                                 source=ColumnDataSource(dataframe))
export_png(figure, filename=''.join([output_dir, rx_pwr_png]), timeout=timeout)

"""
CORE OPERATIONS-IV: Path-loss maps
"""

"""
CORE OPERATIONS-V: Path-loss/Path-gain v distance curves
"""
