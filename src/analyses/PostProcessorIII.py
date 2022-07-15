"""
Yggdrasil: A consolidated post-processor for Project Odin

This script reads a 'parsed_metadata.log' file -- obtained by running the gr_read_file_metadata Linux utility on the
'samples_with_metadata.log' file of a specific route traversed by us during our POWDER measurement campaign -- in order
to evaluate the various metadata fields incorporated within it.

Next, this script matches the GPS events recorded for the specified route with the evaluated metadata in order to
construct a consolidated dataclass {seq_number, timestamp, ODIN_GPS_EVENT, ODIN_PDP_SEGMENT}
that is used for visualizations on either the Dash-Mapbox API combination OR the Bokeh-GoogleMaps API combination.

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Additional Configuration Inputs: Yaguang Zhang <zhan1472@purdue.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ.
Copyright (c) 2021. All Rights Reserved.
"""

# Project Odin Consolidated Post-Processing Engine (Yggdrasil)

# The imports
import os
import re
import json
import datetime
import dataclasses
import numpy as np
import pandas as pd
from enum import Enum
from typing import Dict
from bokeh.io import export_png
from bokeh.plotting import gmap
from bokeh.palettes import brewer
from dataclasses import dataclass
from scipy import signal, integrate
import sk_dsp_comm.fir_design_helper as fir_d
from bokeh.models import GMapOptions, ColumnDataSource, ColorBar, LinearColorMapper

"""
Data Object Setup | Enumerations & Dataclasses | Other Declarations

NOTE: A few enumerations and dataclasses are repeated from Raven in order to render this entity as a standalone utility.
"""

ODIN_DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


class Units(Enum):
    """
    An enumeration listing all possible units for GPSEvent members
    """
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
    """
    An enumeration listing all possible fix-types supported by the GPS receiver
    """
    NO_FIX = 0
    DEAD_RECKONING = 1
    TWO_DIMENSIONAL = 2
    THREE_DIMENSIONAL = 3
    GNSS = 4
    TIME_FIX = 5


class CarrierSolutionType(Enum):
    """
    An enumeration listing all possible carrier solution-types supported by the GPS receiver
    """
    NO_SOLUTION = 0
    FLOAT_SOLUTION = 1
    FIXED_SOLUTION = 2


@dataclass(order=True)
class Member:
    """
    Secondary information tier in GPSEvent

    A "core" member which encapsulates highly specific details about latitude, longitude, altitude, speed, heading, and
    other components in the primary information tier of GPSEvent
    """
    is_high_precision: bool = False
    main_component: float = 0.0
    high_precision_component: float = 0.0
    component: float = 0.0
    precision: float = 0.0
    units: Units = Units.DIMENSIONLESS


@dataclass(order=True)
class GPSEvent:
    """
    Primary information tier in GPSEvent (ODIN_GPS_EVENT)

    The GPSEvent class: ODIN_GPS_EVENT communicated over the ODIN_GPS_EVENTS Kafka topic encapsulates this data object

    DESIGN NOTES (Odin v21.09) | Backward-compatibility necessities:
    1. The timestamp member is maintained as a string -- instead, of a datetime object;
    2. The core_length & total_length fields have been maintained for backward-compatibility, although they are not
       used here due to the fact that low-memory uC needs are rendered irrelevant;
    3. In order to interface this entity with Raven/Forge or future modules, make appropriate changes to this dataclass'
       contract - timestamp type changes, [core_length | total_length] changes, etc.
    """
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    is_gnss_fix_ok: bool = False
    siv: int = 0
    fix_type: FixType = FixType.NO_FIX
    carrier_solution_type: CarrierSolutionType = CarrierSolutionType.NO_SOLUTION
    latitude: Member = Member()  # A secondary visualization member
    longitude: Member = Member()  # A secondary visualization member
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
    """
    ODIN_PDP_SEGMENT

    The PDPSegment class: ODIN_PDP_SEGMENT is an internalized data segment constituting a set of power-delay profiles
    collected at the communication subsystem of Project Odin's RxRealm -- that constitutes the core informational
    element in our Sliding Correlator Channel Sounder design
    """
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
    rx_samples: np.array = np.array([], dtype=np.csingle)  # The primary evaluation collection
    rx_power: float = 0.0  # The primary visualization member


@dataclass(order=True)
class Pod:
    """
    ODIN_GPS_EVENT and ODIN_PDP_SEGMENT as two peas in a "pod" (ODIN_POD)

    A class encapsulating the core data elements of Project Odin's consolidated post-processor -- after completing
    internal manipulations to match an ODIN_GPS_EVENT with an ODIN_PDP_SEGMENT
    """
    seq_number: int = 0
    timestamp: str = str(datetime.datetime.utcnow())
    gps_event: GPSEvent = GPSEvent()
    pdp_segment: PDPSegment = PDPSegment()


gps_events, pdp_segments, pods = [], [], []

"""
TODO: Configurations-I | GPS logs
"""
gps_data_dir = 'C:/Users/kesha/Workspaces/Odin/deployment/measurement-campaign/routes/gps-data/urban-campus-II/'
# gps_data_dir = 'C:/Users/kesha/Workspaces/Odin/deployment/measurement-campaign/routes/gps-data/urban-vegetation/'
# gps_data_dir = 'C:/Users/kesha/Workspaces/Odin/deployment/measurement-campaign/routes/gps-data/suburban-fraternities/'

"""
TODO: Configurations-II | Power delay profiles and associated metadata

MANUAL_SETUP: Obtain the 'parsed_metadata.log' file corresponding to a certain traversed route by running the 
              gr_read_file_metadata Linux utility on the 'samples_with_metadata.log' file.
"""
pdp_samples_file_name = 'samples.log'
start_timestamp_file_name = 'timestamp.log'
parsed_metadata_file_name = 'parsed_metadata.log'
comm_data_dir = 'D:/playground/backups/urban-campus-II/'
# comm_data_dir = 'D:/playground/backups/urban-vegetation/'
# comm_data_dir = 'D:/playground/backups/suburban-fraternities/'

"""
TODO: Configurations-III | Rx gain | Sample rate | Pre-filtering options | Temporal truncation & Time-windowing options
                         | Noise elimination & Sample thresholding options

NOTE: These configurations are exposed as a Python dictionary to match Yaguang Zhang's MATLAB-based post-processors.
      For more details on what these configurations entail, please refer to project Odin's GitHub submodule named 
      "UtahMeasurementCampaignCode": here, 'TallEnoughAbs' is used.
"""
rx_gain = 76  # Rx USRP gain in decibels
sample_rate = 2e6  # The sample rate set in UHD_USRP_Source (and its associated GNURadio blocks) at the RxRealm
invalid_min_magnitude = 1e5  # Invalid magnitude of the complex I/Q samples due to some discrepancy during recording

time_windowing_config = {'multiplier': 0.5, 'truncation_length': 200000}
noise_elimination_config = {'multiplier': 3.5, 'min_peak_index': 2000, 'num_samples_discard': 0,
                            'max_num_samples': 500000, 'relative_range': [0.875, 0.975], 'threshold_ratio': 0.9}
prefilter_config = {'passband_freq': 60e3, 'stopband_freq': 65e3, 'passband_ripple': 0.01, 'stopband_attenuation': 80}

"""
TODO: Configurations-IV | Map Visualization Options
"""
google_maps_api_key = 'AIzaSyDzb5CB4L9l42MyvSmzvaSZ3bnRINIjpUk'
map_type = 'hybrid'  # Allowed types = 'satellite', 'terrain', 'hybrid', 'roadmap'
png_file_export_timeout = 300  # In seconds [Selenium Requirements: <FireFox, GeckoDriver> | <Chromium, ChromeDriver>]

map_width, map_height, map_zoom_level, map_title = 5500, 2800, 20, 'Urban Campus-II [Van]'
map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7651), longitude=Member(component=-111.8500))

# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'Urban Vegetation [Cart]'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7655), longitude=Member(component=-111.8479))

# map_width, map_height, map_zoom_level, map_title = 3500, 3500, 21, 'Suburban Fraternities [Cart]'
# map_central = GPSEvent(seq_number=-1, latitude=Member(component=40.7670), longitude=Member(component=-111.8480))

tx_pin_size, tx_pin_alpha, tx_pin_color = 80, 1.0, 'red'
rx_pins_size, rx_pins_alpha = 50, 1.0
tx_location = GPSEvent(seq_number=-1, latitude=Member(component=40.766173670),
                       longitude=Member(component=-111.847939330))

color_bar_layout_location = 'right'
color_palette, color_palette_index = 'RdYlGn', 11
color_bar_width, color_bar_height, color_bar_label_size, color_bar_orientation = 125, 2700, '125px', 'vertical'

"""
TODO: Configurations-V | Final Visualized PNG Image File Options
"""
png_file_name = 'urban-campus-II-rx-power.png'
# png_file_name = 'urban-vegetation-rx-power.png'
# png_file_name = 'suburban-fraternities-rx-power.png'
output_dir = 'C:/Users/kesha/Workspaces/Odin/src/rotator/e2e/test/ArkAngel-VI/'

"""
Utility Routines | Extract individual data members from within ODIN_PODs
"""


def pack_dict_into_dataclass(dict_: Dict, dataclass_: dataclass) -> dataclass:
    """
    Pack the data from a Python dictionary into a dataclass instance

    Args:
        dict_: The dictionary to be packed into a dataclass instance
        dataclass_: The dataclass whose instance is to be returned post-dictionary-packing

    Returns: An instance of the provided dataclass reference packed with fields and values from the provided dictionary
    """
    loaded_dict = {}
    fields = {f.name: f.type for f in dataclasses.fields(dataclass_)}
    for k, v in dict_.items():
        loaded_dict[k] = (lambda: v, lambda: pack_dict_into_dataclass(v, Member))[fields[k] == Member]()
    return dataclass_(**loaded_dict)  # Pack/Un-pack


def latitude(y: GPSEvent) -> float:
    """
    Extract the latitude component (floating-point number) from within the given GPSEvent

    Args:
        y: The GPSEvent dataclass instance from which the latitude component is to be extracted and returned

    Returns: The latitude component (floating-point number) from within the given GPSEvent
    """
    return y.latitude.component


def longitude(y: GPSEvent) -> float:
    """
    Extract the longitude component (floating-point number) from within the given GPSEvent

    Args:
        y: The GPSEvent dataclass instance from which the longitude component is to be extracted and returned

    Returns: The longitude component (floating-point number) from within the given GPSEvent
    """
    return y.longitude.component


def rx_power(y: PDPSegment) -> float:
    """
    Extract the received signal power (floating-point number) from within the given PDPSegment

    Args:
        y: The PDPSegment dataclass instance from which the received signal power is to be extracted and returned

    Returns: The received signal power (floating-point number) from within the given PDPSegment
    """
    return y.rx_power


def rx_power__(x: np.array) -> float:
    """
    Compute the signal power measured at the RxRealm's communication subsystem using the provided complex I/Q samples

    Args:
        x: An array of complex I/Q samples extracted from the 'samples.log' file in accordance with the metadata
           extracted from the 'parsed_metadata.log' file

    Returns: The computed signal power measured at the RxRealm's communication subsystem
    """
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

    # PSD Evaluation: Received signal power (at the RxRealm's communication subsystem) computation
    pwr_values = np.square(np.abs(np.fft.fft(samps))) / n_samples
    freq_values = np.fft.fftfreq(n_samples, (1 / fs))
    indices = np.argsort(freq_values)

    # Trapezoidal numerical integration to compute signal power at the Rx from the organized PSD data | Error Handling
    computed_rx_power = integrate.trapz(y=pwr_values[indices], x=freq_values[indices])
    if computed_rx_power != 0.0:
        return 10 * np.log10(computed_rx_power) - rx_gain
    return -np.inf  # The samples corresponding to this segment were either recorded incorrectly or parsed incorrectly


# ODIN_GPS_EVENT construction: Extract gps_event
for filename in os.listdir(gps_data_dir):
    with open(''.join([gps_data_dir, filename])) as file:
        gps_events.append(pack_dict_into_dataclass(json.load(file), GPSEvent))

# ODIN_PDP_SEGMENT construction: Extract timestamp_0 (start_timestamp)
with open(''.join([comm_data_dir, start_timestamp_file_name])) as file:
    elements = file.readline().split()  # FileX-based Contractual Constraint-1
    timestamp_0 = datetime.datetime.strptime(''.join([elements[2], ' ', elements[3]]),
                                             ODIN_DATE_TIME_FORMAT)  # FileX-based Contractual Constraint-2

# ODIN_PDP_SEGMENT construction: Evaluate parsed_metadata | Extract power-delay profile samples
segment_done = False
timestamp_ref = datetime.datetime.strptime(gps_events[0].timestamp, ODIN_DATE_TIME_FORMAT)
pdp_samples_file = ''.join([comm_data_dir, pdp_samples_file_name])
with open(''.join([comm_data_dir, parsed_metadata_file_name])) as file:
    for line_num, line in enumerate(file):
        if line_num % 18 == 0:  # FileY-based Contractual Constraint-1
            seq_number = int(re.search(r'\d+', line)[0])
        elif (line_num - 3) % 18 == 0:  # FileY-based Contractual Constraint-2
            # noinspection RegExpAnonymousGroup
            timestamp = timestamp_0 + datetime.timedelta(seconds=float(re.search(r'[+-]?\d+(\.\d+)?', line)[0]))
        elif (line_num - 11) % 18 == 0 and timestamp >= timestamp_ref:  # FileY-based Contractual Constraint-3
            num_samples = int(re.search(r'\d+', line)[0])
            segment_done = True
        else:
            """
            Nothing to do here...
            """

        if segment_done:  # FileY-based Contractual Constraint-4
            segment_done = False
            rx_samples = np.fromfile(pdp_samples_file, offset=seq_number * num_samples, count=num_samples,
                                     dtype=np.csingle)
            # Invalidate segments with incorrectly-read/dropped samples | Invalidate incorrectly-recorded samples
            if np.isnan(rx_samples).any() or np.abs(np.min(rx_samples)) > invalid_min_magnitude:
                continue
            pdp_segments.append(PDPSegment(seq_number=seq_number + 1, timestamp=str(timestamp), num_samples=num_samples,
                                           rx_samples=rx_samples, rx_power=rx_power__(rx_samples)))

# ODIN_POD construction: Match ODIN_GPS_EVENT and ODIN_PDP_SEGMENT timestamps
for gps_event in gps_events:
    seq_number, timestamp = gps_event.seq_number, gps_event.timestamp
    pdp_segment = min(pdp_segments, key=lambda x: abs(datetime.datetime.strptime(timestamp, ODIN_DATE_TIME_FORMAT) -
                                                      datetime.datetime.strptime(x.timestamp, ODIN_DATE_TIME_FORMAT)))
    # Do not associate a gps_event with its pdp_segment if the rx-power is invalid
    if pdp_segment.rx_power == -np.inf:
        continue
    pods.append(Pod(seq_number=seq_number, timestamp=timestamp, gps_event=gps_event, pdp_segment=pdp_segment))

# Organization for Visualization: Moving the primary & secondary visualization members into a pd dataframe for Bokeh
dataframe = pd.DataFrame(data=[[latitude(x.gps_event), longitude(x.gps_event), rx_power(x.pdp_segment)]
                               for x in pods], columns=['latitude', 'longitude', 'rx-power'])

# Visualization: Google Maps rendition of the received signal power levels along the specified route
palette = brewer[color_palette][color_palette_index]
color_mapper = LinearColorMapper(palette=palette, low=dataframe['rx-power'].min(), high=dataframe['rx-power'].max())
color_bar = ColorBar(color_mapper=color_mapper, width=color_bar_width, height=color_bar_height,
                     major_label_text_font_size=color_bar_label_size, label_standoff=color_palette_index,
                     orientation=color_bar_orientation)

google_maps_options = GMapOptions(lat=map_central.latitude.component, lng=map_central.longitude.component,
                                  map_type=map_type, zoom=map_zoom_level)
figure = gmap(google_maps_api_key, google_maps_options, title=map_title, width=map_width, height=map_height)
figure.add_layout(color_bar, color_bar_layout_location)

# figure_tx_point = figure.diamond([tx_location.longitude.component], [tx_location.latitude.component],
#                                  size=tx_pin_size, alpha=tx_pin_alpha, color=tx_pin_color)

figure_rx_points = figure.circle('longitude', 'latitude', size=rx_pins_size, alpha=rx_pins_alpha,
                                 color={'field': 'rx-power', 'transform': color_mapper},
                                 source=ColumnDataSource(dataframe))

export_png(figure, filename=''.join([output_dir, png_file_name]), timeout=png_file_export_timeout)
# The End
