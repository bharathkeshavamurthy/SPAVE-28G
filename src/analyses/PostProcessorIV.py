"""
This script encapsulates the operations involved in visualizing the antenna patterns (elevation & azimuth) of our 28GHz
communication system used in our mmWave propagation modeling activities on the POWDER testbed in Salt Lake City -- and,
subsequently generate the path-loss and SNR maps of the routes traversed during this measurement campaign.

Author: Bharath Keshavamurthy <bkeshav1@asu.edu | bkeshava@purdue.edu>
Organization: School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ
              School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
Copyright (c) 2022. All Rights Reserved.
"""

# The imports
import numpy
import plotly
import scipy.io
from collections import namedtuple
from scipy.interpolate import interp1d

# Part-I: Operations for processing and visualizing the antenna patterns

"""
CONFIGURATIONS
"""
max_gain, angle_res_ext = 22.0, 5.0  # Maximum antenna gain for our setup, in decibels | Extend angle resolution
plotly.tools.set_credentials_file(username='bkeshav1', api_key='CLTFaBmP0KN7xw1fUheu')  # Plotly User Credentials
log_file = 'C:/Users/kesha/Workspaces/Odin/src/rotator/e2e/logs/antenna-logs/antenna_pattern.mat'  # Pattern .mat log

"""
UTILITIES
"""


def hpbw(_angles, _amps, _max_gain):
    interp = interp1d(_angles, _amps)
    angles_ = numpy.arange(start=0.0, stop=360.0,
                           step=360.0 / (angle_res_ext * len(_angles)))

    amps_ = interp(angles_)
    amps_th_idx = numpy.where(amps_ <= _max_gain - 3.0)[0]

    low, high = amps_th_idx[0], amps_th_idx[-1]
    angles_hpbw_, powers_hpbw_ = (angles_[low], angles_[high]), (amps_[low], amps_[high])

    return angles_hpbw_, powers_hpbw_, angles_hpbw_[0] + 360.0 + angles_hpbw_[1]


decibel_1, decibel_2 = lambda x: 10 * numpy.log10(x), lambda x: 20 * numpy.log10(x)
pattern = namedtuple('pattern', ['angles', 'amplitudes', 'amplitudes_db', 'angles_hpbw', 'powers_hpbw', 'hpbw'])

"""
CORE OPERATIONS
"""

log = scipy.io.loadmat(log_file)
az_log, el_log = log['pat28GAzNorm'], log['pat28GElNorm']

az_angles, az_amps, az_amps_db = az_log.azs, az_log.amps, decibel_2(az_log.amps)
az_angles_hpbw, az_powers_hpbw, az_hpbw = hpbw(az_angles, az_amps_db, max_gain)
az_pattern = pattern(az_angles, az_amps, az_amps_db, az_angles_hpbw, az_powers_hpbw, az_hpbw)

el_angles, el_amps, el_amps_db = el_log.els, el_log.amps, decibel_2(el_log.amps)
el_angles_hpbw, el_powers_hpbw, el_hpbw = hpbw(el_angles, el_amps_db, max_gain)
el_pattern = pattern(el_angles, el_amps, el_amps_db, el_angles_hpbw, el_powers_hpbw, el_hpbw)
