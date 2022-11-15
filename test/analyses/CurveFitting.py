"""
This script encapsulates the routines involved in fitting linear, nth-order polynomial, and exponential curves to the
data results obtained from the primary post-processing operations involved in SPAVE-28G/Project-Odin.

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2022. All Rights Reserved.
"""

import numpy as np
import pandas as pd
from typing import Tuple
from scipy.optimize import curve_fit


# Fit a linear curve to data to be plotted on a figure with the X-axis in log-scale
def linear_fit(filename: str, sheetname: str = 'Pathlosses',
               xlengine: str = 'openpyxl', xcolname: str = 'Distance', ycolname: str = 'Pathloss') -> Tuple[float]:
    """
    Examples:

        Fitting lines to the Pathloss (dB) v (log) Tx-Rx Distance (m) on Plotly Chart Studio

        linear_log_fit('urban_campus_pl.xlsx')
        (165.8306715703541, 279.216805311148, array([28.52772592, 50.87546636]))

        linear_log_fit('suburban_fraternities_pl.xlsx')
        (116.2318100119195, 171.9190111279568, array([34.23470145, 13.74628217]))

        linear_log_fit('urban_vegetation_pl.xlsx')
        (109.9143603013646, 156.2907044554164, array([ 64.7075709 , -26.66147833]))

        linear_log_fit('C:/Users/kesha/Downloads/3gpp_tr38901_pl.xlsx')
        (109.9143603013646, 865.2610067590323, array([38.5845435 , 35.20072777]))

        linear_log_fit('C:/Users/kesha/Downloads/itu_m2135_pl.xlsx')
        (109.9143603013646, 865.2610067590323, array([38.59140152, 33.87781245]))
    """
    df = pd.read_excel(filename, sheet_name=sheetname, engine=xlengine)

    x, y = df[xcolname], df[ycolname]
    return np.min(x), np.max(y), np.polyfit(np.log10(x), y, deg=1)


def exponential_fit(filename: str, sheetname: str = 'Spatial_Correlation_Coefficient', xlengine: str = 'openpyxl',
                    xcolname: str = 'Distance', ycolname: str = 'Spatial Autocorrelation Coefficient') -> Tuple[float]:
    """
    Examples:

        Fitting exponentials to the Spatial Autocorrelation Coefficient v Tx-Rx Distance (m) on Plotly Chart Studio

        exponential_fit('urban_campus_scd.xlsx')
        0.0, 99.9, array([0.13758322, 1.17457316, 0.26954946])...

        exponential_fit('urban_vegetation_scd.xlsx')
        0.0, 99.7, array([0.49688067, 0.31398854, 0.08651382])...

        exponential_fit('suburban_fraternities_scd.xlsx')
        0.0, 99.9, array([0.43060731, 0.69371094, 3.12342714])...

        Fitting exponentials to the Spatial Autocorrelation Coefficient v Tx-Rx Alignment (deg) on Plotly Chart Studio

        exponential_fit('suburban_fraternities_sca.xlsx', xcolname='Alignment')
        0.0, 2.204999999999975, array([ 0.2943438,  0.5797391, 81.3181963])...

        exponential_fit('urban_campus_sca.xlsx', xcolname='Alignment')
        0.0, 2.204999999999975, array([ 0.14966232,  0.7146556 , 72.89749247])...
    """

    def func(x_: np.array, a: float, b: float, c: float) -> np.array:
        return a + b * np.exp(-c * x_)

    df = pd.read_excel(filename, sheet_name=sheetname, engine=xlengine)

    x, y = df[xcolname], df[ycolname]
    return np.min(x), np.max(x), curve_fit(func, x, y)
