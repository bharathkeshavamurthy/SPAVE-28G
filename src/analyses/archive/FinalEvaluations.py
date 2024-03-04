"""
Final evaluations from MPC analyses logs
"""

import plotly
import numpy as np
# from scipy import signal
from itertools import pairwise
import plotly.graph_objs as go
from scipy.optimize import curve_fit

"""
CONFIGURATIONS
"""
# sg_wsize, sg_poly_order = 15, 3
plotly.tools.set_credentials_file(username='total.academe', api_key='0H3Dx43A6wrbi7tY8ucl')

"""
UTILITIES
"""

decibel_1, linear_1 = lambda x: 10 * np.log10(x), lambda x: 10 ** (x / 10.0)
deg2rad, rad2deg = lambda x: x * (np.pi / 180.0), lambda x: x * (180.0 / np.pi)


def ecdf(x):
    x_, cts = np.unique(x, return_counts=True)

    cum_sum = np.cumsum(cts)
    return x_, cum_sum / cum_sum[-1]


def stochastic_fit(x, y):
    def func(_x, _ld):
        return 1 - np.exp(-_ld * _x)

    return curve_fit(func, x, y)


def rms_delay_spread(alphas, taus):
    num1, num2, den = [], [], []

    for idx in range(len(alphas)):
        tau, p_tau = taus[idx], alphas[idx]
        num1.append(np.square(tau) * p_tau)

        num2.append(tau * p_tau)
        den.append(p_tau)

    num1_sum, num2_sum, den_sum = np.sum(num1), np.sum(num2), np.sum(den)

    return np.sqrt((num1_sum / den_sum) - np.square(num2_sum / den_sum))


def rms_aoa_direction_spread(alphas, phis, thetas):
    e_vecs, p_vec, mu_vec = [], [], []

    for idx in range(len(alphas)):
        phi, theta = phis[idx], thetas[idx]

        e_vecs.append(np.array([np.cos(phi) * np.sin(theta), np.sin(phi) * np.sin(theta), np.cos(theta)]))

        mu_vec.append(alphas[idx] * e_vecs[idx])
        p_vec.append(alphas[idx])

    mu_omega = np.sum(mu_vec, axis=0)

    return np.sqrt(np.sum([np.square(np.linalg.norm(e_vecs[_idx] -
                                                    mu_omega)) * p_vec[_idx] for _idx in range(len(alphas))]))


"""
DATA
"""

'''
URBAN
'''

environment = 'Urban'

delays = {0: [0, 12.5, 12.5, 12.5, 25, 25, 87.5, 100, 100],
          1: [25, 37.5, 100, 100, 100, 100, 100, 100, 100],
          2: [12.5, 62.5, 100, 100, 100, 100, 100, 100, 100],
          3: [37.5, 62.5, 75, 75, 75, 75, 75, 75, 100],
          4: [62.5, 62.5, 62.5, 62.5, 75, 75, 100, 100, 100],
          5: [50, 75, 75, 75, 75, 75, 75, 100, 100],
          6: [0, 12.5, 12.5, 25, 25, 62.5, 75, 75, 75],
          7: [12.5, 12.5, 12.5, 12.5, 12.5, 50, 50, 50, 75],
          8: [12.5, 12.5, 12.5, 12.5, 12.5, 12.5, 12.5, 12.5, 12.5],
          9: [0, 37.5, 37.5, 37.5, 37.5, 37.5, 37.5, 37.5, 37.5],
          10: [50, 62.5, 87.5, 87.5, 87.5, 100, 100, 100, 100],
          11: [37.5, 87.5, 87.5, 100, 100, 100, 100, 100, 100],
          12: [62.5, 75, 75, 75, 75, 75, 75, 75, 87.5],
          13: [0, 0, 0, 0, 12.5, 62.5, 62.5, 62.5, 75],
          14: [37.5, 37.5, 75, 75, 75, 75, 75, 75, 87.5]}  # in ns

amplts = {0: [-47.09, -120, -134, -134, -134, -134, -134, -134, -124.84],
          1: [-116.38, -47.57, -47.57, -47.57, -125, -125, -115.54, -123.41, -123.41],
          2: [-122.48, -113.46, -113.66, -122.48, -48.70, -119.13, -126.48, -126.48, -126.48],
          3: [-51.36, -117.52, -134.04, -134.04, -134.04, -134.04, -134.04, -134.04, -134.04],
          4: [-49.91, -120.13, -134.04, -134.04, -134.04, -134.04, -134.04, -134.04, -134.04],
          5: [-33.56, -108.21, -108.73, -108.73, -108.73, -108.73, -108.73, -105.36, -105.36],
          6: [-41.61, -117.77, -115.08, -116.16, -113.57, -108.33, -117.84, -117.84, -117.84],
          7: [-112.58, -112.58, -112.58, -112.58, -112.58, -111.93, -111.93, -111.93, -37.18],
          8: [-32.13, -103, -112.04, -112.04, -112.04, -112.04, -112.04, -110.95, -110.95],
          9: [-34.68, -112.86, -112.86, -112.86, -112.86, -112.86, -112.86, -112.86, -112.86],
          10: [-132.28, -114.07, -43.19, -114.06, -43.19, -136.3, -136.3, -136.3, -136.3],
          11: [-115.02, -114.95, -114.95, -47.03, -126.1, -126.1, -126.1, -126.1, -126.1],
          12: [-116.82, -48.86, -121.6, -48.86, -48.86, -121.6, -121.6, -120.98, -44.18],
          13: [-112.44, -112.44, -112.44, -112.44, -105.94, -105, -105, -105, -36.43],
          14: [-45.91, -118.72, -127.84, -127.84, -127.84, -127.84, -127.84, -127.84, -117.58]}  # in dB

aoa_azs = {0: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           1: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           2: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           3: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           4: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           5: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           6: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           7: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           8: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           9: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           10: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           11: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           12: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           13: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           14: [-180, -180, -180, -180, -180, -180, -180, -180, -180]}  # in deg

aoa_els = {0: [-180, -135, -135, -33.75, -135, -33.75, -135, -33.75, -45],
           1: [-56.25, 0, -112.5, -22.5, -157.5, -56.25, -11.25, -11.25, 0],
           2: [0, -180, -101.25, -78.75, -33.75, -22.5, -157.5, -56.25, -33.75],
           3: [-56.25, -168.75, -22.5, -101.25, -168.75, -45, -22.5, -101.25, -157.5],
           4: [-56.25, -11.25, -146.25, -146.25, -90, -168.75, -168.75, -56.25, -56.25],
           5: [-67.5, -67.5, -135, -45, -135, -45, -67.5, -168.75, -33.75],
           6: [-180, -180, -157.5, -157.5, -157.5, -157.5, -157.5, -157.5, -157.5],
           7: [-157.5, 0, -157.5, -90, 0, -157.5, -90, 0, -157.5],
           8: [-123.75, -56.25, -56.25, -56.25, -146.25, -146.25, -112.5, -157.5, -157.5],
           9: [-11.25, -135, -123.75, 0, -22.5, -123.75, 0, -56.25, -90],
           10: [-112.5, -180, -112.5, 0, -67.5, 0, -67.5, 0, -67.5],
           11: [-146.25, -168.75, -56.25, -146.25, -11.25, -112.5, -112.5, -67.5, -45],
           12: [-45, -101.25, -101.25, -168.75, 0, -157.5, -123.75, -11.25, -123.75],
           13: [-45, -112.5, -11.25, -78.75, -146.25, -112.5, -22.5, -22.5, -123.75],
           14: [-135, -56.25, -123.75, -123.75, -123.75, -123.75, -33.75, -135, -135]}  # in deg

'''
SUBURBAN
'''

'''
environment = 'SUBURBAN'

delays = {0: [0, 0, 25, 25, 25, 25, 25, 25, 50],
          1: [0, 25, 25, 25, 25, 75, 75, 100, 100],
          2: [0, 0, 0, 0, 0, 37.5, 37.5, 37.5, 87.5],
          3: [12.5, 37.5, 75, 75, 75, 75, 75, 100, 100],
          4: [12.5, 12.5, 25, 25, 25, 25, 25, 25, 75]}  # in ns

amplts = {0: [-36.44, -109.82, -113.3, -113.3, -110.24, -110.24, -110.24, -113.3, -107.01],
          1: [-35.87, -109.16, -113.82, -113.82, -113.82, -110, -109.51, -111.76, -111.76],
          2: [-39.15, -112.24, -112.24, -112.24, -112.24, -108.13, -108.13, -108.13, -107.32],
          3: [-110.54, -109.54, -37.05, -109.51, -37.05, -37.05, -109.51, -107.2, -107.2],
          4: [-37.17, -109.71, -116.22, -116.22, -116.22, -116.22, -116.22, -116.22, -108.43]}  # in dB

aoa_azs = {0: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           1: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           2: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           3: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           4: [-180, -180, -180, -180, -180, -180, -180, -180, -180]}  # in deg

aoa_els = {0: [-90, -135, -90, -78.75, -168.75, -0, -78.75, -180, 0],
           1: [-123.75, -101.25, -56.25, -168.75, -157.5, -56.25, -22.5, -45, -101.25],
           2: [-112.5, -123.75, -56.25, -78.75, -67.5, -180, -22.5, -101.25, -180],
           3: [-112.5, -67.5, -11.25, -90, -112.5, -112.5, -11.25, -11.25, -11.25],
           4: [-168.75, -168.75, 0, -168.75, -45, -101.25, -135, -101.25, -101.25]}  # in deg
'''

'''
FOLIAGE
'''

'''
environment = 'Foliage'

delays = {0: [12.5, 12.5, 12.5, 12.5, 12.5, 12.5, 50, 62.5, 62.5],
          1: [25, 25, 25, 37.5, 37.5, 37.5, 37.5, 37.5, 100],
          2: [37.5, 62.5, 62.5, 87.5, 87.5, 87.5, 87.5, 87.5, 87.5],
          3: [62.5, 62.5, 62.5, 62.5, 75, 75, 75, 100, 100],
          4: [0, 0, 0, 0, 0, 0, 0, 0, 0]}  # in ns

amplts = {0: [-125.1, -125.1, -125.1, -125.1, -125.1, -125.1, -119.49, -43.1, -117.64],
          1: [-121.25, -121.25, -121.25, -118.72, -118.72, -118.72, -118.72, -118.72, -47.77],
          2: [-45.86, -119, -11, -121, -121, -121, -121, -121, -116.86],
          3: [-45.08, -125.38, -125.38, -125.38, -129.79, -129.79, -129.79, -122.36, -122.44],
          4: [0, 0, 0, 0, 0, 0, 0, 0, 0]}  # in dB

aoa_azs = {0: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           1: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           2: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           3: [-180, -180, -180, -180, -180, -180, -180, -180, -180],
           4: [-180, -180, -180, -180, -180, -180, -180, -180, -180]}  # in deg

aoa_els = {0: [-78.75, -67.5, -78.75, -112.5, -33.75, -112.5, -33.75, -168.75, -123.75],
           1: [-135, 0, -101.25, -33.75, -112.5, -146.25, -135, -180, -135],
           2: [-157.5, -157.5, -67.5, -180, -123.75, -123.75, -135, -56.25, -135],
           3: [-90, -180, -180, -45, -180, -101.25, -56.25, -146.25, -56.25],
           4: [-168.75, -33.75, -45, -180, -22.5, -33.75, -112.5, -11.25, -22.5]}  # in deg
'''

""" 
VISUALIZATIONS
"""

'''
RMS Delay Spreads
'''

rms_delay_spreads = np.array([rms_delay_spread(
    [linear_1(_x) for _x in amplts[_k]], [_x * 1e-9 for _x in delays[_k]]) / 1e-9 for _k in delays.keys()])

rms_delay_spread_x, rms_delay_spread_ecdf = ecdf(rms_delay_spreads)
stochastic_rms_delay_spread_param = stochastic_fit(rms_delay_spread_x, rms_delay_spread_ecdf)

rms_ds_layout = dict(yaxis=dict(title='CDF Probability'),
                     xaxis=dict(title='RMS Delay Spread in ns', type='log'),
                     title=f'{environment}: RMS Delay Spreads Cumulative Distribution Function')

stochastic_rms_ds_trace = go.Scatter(
    x=rms_delay_spread_x, mode='lines+markers', name='Stochastic',
    y=[1 - np.exp(-1 * stochastic_rms_delay_spread_param * _x) for _x in rms_delay_spread_x])
rms_ds_trace = go.Scatter(x=rms_delay_spread_x, y=rms_delay_spread_ecdf, mode='lines+markers', name='Meas')

rms_ds_url = plotly.plotly.plot(dict(data=[rms_ds_trace, stochastic_rms_ds_trace], layout=rms_ds_layout))

print(f'SPAVE-28G | Final Evaluations | {environment} | RMS Delay Spread CDF: {rms_ds_url}!')

'''
RMS AoA Direction Spreads
'''

rms_aoa_dir_spreads = np.array([rms_aoa_direction_spread([linear_1(_x) for _x in amplts[_k]], [
    deg2rad(_x) for _x in aoa_azs[_k]], [deg2rad(_x) for _x in aoa_els[_k]]) for _k in amplts.keys()])

rms_aoa_dir_spread_x, rms_aoa_dir_spread_ecdf = ecdf(rms_aoa_dir_spreads)
stochastic_rms_aoa_dir_spread_param = stochastic_fit(rms_aoa_dir_spread_x, rms_aoa_dir_spread_ecdf)

rms_aoa_dirs_layout = dict(yaxis=dict(title='CDF Probability'),
                           xaxis=dict(title='RMS AoA Direction Spread'),
                           title=f'{environment}: RMS AoA Direction Spreads Cumulative Distribution Function')

stochastic_rms_aoa_dirs_trace = go.Scatter(
    x=rms_aoa_dir_spread_x, mode='lines+markers', name='Stochastic',
    y=[1 - np.exp(-1 * stochastic_rms_aoa_dir_spread_param * _x) for _x in rms_aoa_dir_spread_x])
rms_aoa_dirs_trace = go.Scatter(x=rms_aoa_dir_spread_x, y=rms_aoa_dir_spread_ecdf, mode='lines+markers', name='Meas')

rms_aoa_dirs_url = plotly.plotly.plot(dict(data=[rms_aoa_dirs_trace,
                                                 stochastic_rms_aoa_dirs_trace], layout=rms_aoa_dirs_layout))

print(f'SPAVE-28G | Final Evaluations | {environment} | RMS AoA Direction Spread CDF: {rms_aoa_dirs_url}!')

'''
Inter-Arrival Times
'''

all_delays = [_d for _k in delays.keys() for _d in delays[_k]]
inter_arr_times = np.array([_y - _x for _x, _y in pairwise(sorted(all_delays))])

inter_arr_times_x, inter_arr_times_ecdf = ecdf(inter_arr_times)
stochastic_inter_arr_times_param = stochastic_fit(inter_arr_times_x, inter_arr_times_ecdf)

inter_arr_times_layout = dict(yaxis=dict(title='CDF Probability'),
                              xaxis=dict(title='Inter-Arrival Times in ns', type='log'),
                              title=f'{environment}: Inter-Arrival Times Cumulative Distribution Function')

stochastic_inter_arr_times_trace = go.Scatter(
    x=inter_arr_times_x, mode='lines+markers', name='Stochastic',
    y=[1 - np.exp(-1 * stochastic_inter_arr_times_param * _x_val) for _x_val in inter_arr_times_x])
inter_arr_times_trace = go.Scatter(x=inter_arr_times_x, y=inter_arr_times_ecdf, mode='lines+markers', name='Meas')

inter_arr_times_url = plotly.plotly.plot(dict(layout=inter_arr_times_layout,
                                              data=[inter_arr_times_trace, stochastic_inter_arr_times_trace]))

print(f'SPAVE-28G | Final Evaluations | {environment} | Inter-Arrival Times CDF: {inter_arr_times_url}!')

'''
Decay Characteristics
'''

all_delays = sorted([_d for _k in delays.keys() for _d in delays[_k]], reverse=True)
all_amplts = sorted([_a for _k in amplts.keys() for _a in amplts[_k]], reverse=True)

decay_chars_layout = dict(yaxis=dict(title='Peak Component Power in dB'),
                          xaxis=dict(title='Component Delay Value in ns', type='log'),
                          title=f'{environment}: MPC Delay Characteristics | Peak Component Power vs Delay')

decay_chars_trace = go.Scatter(x=all_delays, y=all_amplts, mode='markers')
decay_chars_url = plotly.plotly.plot(dict(data=[decay_chars_trace], layout=decay_chars_layout))

print(f'SPAVE-28G | Final Evaluations | {environment} | Decay Characteristics Plot: {decay_chars_url}!')
