"""
ITU-R M.2135 Pathloss v Distance Playground [Testing Script]

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

import numpy as np
import pandas as pd
from scipy import constants


def pathloss_uma_itur_m2135():
    pls, pl_los, pl_nlos = [], 0.0, 0.0
    pi, c = np.pi, constants.speed_of_light
    h, w, h_ue, h_bs, f_c = 21.3, 15.4, 2.0, 25.276639648, 28e9

    for d_3d in range(30, 1001, 1):

        d_bp, d_2d = 2 * pi * h_ue * h_bs * (f_c / c), np.sqrt(np.square(d_3d) - np.square(h_bs - h_ue))

        if 10.0 < d_2d < d_bp:
            pl_los = (np.log10(d_3d) * min(0.03 * (h ** 1.72), 10.0)) + \
                     (20.0 * np.log10(40.0 * pi * d_3d * (f_c / 3e9))) + \
                     (0.002 * np.log10(h) * d_3d) - min(0.044 * (h ** 1.72), 14.77)
        elif d_bp < d_2d < 10e3:
            pl_los = (np.log10(d_3d) * min(0.03 * (h ** 1.72), 10.0)) + \
                     (20.0 * np.log10(40.0 * pi * d_3d * (f_c / 3e9))) + \
                     (0.002 * np.log10(h) * d_3d) - min(0.044 * (h ** 1.72), 14.77) + (40.0 * np.log10(d_3d / d_bp))

        if 10.0 < d_2d < 5e3:
            pl_nlos = 161.04 - (7.1 * np.log10(w)) + (7.5 * np.log10(h)) - \
                      ((24.37 - (3.7 * np.square(h / h_bs))) * np.log10(h_bs)) + \
                      ((43.42 - (3.1 * np.log10(h_bs))) * (np.log10(d_3d) - 3.0)) + \
                      (20.0 * np.log10(f_c / 1e9)) - ((3.2 * np.square(np.log10(11.75 * h_ue))) - 4.97)

        pls.append(max(pl_los, pl_nlos) if pl_los != 0.0 or pl_nlos != 0.0 else np.nan)

    df = pd.DataFrame(pls)
    df.to_excel('E:/SPAVE-28G/analyses/all-routes-numerical-evaluations/itur_pld.xlsx', index=False)


pathloss_uma_itur_m2135()
