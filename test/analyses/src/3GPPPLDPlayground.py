"""
3GPP TR38.901 Pathloss v Distance Playground [Testing Script]

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

import numpy as np
import pandas as pd
from scipy import constants


def pathloss_uma_3gpp_tr38901():
    c = constants.speed_of_light
    pls, pl_los, pl_nlos = [], 0.0, 0.0
    h_ue, h_bs, f_c = 2.0, 25.276639648, 28e9

    for d_3d in range(30, 1001, 1):

        f_c_, h_ue_, h_bs_ = f_c / 1e9, h_ue - 1.0, h_bs - 1.0
        d_bp, d_2d = 4 * h_ue_ * h_bs_ * f_c_ * (1e9 / c), np.sqrt(np.square(d_3d) - np.square(h_bs - h_ue))

        if 10.0 <= d_2d <= d_bp:
            pl_los = 28.0 + (22.0 * np.log10(d_3d)) + (20.0 * np.log10(f_c_))
        elif d_bp <= d_2d <= 5e3:
            pl_los = 28.0 + (40.0 * np.log10(d_3d)) + (20.0 * np.log10(f_c_)) - (9.0 * np.log10(np.square(d_bp) +
                                                                                                np.square(h_ue - h_bs)))

        if 10.0 < d_2d < 5e3:
            pl_nlos = 13.54 + (39.08 * np.log10(d_3d)) + (20.0 * np.log10(f_c_)) - (0.6 * (h_ue - 1.5))

        pls.append(max(pl_los, pl_nlos) if pl_los != 0.0 or pl_nlos != 0.0 else np.nan)

    df = pd.DataFrame(pls)
    df.to_excel('<output_file_location>', index=False)


pathloss_uma_3gpp_tr38901()
