"""
METIS Pathloss v Distance Playground [Testing Script]

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

import numpy as np
import pandas as pd
from scipy import constants


def pathloss_umi_metis():
    c = constants.speed_of_light
    pls, pl_los, pl_nlos = [], 0.0, 0.0
    h_ue, h_bs, f_c = 2.0, 25.276639648, 28e9

    for d_3d in range(30, 1001, 1):
        h_ue_, h_bs_, f_c_, d_2d = h_ue - 1.0, h_bs - 1.0, f_c / 1e9, np.sqrt(np.square(d_3d) - np.square(h_bs - h_ue))

        pl_0 = -1.38 * np.log10(f_c_) + 3.34
        d_bp = 0.87 * np.exp(-np.log10(f_c_) / 0.65) * ((4.0 * h_ue_ * h_bs_) / (c / f_c))

        pl_1 = (22.0 * np.log10(d_3d)) + 28.0 + (20.0 * np.log10(f_c_)) + pl_0
        pl_1_func = lambda _d: (22.0 * np.log10(_d)) + 28.0 + (20.0 * np.log10(f_c_)) + pl_0
        pl_2 = (40.0 * np.log10(d_3d)) + 7.8 - (18.0 * np.log10(h_bs * h_ue)) + (2.0 * np.log10(f_c_)) + pl_1_func(d_bp)

        if 10.0 < d_2d < 5e3:
            if 10.0 < d_3d <= d_bp:
                pl_los = pl_1
            elif d_bp < d_3d <= 5e2:
                pl_los = pl_2

        if 10.0 < d_2d < 2e3:
            pl_nlos = (36.7 * np.log10(d_3d)) + 23.15 + (26.0 * np.log10(f_c_)) - (0.3 * h_ue)

        pls.append(max(pl_los, pl_nlos) if pl_los != 0.0 or pl_nlos != 0.0 else np.nan)

    df = pd.DataFrame(pls)
    df.to_excel('<output_file_location>', index=False)


pathloss_umi_metis()
