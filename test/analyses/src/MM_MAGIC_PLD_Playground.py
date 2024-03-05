"""
mmMAGIC Pathloss v Distance Playground [Testing Script]

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

import numpy as np
import pandas as pd


def pathloss_umi_mm_magic():
    pls, f_c_ = [], 28e9 / 1e9

    for d_3d in range(30, 1001, 1):
        pl_los = (19.2 * np.log10(d_3d)) + 32.9 + (20.8 * np.log10(f_c_))
        pl_nlos = (45.0 * np.log10(d_3d)) + 31.0 + (20.0 * np.log10(f_c_))
        pls.append(max(pl_los, pl_nlos) if pl_los != 0.0 or pl_nlos != 0.0 else np.nan)

    df = pd.DataFrame(pls)
    df.to_excel('<output_file_location>', index=False)


pathloss_umi_mm_magic()
