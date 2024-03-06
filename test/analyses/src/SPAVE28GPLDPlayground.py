"""
SPAVE-28G Pathloss v Distance Playground [Testing Script]

Author: Bharath Keshavamurthy <bkeshava@purdue.edu | bkeshav1@asu.edu>
Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
              School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ

Copyright (c) 2023. All Rights Reserved.
"""

import numpy as np
import pandas as pd
from geopy import distance

tx_lat, tx_lon = 40.766173670, -111.847939330


# Tx-Rx 3D distance (m)
def distance_3d(rx_lat, rx_lon):
    tx_coords = (tx_lat, tx_lon)
    rx_coords = (rx_lat, rx_lon)
    distance_2d = distance.distance(tx_coords, rx_coords).m
    return np.sqrt(np.square(distance_2d) + np.square(25.276639648 - 2.0))


dists, dist_pl_dict = [], {}
df = pd.read_csv('<urban_pl_df_aggregated_dataframe_location>')

for idx, row in df.iterrows():
    lat, lon, pl = row['latitude'], row['longitude'], row['pathloss']
    dist = np.round(distance_3d(lat, lon), 5)

    if dist not in dists:
        dists.append(dist)
        dist_pl_dict[dist] = pl

o_df = pd.DataFrame(dist_pl_dict.items(), columns=['Distance', 'Pathloss'])
o_df.sort_values('Distance', inplace=True)

o_df.to_excel('<urban_pl_df_aggregated_excel_file_location>')
