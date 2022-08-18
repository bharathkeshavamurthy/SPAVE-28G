"""
This script encapsulates the operations involved in the empirical validation of the Saleh-Valenzuela Clustered mmWave
Channel Model using the data collected during our 28-GHz V2X outdoor measurement campaign in Salt Lake City, UT.

The visualizations associated with this script include cluster inter-arrival times, intra- and inter-cluster AoA/AoD,
delay spread, direction spread, etc. The reference paper for this modeling and validation is given below:

@ARTICLE{SVC,
    author={Gustafson, Carl and Haneda, Katsuyuki and Wyne, Shurjeel and Tufvesson, Fredrik},
    journal={IEEE Transactions on Antennas and Propagation},
    title={On mm-Wave Multipath Clustering and Channel Modeling},
    year={2014},
    volume={62},
    number={3},
    pages={1445-1455},
    doi={10.1109/TAP.2013.2295836}}

Author: Bharath Keshavamurthy <bkeshav1@asu.edu | bkeshava@purdue.edu>
Organization: School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ
              School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
Copyright (c) 2022. All Rights Reserved.
"""
