"""
This script encapsulates the operations involved in the empirical validation of the Quasi-Deterministic Channel (QDC)
mmWave model using the data collected during our 28-GHz V2X outdoor measurement campaign in Salt Lake City, UT.

The visualizations associated with this script include cluster inter-arrival times, intra- and inter-cluster AoA/AoD,
delay spread, direction spread, etc. The reference paper for this modeling and validation is given below:

@ARTICLE{QDC,
         author={Wang, Jian and Gentile, Camillo and Papazian, Peter B. and Choi, Jae-Kark and Senic, Jelena},
         journal={IEEE Antennas and Wireless Propagation Letters},
         title={Quasi-Deterministic Model for Doppler Spread in Millimeter-Wave Communication Systems},
         year={2017},
         volume={16},
         pages={2195-2198},
         doi={10.1109/LAWP.2017.2705578}}

Author: Bharath Keshavamurthy <bkeshav1@asu.edu | bkeshava@purdue.edu>
Organization: School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ
              School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
Copyright (c) 2022. All Rights Reserved.
"""
