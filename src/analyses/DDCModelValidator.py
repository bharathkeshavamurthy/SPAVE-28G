"""
This script encapsulates the operations involved in the empirical validation of the Double Directional Channel (DDC)
mmWave model using the data collected during our 28-GHz V2X outdoor measurement campaign in Salt Lake City, UT.

The visualizations associated with this script include cluster inter-arrival times, intra- and inter-cluster AoA/AoD,
delay spread, direction spread, etc. The reference paper for this modeling and validation is given below:

@INPROCEEDINGS{DDC,
               author={Tang, Shuaiqin and Kumakura, Keiichiro and Kim, Minseok},
               booktitle={2020 International Symposium on Antennas and Propagation (ISAP)},
               title={Millimeter-Wave Double-Directional Channel Sounder using COTS RF Transceivers},
               year={2021},
               pages={753-754},
               doi={10.23919/ISAP47053.2021.9391484}}

Author: Bharath Keshavamurthy <bkeshav1@asu.edu | bkeshava@purdue.edu>
Organization: School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ
              School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
Copyright (c) 2022. All Rights Reserved.
"""
