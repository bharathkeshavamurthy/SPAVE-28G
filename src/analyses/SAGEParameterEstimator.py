"""
This script encapsulates the operations involved in estimating the propagation parameters associated with the various
Multi-Path Components (MPCs) in our 28-GHz outdoor measurement campaign on the POWDER testbed in Salt Lake City, UT.
The constituent algorithm is Space-Alternating Generalized Expectation-Maximization (SAGE), derived from the following
reference paper:

@INPROCEEDINGS{SAGE,
  author={Yin, Xuefeng and He, Yongyu and Song, Zinuo and Kim, Myung-Don and Chung, Hyun Kyu},
  booktitle={The 8th European Conference on Antennas and Propagation (EuCAP 2014)},
  title={A sliding-correlator-based SAGE algorithm for Mm-wave wideband channel parameter estimation},
  year={2014},
  pages={625-629},
  doi={10.1109/EuCAP.2014.6901837}}

Author: Bharath Keshavamurthy <bkeshav1@asu.edu | bkeshava@purdue.edu>
Organization: School of Electrical, Computer and Energy Engineering, Arizona State University, Tempe, AZ
              School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN
Copyright (c) 2022. All Rights Reserved.
"""
