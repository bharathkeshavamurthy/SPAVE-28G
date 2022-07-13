#!/bin/bash
# This shell script controls the startup actions relevant to the Sliding Correlator Channel Sounder Power Delay Profile Sample Logging in Project Odin.
# Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
# Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
# Copyright (c) 2021. All Rights Reserved.

# sudo apt-get update && sudo apt-get upgrade

# The libuhd.so library's location has been added to /etc/ld.so.conf -- so, run ldconfig to load that into this session when Pi boots up.
sudo ldconfig

# Download the UHD images, if not already done so...
sudo uhd_images_downloader

# Once the images have been downloaded, set the "UHD_IMAGES_DIRECTORY" environment variables in order to set things up for the next stages.
export UHD_IMAGES_DIRECTORY="/usr/local/share/uhd/images"

# With the master clock frequency set to 10MHz (sample_rate=200kHz), determine the connected UHD devices -- and, get things ready for the GNURadio Python Script Execution.
sudo uhd_usrp_probe --args="master_clock_rate=10e6"

python3 /home/pi/Workspaces/odin/sounder/src/Sliding_Correlator_Channel_Sounder.py &
