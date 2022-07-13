"""
This script allows me to test the timing synchronization across modules in Project Odin v21.06.
Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
Copyright (c) 2021. All Rights Reserved.
"""

# The imports
import ntplib
import datetime
import traceback

# from time import ctime

client = ntplib.NTPClient()
while 1:
    try:
        response = client.request('pool.ntp.org')
        print('[INFO] TimingSynchronization main: Time = [{}]'.format(
            datetime.datetime.utcfromtimestamp(response.tx_time)))
    except Exception as e:
        print('[ERROR] TimingSynchronization main: Exception caught while querying UTC time via NTP - {}'.format(
            traceback.print_tb(e.__traceback__)))
        continue
# The evaluation/testing ends here...
