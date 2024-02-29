"""
RxRealm Python Controller Eitri Bluetooth Test

Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
Copyright (c) 2021. All Rights Reserved.
"""

import time
import ntplib
import datetime
from serial import Serial
from datetime import datetime
from ntplib import NTPException

ntp_client = ntplib.NTPClient()
datetime.utcfromtimestamp(ntp_client.request('pool.ntp.org').tx_time)


def get_timestamp():
    """

    Returns: NTPLib Synchronized Timestamp

    """
    return str(datetime.utcfromtimestamp(ntp_client.request('pool.ntp.org').tx_time))


def start():
    """
    Start the tests
    """
    init, rotated, data_read = True, False, False
    i, j = 0, 0
    uc = Serial(port='COM10', baudrate=115200, timeout=1000.0)
    while j < len(yaw_angles):
        try:
            # noinspection PyUnresolvedReferences
            if uc.isOpen():
                if init:
                    uc.write(bytes('$$$', encoding='UTF-8'))
                    time.sleep(1.0)
                    uc.write(bytes('U,9600,N\n', encoding='UTF-8'))
                    init = False
                    continue
                read_data = uc.readline().decode(encoding='UTF-8')
                if read_data.startswith('$'):
                    data_read = True
                    print('[INFO] [{}] | RxRealm | Received GPS data from Eitri | [{}]'.format(get_timestamp(),
                                                                                               read_data.strip('\n')))
                else:
                    data_read = False
                if (i % 5 == 0) and data_read:
                    uc.write(
                        bytes(''.join(['<', str(yaw_angles[j]), '#', str(pitch_angles[j]), '>']), encoding='UTF-8'))
                    rotated = True
                    print('[INFO] [{}] RxRealm | Rotated Yaw Angle = [{}] | Rotated Pitch Angle = [{}]'.format(
                        get_timestamp(), yaw_angles[j], pitch_angles[j]))
                    j += 1
                i += 1 if data_read else 0
            else:
                print('[INFO] [{}] | RxRealm | Eitri is not up to receiving mandates from you...')
        except NTPException as ntpe:
            print('[WARN] [] | RxRealm | NTPException caught | Unable to find timestamp | Retrying | [{}]'.format(ntpe))
            if rotated:
                j -= 1
            if data_read:
                i -= 1
            continue
    print('[INFO] [{}] | RxRealm | Testing completed!')


# Test scenarios
yaw_angles = [34.70, 100.0, 10.0, 45.0, 0.0, 90.0, 34.70]
pitch_angles = [0.0, 5.0, -5.0, 10.0, -10.0, 2.5, -2.5]
start()
