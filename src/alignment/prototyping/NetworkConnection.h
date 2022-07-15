/*
  A NetworkConnection class for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
    and ServoCity continuous servos--along with IMU logging.

  A TCP connection between this controller and SwisTrack

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#if defined(_WIN32) || defined(_WIN64)
    #include "windows.h"
    #include <winsock2.h>
#endif // _WIN32 || _WIN64

#ifndef NETWORK_CONNECTION_H_
#define NETWORK_CONNECTION_H_

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

// CONNECTTOHOST – Connects to a remote host
signed char OpenTCPConnection(int PortNo, char* IPAddress);

void CloseTCPConnection();

signed int ReceiveTCP(char *server_reply);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // NETWORK_CONNECTION_H_