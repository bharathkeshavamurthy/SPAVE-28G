/*
  A Utility class for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080, 
    and ServoCity continuous servos--along with IMU logging.

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#if defined(_WIN32) || defined(_WIN64)
    #include "windows.h"
#endif /* _WIN32 || _WIN64 */

#ifndef UTILITY_H_
#define UTILITY_H_

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#ifdef _WIN32
void curPos(int x, int y);
void clearTerminal();
#endif // _WIN32

void updateTime(SYSTEMTIME *time, FILETIME *timeF, ULONGLONG *time64);

ULONGLONG diffTimeSeconds(ULONGLONG timeStart, ULONGLONG timeEnd);

ULONGLONG diffTimeMilliseconds(ULONGLONG timeStart, ULONGLONG timeEnd);

ULONGLONG diffTimeMicroSeconds(ULONGLONG timeStart, ULONGLONG timeEnd);

unsigned char SpaceKeyPressed();

unsigned char QKeyPressed();

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // UTILITY_H_