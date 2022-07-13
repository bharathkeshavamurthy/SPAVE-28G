/*
  A PositionController class for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
    and ServoCity continuous servos--along with IMU logging.

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#if defined(_WIN32) || defined(_WIN64)
    #include "windows.h"
#endif // _WIN32 || _WIN64

#ifndef POSITION_CONTROLLER_H_
#define POSITION_CONTROLLER_H_

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

void positionControlExec();

void startPositionController(int nParticles);

void stopPositionController();

void printOnTerminal();

void ready_position_data();

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // POSITION_CONTROLLER_H_