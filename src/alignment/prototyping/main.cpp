/*
  Main class for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
    and ServoCity continuous servos--along with IMU logging.

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#include <stdio.h>
#include "PositionController.h"
#include "Utility.h"
#if defined(_WIN32) || defined(_WIN64)
    #include "windows.h"
#endif

// Tracking 2 particles: One is the "Home Plate" AND the other is the laser pointer during rotation testing...
#define NUM_PARTICLES 2

int main(int argc, char *argv[]) {
	unsigned int exitProg=0;
    startPositionController(NUM_PARTICLES);
    while(!exitProg) {
        positionControlExec();
        printOnTerminal();
        if(QKeyPressed()) {
            exitProg = 1;
        }
    }
    stopPositionController();
	return 0;
}