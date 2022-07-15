/*
  A PositionController class for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
	and ServoCity continuous servos--along with IMU logging.

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#include "math.h"
#include "Utility.h"
#include "PositionController.h"
#include "NetworkConnection.h"
#include "MySwisTrackClient.h"

#if defined(_WIN32) || defined(_WIN64)
    #include "windows.h"
#endif // _WIN32 || _WIN64

/* The "Home Plate" and the current laser incidence point are the two particles to be tracked... */
#define NUM_PARTICLES_MAX 2

// Position Controller variable declarations
unsigned int stable, entered = 0;
unsigned int numParticles = 0;
MySwisTrackClient swistrackClient;
double controllerFreq = 0;
signed int particles[NUM_PARTICLES_MAX];

// Network connection & associated data transfer members
char server_reply[2000];
int recv_size;

SYSTEMTIME recvTime, currTime, controlFreqUpdateTime;
FILETIME recvTimeF, currTimeF, controlFreqUpdateTimeF;
ULONGLONG recvTime64, currTime64, controlFreqUpdateTime64;

void printOnTerminal() {
	/* TODO: Use the laser pointer's motion to identify it AND Use the home plate's stationary status to identify it--instead of hard-coding '0' and '1' as their identifiers... */
	
    // printf("Home Plate Co-ordinates: x=%6.2f, y=%6.2f\r\n", swistrackClient.xPos[particles[0]], swistrackClient.yPos[particles[0]]);
	// printf("Laser Incidence Point Co-ordinates: x=%6.2f, y=%6.2f\r\n", swistrackClient.xPos[particles[1]], swistrackClient.yPos[particles[1]]);
	
	/* Not printing here...Check MySwisTrackClient.cpp */
}

void startPositionController(int nParticles) {
    unsigned int i=0;
    // Open the connection with Swistrack to get the tracking information
    OpenTCPConnection(3000, "127.0.0.1");
    numParticles = nParticles;
    swistrackClient.setNumParticles(nParticles);
    updateTime(&recvTime, &recvTimeF, &recvTime64);
    updateTime(&controlFreqUpdateTime, &controlFreqUpdateTimeF, &controlFreqUpdateTime64);
	clearTerminal();
}

void stopPositionController() {
    CloseTCPConnection();
}

void ready_position_data() {
	recv_size = ReceiveTCP(server_reply);
	if(recv_size > 0) {
		updateTime(&currTime, &currTimeF, &currTime64);
		if(diffTimeSeconds(controlFreqUpdateTime64, currTime64) >= 1) {
			controllerFreq = 1000.0 / ( (double) diffTimeMilliseconds(controlFreqUpdateTime64, currTime64) / (double) swistrackClient.getPacketCount(0) );
			swistrackClient.resetPacketCount(0);
			updateTime(&controlFreqUpdateTime, &controlFreqUpdateTimeF, &controlFreqUpdateTime64);
		}
		swistrackClient.processData(server_reply, recv_size);
		if(swistrackClient.getNumParticlesDetected() != numParticles) {
			/* Nothing to do here... */
		}	
		memset(server_reply, 0x00, 300);
	}
}

void positionControlExec() {
	ready_position_data();
}