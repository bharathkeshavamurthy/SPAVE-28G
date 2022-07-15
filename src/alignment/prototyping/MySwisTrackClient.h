/*
  A SwisTrack client for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
	and ServoCity continuous servos--along with IMU logging.

  Receive CommunicationMessages from SwisTrack | Parse these messages | Relay the parsed data to the PositionController

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#ifndef HEADER_MySwisTrackClient
#define HEADER_MySwisTrackClient

#include "CommunicationNMEAInterface.h"

#define MAX_NUM_PARTICLES 7

// This is the main class of our sample program. It reads data from STDIN and passes them through the NMEA parser.
class MySwisTrackClient: public CommunicationNMEAInterface {

public:
	// Constructor
	MySwisTrackClient();

	// Main loop.
	void processData(char * buff, int len);

	// CommunicationNMEAInterface methods
	void OnNMEAProcessMessage(CommunicationMessage *m, bool withchecksum);
	void OnNMEAProcessMessageChecksumError(CommunicationMessage *m);
	void OnNMEAProcessUnrecognizedChar(char c);
	void OnNMEASend(const std::string &str);

    void setNumParticles(int num);
    unsigned int getPacketCount(int id);
    void resetPacketCount(int id);
	uint8_t getNumParticlesDetected(void);

	// Just as an example, we count the number of PARTICLE messages per frame
	int mParticleMessagesCount;

	// Keeps track of the current frame number
	int mFrameNumber;

    unsigned int numOfParticles;
	double xPos[MAX_NUM_PARTICLES], yPos[MAX_NUM_PARTICLES];
	double xPosPrev[MAX_NUM_PARTICLES], yPosPrev[MAX_NUM_PARTICLES];
	unsigned int currentParticle;
	unsigned int packetCount[MAX_NUM_PARTICLES];
};

#endif // HEADER_MySwisTrackClient
