/*
  CommunicationNMEAInterface for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
	and ServoCity continuous servos--along with IMU logging.

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#ifndef HEADER_CommunicationNMEAInterface
#define HEADER_CommunicationNMEAInterface

class CommunicationNMEAInterface;

#include "CommunicationMessage.h"

/*
	NMEA Interface.
*/
class CommunicationNMEAInterface {

private:
	enum eState {
		cState_None,
		cState_Argument,
		cState_Checksum1,
		cState_Checksum2,
		cState_Checksum2Discard
	};

	enum eState mState;
	static const int mBufferSize = 1024;
	char mBuffer[mBufferSize];
	int mBufferPos;
	int mChecksum;
	CommunicationMessage *mMessage;

public:
	// Constructor
	CommunicationNMEAInterface(): mState(cState_None), mBufferPos(0), mChecksum(0), mMessage(0) {}

	// Processes incoming data. This method should be called each time a piece of data is received. The data doesn't need to contain a complete NMEA record
	void NMEAProcessData(const char *buffer, int len);

	// This method can be called to send an NMEA message
	void NMEASendMessage(CommunicationMessage *m);

	// Called when a complete NMEA record has been received
	virtual void OnNMEAProcessMessage(CommunicationMessage *m, bool withchecksum) = 0;

	// Called when an NMEA record with a checksum error ist received
	virtual void OnNMEAProcessMessageChecksumError(CommunicationMessage *m) = 0;

	// Called to process a char that does not belong to an NMEA record
	virtual void OnNMEAProcessUnrecognizedChar(char c) = 0;

	// Called to send text to the client
	virtual void OnNMEASend(const std::string &str) = 0;
};

#endif // HEADER_CommunicationNMEAInterface