/*
  CommunicationNMEAInterface for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
	and ServoCity continuous servos--along with IMU logging.

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#include <sstream>
#include "CommunicationNMEAInterface.h"

void CommunicationNMEAInterface::NMEAProcessData(const char *buffer, int len) {
	for (int i = 0; i < len; i++) {
		char inchar = buffer[i];
		if (mState == cState_None) {
			if (inchar == '$') {
				mMessage = new CommunicationMessage();
				mBufferPos = 0;
				mChecksum = 0;
				mState = cState_Argument;
			} else {
				OnNMEAProcessUnrecognizedChar(inchar);
			}
		} else if (mState == cState_Argument) {
			if (inchar == ',') {
				mBuffer[mBufferPos] = 0;
				mMessage->AddParsedArgument(mBuffer);
				mChecksum = mChecksum ^ inchar;
				mBufferPos = 0;
			} else if (inchar == '*') {
				mBuffer[mBufferPos] = 0;
				mMessage->AddParsedArgument(mBuffer);
				mState = cState_Checksum1;
			} else if ((inchar == '\n') || (inchar == '\r')) {
				mBuffer[mBufferPos] = 0;
				mMessage->AddParsedArgument(mBuffer);
				OnNMEAProcessMessage(mMessage, false);
				delete mMessage;
				mMessage = 0;
				mState = cState_None;
			} else {
				mBuffer[mBufferPos++] = inchar;
				mChecksum = mChecksum ^ inchar;
			}
		} else if (mState == cState_Checksum1) {
			char checksum_expected = "0123456789ABCDEF"[(mChecksum >> 4) & 0xF];
			if (inchar == '\n') {
				OnNMEAProcessMessageChecksumError(mMessage);
				delete mMessage;
				mMessage = 0;
				mState = cState_None;
			} else if (inchar == checksum_expected) {
				mState = cState_Checksum2;
			} else {
				OnNMEAProcessMessageChecksumError(mMessage);
				delete mMessage;
				mMessage = 0;
				mState = cState_Checksum2Discard;
			}
		} else if (mState == cState_Checksum2) {
			char checksum_expected = "0123456789ABCDEF"[mChecksum & 0xF];
			if (inchar == checksum_expected) {
				OnNMEAProcessMessage(mMessage, true);
			} else {
				OnNMEAProcessMessageChecksumError(mMessage);
			}
			delete mMessage;
			mMessage = 0;
			mState = cState_None;
		} else if (mState == cState_Checksum2Discard) {
			mState = cState_None;
		} else {
		}
	}
}

void CommunicationNMEAInterface::NMEASendMessage(CommunicationMessage *m) {
	// Concatenate
	std::ostringstream line;
	line << "$" << m->mCommand;
	CommunicationMessage::tParameters::iterator it = m->mParameters.begin();
	while (it != m->mParameters.end()) {
		line << "," << *it;
		it++;
	}

	// Calculate checksum
	int checksum = 0;
	std::string linestr = line.str();
	int len = linestr.length();
	const char *buffer = linestr.c_str();
	for (int n = 1; n < len; n++) {
		checksum = checksum ^ buffer[n];
	}
	char ch1 = "0123456789ABCDEF"[(checksum >> 4) & 0xF];
	char ch2 = "0123456789ABCDEF"[checksum & 0xF];
	line << "*" << ch1 << ch2;

	// Send
	line << "\r";
	OnNMEASend(line.str());
	return;
}