/*
  A CommunicationMessage wrapper for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
	and ServoCity continuous servos--along with IMU logging.

  Encapsulates the messages received from SwisTrack...

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#include "CommunicationMessage.h"
#include <sstream>
#include <cctype>
#include <algorithm>

CommunicationMessage::CommunicationMessage(const std::string &cmd, CommunicationMessage *inreplyto):
		mPopIndex(0), mParameters(), mCommand(cmd), mInReplyTo(inreplyto){

}

bool CommunicationMessage::GetBool(unsigned int i, bool defvalue) {
	if (mParameters.size() <= i) {
		return defvalue;
	}
	std::string str = mParameters[i];
	std::string strlc(str);
	std::transform(strlc.begin(), strlc.end(), strlc.begin(), (int(*)(int))std::tolower);
	if (strlc == "true") {
		return true;
	}
	if (strlc == "false") {
		return false;
	}
	std::istringstream istr(str);
	bool val = 0;
	istr >> val;
	return val;
}

int CommunicationMessage::GetInt(unsigned int i, int defvalue) {
	if (mParameters.size() <= i) {
		return defvalue;
	}
	std::string str = mParameters[i];

	std::istringstream istr(str);
	int val = 0;
	istr >> val;
	return val;
}

double CommunicationMessage::GetDouble(unsigned int i, double defvalue) {
	if (mParameters.size() <= i) {
		return defvalue;
	}
	std::string str = mParameters[i];

	std::istringstream istr(str);
	double val = 0;
	istr >> val;
	return val;
}

std::string CommunicationMessage::GetString(unsigned int i, const std::string &defvalue) {
	if (mParameters.size() <= i) {
		return defvalue;
	}
	return mParameters[i];
}

bool CommunicationMessage::AddBool(bool value) {
	if (value) {
		mParameters.push_back("true");
	} else {
		mParameters.push_back("false");
	}
	return true;
}

bool CommunicationMessage::AddInt(int value) {
	std::ostringstream oss;
	oss << value;
	mParameters.push_back(oss.str());
	return true;
}

bool CommunicationMessage::AddDouble(double value) {
	std::ostringstream oss;
	oss << value;
	mParameters.push_back(oss.str());
	return true;
}

bool CommunicationMessage::AddString(const std::string &value) {
	mParameters.push_back(value);
	return true;
}

bool CommunicationMessage::AddParsedArgument(const std::string &value) {
	if (mCommand == "") {
		mCommand = value;
	} else {
		mParameters.push_back(value);
	}
	return true;
}