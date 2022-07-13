/*
  A SwisTrack client for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
	and ServoCity continuous servos--along with IMU logging.

  Receive CommunicationMessages from SwisTrack | Parse these messages | Relay the parsed data to the PositionController

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#include <string>
#include <iostream>
#include <math.h>
#include <stdlib.h>
#include "MySwisTrackClient.h"

MySwisTrackClient::MySwisTrackClient(): CommunicationNMEAInterface() {
	mParticleMessagesCount = 0;
	mFrameNumber = 0;
}

void MySwisTrackClient::setNumParticles(int num) {
    numOfParticles = num;
}

void MySwisTrackClient::processData(char * buff, int len) {
    // Pass it to the NMEA parser (this will subsequently call our OnNMEA* methods)
	NMEAProcessData(buff, len);
}

unsigned int MySwisTrackClient::getPacketCount(int id) {
    return packetCount[id];
}

void MySwisTrackClient::resetPacketCount(int id) {
    packetCount[id] = 0;
}

void MySwisTrackClient::OnNMEAProcessMessage(CommunicationMessage *m, bool withchecksum) {
	/* This method is called each time a complete message was received (with either a valid or no checksum).
	 * A message consists of a command string and arguments. The command string can be accessed via
	 *     m->mCommand
	 * while the arguments can be read (and converted) with
	 *     bool myvar = m->GetBool(int i, bool defaultvalue);
	 *     int myvar = m->GetInt(int i, int defaultvalue);
	 *     double myvar = m->GetDouble(int i, double defaultvalue);
	 *     std::string myvar = m->GetString(int i, const std::string defaultvalue);
	 * or
	 *     bool myvar = m->PopBool(bool defaultvalue);
	 *     int myvar = m->PopInt(int defaultvalue);
	 *     double myvar = m->PopDouble(double defaultvalue);
	 *     std::string myvar = m->PopString(const std::string defaultvalue);
	 * The Get* methods return the argument at a specific index, or the default value if that argument doesn't exist.
	 * The Pop* methods return the next argument each time they are called.
	 */
	if (m->mCommand == "STEP_START") {
		// This message indicates the beginning of a new frame
		mFrameNumber = 0;
		mParticleMessagesCount = 0;
	} else if (m->mCommand == "FRAMENUMBER") {
		// This message contains the frame number
		mFrameNumber = m->PopInt(0);
	} else if (m->mCommand == "PARTICLE") {
		// This message transmits the position of a particle for the current frame
		mParticleMessagesCount++;
		currentParticle = m->GetInt(0,0);
        xPos[currentParticle] = m->GetDouble(1, 0);
        yPos[currentParticle] = m->GetDouble(2, 0);
		std::cout << currentParticle << ": x = " << xPos[currentParticle] << " | y = " << yPos[currentParticle] << std::endl;
        packetCount[currentParticle]++;

	} else if (m->mCommand == "STEP_STOP") {
		// This message indicates the end of the current frame
	} else {
		std::cout << "Unknown message '" << m->mCommand << "' received!" << std::endl;
	}
}

void MySwisTrackClient::OnNMEAProcessMessageChecksumError(CommunicationMessage *m) {
	/* We don't really expect messages with checksum errors here.
	 * Hence, we just discard such messages.
	 *
	 * In case you are working with lossy channels, you could do something more fancy here.
	 */
}

void MySwisTrackClient::OnNMEAProcessUnrecognizedChar(char c) {
	/* Unrecognized chars are bytes that are transmitted between valid NMEA messages.
	 * An NMEA stream usually only consists of NMEA messages, but people sometimes put human readable comments in between.
	 * On lossy channels, messages missing the $ sign (start-of-message) would also fall in here.
	 *
	 * This method is called for each char that does not belong to an NMEA message.
	 * But unless you have a good reason, leave this method empty.
	 */
}

void MySwisTrackClient::OnNMEASend(const std::string &str) {
	/* Whenever you send a message using NMEASendMessage(CommunicationMessage *m),
	 * this method is called to actually transmit the bytes.
	 */

	// This is just useless cosmetics, as we never send stuff back to SwisTrack in this simple example
	std::cout << str;
}

uint8_t MySwisTrackClient::getNumParticlesDetected(void) {
	return mParticleMessagesCount;
}