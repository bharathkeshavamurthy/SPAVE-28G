/*
 * Eitri: The metal worker in Project Odin.
 * An Arduino sketch to perform the following tasks: 
 * 1. Publish the GPS readings received from the SparkFun UBlox ZED-F9P RTK-GPS module;
 * 2. Receive the required angles of rotation (yaw and pitch) from the XXRealm Python Controller over Bluetooth/USB; and
 * 3. Yaw and Pitch rotations using the SparkFun Blackboard Rev C, IMU BNO080, and ServoCity continuous servos -- along with IMU logging.
 * 
 * Eitri_Tx: No GPS
 * 
 * Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
 * Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
 * Copyright (c) 2021. All Rights Reserved.
*/

/* Pre-processor Directives */
#include <Wire.h>
#include <Servo.h>
#include "SparkFun_BNO080_Arduino_Library.h"

/* Rotation Control Logic Definitions */
#define STEADY_STATE 10                    // Avoid "nan" yaw and pitch values by forcing steady-state reads...
#define READ_EQUILIBRIUM 0                 // The number of samples to be read before we decide to use the IMU report

/* Servo PWM Control Values */
#define MAX_PWM 2250                       // Forward Motion (PWM ON pulse duration)
#define MID_PWM 1500                       // Stop Motion (PWM ON pulse duration)
#define MIN_PWM 750                        // Backward Motion (PWM ON pulse duration)

/* IMU Instance Creation
 * Servo control pin definitions | Servo (X and Z) instance creation
 * X = Yaw movements | Z = Pitch & Roll movements | For pitch movements, place the object being rotated perpendicular to the pan plate -- in the X-Z plane | Manual height adjustments
*/
BNO080 bno080;
int pinX = 9, pinZ = 10;
Servo servoX, servoZ;

/* Core Rotational Variables */
float yaw = 0.0, prevYaw = 0.0, pitch = 0.0, prevPitch = 0.0, yawDelta = 0.0;       // The estimated yaw angle, its previous time-step counterpart, and their difference | The estimated pitch angle and its previous time-step counterpart | A variable to hold the difference between current yaw and previous yaw (mandate-triggered) in order to evaluate the stopping criterion
float tmpYaw = 720.0, requiredYawAngle = 0.0, currentYawAngle = 0.0;                // The core yaw axis variables facilitating motor control
float tmpPitch = 720.0, requiredPitchAngle = 0.0, currentPitchAngle = 0.0;          // The core pitch axis variables facilitating motor control
float yawReconfigRequirement = 0.0, pitchReconfigRequirement = 0.0;                 // The reconfigured mandates in terms of the amount of angle that needs to be traversed -- either in the anti-clockwise direction or in the clockwise direction
float aclkYawRotation = 0.0, clkYawRotation = 0.0;                                  // The movement variables that encapsulate the amount of yaw rotation in the anti-clockwise & clockwise directions, respectively
float aclkPitchRotation = 0.0, clkPitchRotation = 0.0;                              // The movement variables that encapsulate the amount of pitch rotation in the anti-clockwise & clockwise directions, respectively
boolean proceedToPitch = false, motorsInitiated = false, mandateAchieved = true;    // A flag to give the go-ahead to move on to pitch changes | A flag to indicate if the motors have been initiated or not | A flag to indicate that the overall correction mandate from the XXRealm Python Controller has been met
unsigned int sampleCounter = 0, steadyStateCounter = 0;                             // A counter for ensuring that the IMU values are read post-equilibrium | A counter to force steady state reads

/* Core GPS Variable */
long prevTime = 0;                                                                  // A previous time member for GPS data logging time-delta storage

/* Core Mandate Extraction Variables */
const byte numChars = 32;                                                           // The length of the mandate (correction string from the XXRealm Python Controller) character array
char receivedChars[numChars];                                                       // The mandate (correction string from the XXRealm Python Controller) character array
boolean newData = false;                                                            // A flag to indicate that a new mandate has been received

/* The setup routine */
void setup() {
  Serial.begin(115200);
  while (!Serial);
  
  Wire.begin();                                                                     // Begin the Wire instance to start reading the I2C interface...
  
  if (!bno080.begin()) {                                                            // Check for the IMU at the I2C interface
    return;
  }
  Wire.setClock(400000);                                                            // I2C Data Rate = 400kHz
  bno080.calibrateAll();                                                            // Turn on calibration for Accelerometer, Gyroscope, and Magnetometer
  bno080.enableGameRotationVector(20);                                              // IMU Refresh for rotation vector data at 20ms
  bno080.enableMagnetometer(20);                                                    // IMU Refresh for magnetometer data at 20ms
  
  servoX.attach(pinX);                                                              // Attach to servoX | Reset to zero motion | Yaw Servo
  servoX.writeMicroseconds(MID_PWM);
  servoZ.attach(pinZ);                                                              // Attach to servoZ | Reset to zero motion | Pitch Servo
  servoZ.writeMicroseconds(MID_PWM);
}

/* The loop routine */
void loop() {
  /* 
   * Data extraction and logging:
   * 1. Read the Quaternion Complex Vector values [i, j, k, real] and Normalize them;
   * 2. Parse Input Report from BNO080 --> Bosch Sensor Hub Transport Protocol (SHTP) over I2C --> Parse raw quaternion values if the channel is CHANNEL_GYRO in the SHTP Header OR if the SHTP Payload is referenced by SENSOR_*_ROTATION_VECTOR; and
   * 3. Use the BNO080 Library for this | Modify the C++ code if needed with the Q-values | https://github.com/fm4dd/pi-bno080 | https://github.com/sparkfun/SparkFun_BNO080_Arduino_Library/blob/master/src/SparkFun_BNO080_Arduino_Library.cpp.
  */
  if (bno080.dataAvailable()) {
    float quatI = bno080.getQuatI();
    float quatJ = bno080.getQuatJ();
    float quatK = bno080.getQuatK();
    float quatReal = bno080.getQuatReal();
    float norm = sqrt( (quatI * quatI) + (quatJ * quatJ) + (quatK * quatK) + (quatReal * quatReal) );
    quatI /= norm;
    quatJ /= norm;
    quatK /= norm;
    quatReal /= norm;
    yaw = atan2( ( 2.0 * ( (quatReal * quatK) + (quatI * quatJ) ) ), ( 1.0 - ( 2.0 * ( (quatJ * quatJ) + (quatK * quatK) ) ) ) ) * (180 / PI);     // Determine the Yaw Angle (in degrees)
    pitch = atan2( ( 2.0 * ( (quatReal * quatI) + (quatJ * quatK) ) ), ( 1.0 - ( 2.0 * ( (quatI * quatI) + (quatJ * quatJ) ) ) ) ) * (180 / PI);   // Determine the Pitch Angle (in degrees)
    sampleCounter += 1;                                                                                                                            // Update the sample counter
    steadyStateCounter += 1;                                                                                                                       // Update the steady state counter
  }
  
  if (steadyStateCounter < STEADY_STATE) {
    return;
  }
    
  /* Mandate Extraction */
  recvWithStartEndMarkers();
  if (newData == true) {
    tmpYaw = atof(strtok(receivedChars, "#"));
    tmpPitch = atof(strtok(NULL, "#"));
    newData = false;
  }

  /* Mandate Execution Dispatch */
  if ( (tmpYaw != 720.0) && (tmpPitch != 720.0) && ( (tmpYaw != requiredYawAngle) || (tmpPitch != requiredPitchAngle) ) ) {
    if (motorsInitiated == true) {
      servoX.writeMicroseconds(MID_PWM);
      servoZ.writeMicroseconds(MID_PWM);
      motorsInitiated = false;
    }
    mandateAchieved = false;
    sampleCounter = 0;                                                                                                   // Start fresh with new samples in order to achieve READ_EQUILIBRIUM
    
    requiredYawAngle = tmpYaw;
    requiredPitchAngle = tmpPitch;
    prevYaw = yaw;                                                                                                       // Raw, unfiltered previous yaw store
    prevPitch = pitch;                                                                                                   // Raw, unfiltered previous pitch store
  }
  
  /* Yaw Rotation Control */
  if ( (mandateAchieved == false) && (proceedToPitch == false) && (sampleCounter >= READ_EQUILIBRIUM) ) {
    currentYawAngle = (yaw < 0.0) ? (360.0 + yaw) : yaw;
    if (motorsInitiated == false) {
      if (currentYawAngle <= requiredYawAngle) {                                                                         // Variant-1
        aclkYawRotation = requiredYawAngle - currentYawAngle;
        clkYawRotation = 360 - (requiredYawAngle - currentYawAngle);
      } else {                                                                                                           // Variant-2
        clkYawRotation = currentYawAngle - requiredYawAngle;
        aclkYawRotation = 360 - (currentYawAngle - requiredYawAngle);
      }
      yawReconfigRequirement = (aclkYawRotation <= clkYawRotation) ? aclkYawRotation : clkYawRotation;
      servoX.writeMicroseconds( (aclkYawRotation <= clkYawRotation) ? MAX_PWM : MIN_PWM );                               // Use the optimal rotation direction (a simple comparison-based decision heuristic)
      motorsInitiated = true;
    } else {
      yawDelta = ( abs(yaw - prevYaw) > 180.0 ) ? ( 360.0 - abs(yaw - prevYaw) ) : abs(yaw - prevYaw);
      if ( yawDelta >= yawReconfigRequirement ) {                                                                        // Stop, once the mandate has been met -- and, move on to pitch adjustments...
        servoX.writeMicroseconds(MID_PWM);
        proceedToPitch = true;
        motorsInitiated = false;
      }
    }
    sampleCounter = 0; 
  }
  
  /* Pitch Rotation Control */
  if ( (mandateAchieved == false) && (proceedToPitch == true) && (sampleCounter >= READ_EQUILIBRIUM) ) {
    currentPitchAngle = pitch;
    if (motorsInitiated == false) {
      pitchReconfigRequirement = abs(currentPitchAngle - requiredPitchAngle);
      servoZ.writeMicroseconds( (currentPitchAngle <= requiredPitchAngle) ? MAX_PWM  : MIN_PWM );                       // Use the optimal rotation direction (a simple comparison-based decision heuristic)
      motorsInitiated = true;
    } else {
      if ( abs(pitch - prevPitch) >=  pitchReconfigRequirement ) {                                                      // Stop, once the mandate has been met -- and, move on to the next mandate (if one exists).
        servoZ.writeMicroseconds(MID_PWM);
        proceedToPitch = false;
        motorsInitiated = false;
        mandateAchieved = true;
      }
    }
    sampleCounter = 0;
  }
}

/* Mandate (Correction String from XXRealm Python Controller) Read Routine */
void recvWithStartEndMarkers() {
  static boolean recvInProgress = false;
  static byte ndx = 0;
  char startMarker = '<';
  char endMarker = '>';
  char rc;
  while ( (Serial.available() > 0) && (newData == false) ) {
    rc = Serial.read();
    if (recvInProgress == true) {
      if (rc != endMarker) {
        receivedChars[ndx] = rc;
        ndx++;
        if (ndx >= numChars) {
          ndx = numChars - 1;
        }
      }
      else {
        receivedChars[ndx] = '\0';
        recvInProgress = false;
        ndx = 0;
        newData = true;
      }
    }
    else if (rc == startMarker) {
      recvInProgress = true;
    }
  }
}
